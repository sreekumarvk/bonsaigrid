// Open-loop, coordinated-omission-correct load generator (the wrk2 / Gil Tene
// method). A dispatcher emits requests at a fixed OFFERED rate; `conns` workers
// execute them. Latency is measured from each request's *ideal* scheduled time,
// not from when a worker picked it up — so once the server saturates and the pipe
// backs up, the tail reflects the real queueing delay instead of hiding it (the
// coordinated-omission bug that closed-loop load generators have).
//
// Sweeping the offered rate traces the classic latency "elbow": flat until the
// server saturates, then the tail explodes. That elbow is the number that matters
// for a cache, and closed-loop testing cannot see it.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type openOut struct {
	set, get                 []int64
	errs, mism, hits, misses int64
	achieved                 float64
}

// runOpen drives `rate` ops/sec through `conns` workers for `dur`.
func runOpen(ctx context.Context, s Store, cfg WLCfg, conns int, rate float64, dur, ttl time.Duration) openOut {
	type job struct {
		ideal time.Time
		read  bool
		key   uint64
	}
	jobs := make(chan job, conns)
	setL := make([][]int64, conns)
	getL := make([][]int64, conns)
	errs := make([]int64, conns)
	mism := make([]int64, conns)
	hits := make([]int64, conns)
	miss := make([]int64, conns)

	var wg sync.WaitGroup
	for i := 0; i < conns; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := range jobs {
				if j.read {
					got, err := s.Get(ctx, KeyStr(j.key))
					lat := time.Since(j.ideal).Microseconds() // from IDEAL time → CO-correct
					if err != nil {
						errs[i]++
						continue
					}
					getL[i] = append(getL[i], lat)
					switch {
					case len(got) == 0:
						miss[i]++
					case bytes.Equal(got, valueOf(cfg.DataSize, j.key)):
						hits[i]++
					default:
						mism[i]++
					}
				} else {
					err := s.Set(ctx, KeyStr(j.key), valueOf(cfg.DataSize, j.key), ttl)
					lat := time.Since(j.ideal).Microseconds()
					if err != nil {
						errs[i]++
						continue
					}
					setL[i] = append(setL[i], lat)
				}
			}
		}(i)
	}

	// Dispatcher (one goroutine; owns the workload RNG). Sends on the ideal
	// schedule; when behind (saturation), it stops sleeping and catches up in a
	// burst, so late requests carry their full queueing latency.
	wl := cfg.New(int64(rate) + 1)
	interval := time.Duration(float64(time.Second) / rate)
	start := time.Now()
	deadline := start.Add(dur)
	var n int64
	for i := int64(0); ; i++ {
		if time.Now().After(deadline) {
			break
		}
		ideal := start.Add(time.Duration(i) * interval)
		if d := time.Until(ideal); d > 0 {
			time.Sleep(d)
		}
		jobs <- job{ideal: ideal, read: wl.IsRead(), key: wl.NextKey()}
		n++
	}
	close(jobs)
	wg.Wait()

	out := openOut{achieved: float64(n) / dur.Seconds()}
	for i := 0; i < conns; i++ {
		out.set = append(out.set, setL[i]...)
		out.get = append(out.get, getL[i]...)
		out.errs += errs[i]
		out.mism += mism[i]
		out.hits += hits[i]
		out.misses += miss[i]
	}
	return out
}

// warmupWrites populates the hot part of the keyspace so GETs can hit, using a
// closed-loop SET burst (as fast as the server allows) for `dur`.
func warmupWrites(ctx context.Context, s Store, cfg WLCfg, conns int, dur, ttl time.Duration) {
	deadline := time.Now().Add(dur)
	var wg sync.WaitGroup
	for i := 0; i < conns; i++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			wl := cfg.New(seed)
			for time.Now().Before(deadline) {
				k := wl.NextKey()
				_ = s.Set(ctx, KeyStr(k), valueOf(cfg.DataSize, k), ttl)
			}
		}(int64(i) + 1)
	}
	wg.Wait()
}

func rates() []int {
	var out []int
	for _, p := range strings.Split(env("RATES", "25000,50000,100000,200000,400000,600000,800000,1000000"), ",") {
		if n, err := strconv.Atoi(strings.TrimSpace(p)); err == nil {
			out = append(out, n)
		}
	}
	return out
}

// runOpenLoop is the MODE=open driver: warm the keyspace, then sweep the offered
// rate ladder, recording CO-correct latency (incl. p99.9), hit/miss, and the
// achieved throughput at each offered rate.
func runOpenLoop(ctx context.Context, s Store, target string) {
	cfg := WLCfg{
		KeyMax:   uint64(mustInt(env("KEY_MAX", "1000000"))),
		ZipfS:    mustFloat(env("ZIPF_S", "1.1")),
		DataSize: mustInt(env("DATA_SIZE", "128")),
		ReadFrac: readFraction(env("RATIO", "1:1")),
	}
	conns := mustInt(env("CONNS", "50"))
	stageDur := time.Duration(mustInt(env("STAGE_SECS", "5"))) * time.Second
	warmup := time.Duration(mustInt(env("WARMUP_SECS", "3"))) * time.Second
	ttl := time.Duration(mustInt(env("TTL_SECS", "3600"))) * time.Second
	out := env("OUT", "results-open-"+target+".json")

	log.Printf("[%s] OPEN-LOOP: conns=%d keyspace=%d zipfS=%.2f data=%dB ratio=%s (read=%.0f%%)",
		target, conns, cfg.KeyMax, cfg.ZipfS, cfg.DataSize, env("RATIO", "1:1"), cfg.ReadFrac*100)
	log.Printf("[%s] warmup writes %s ...", target, warmup)
	warmupWrites(ctx, s, cfg, conns, warmup, ttl)

	res := Result{Target: target}
	for _, rate := range rates() {
		t0 := time.Now()
		o := runOpen(ctx, s, cfg, conns, float64(rate), stageDur, ttl)
		t1 := time.Now()
		hr := 0.0
		if o.hits+o.misses > 0 {
			hr = 100 * float64(o.hits) / float64(o.hits+o.misses)
		}
		st := Stage{
			Level:       conns,
			TargetRate:  rate,
			AchievedRPS: o.achieved,
			Set:         stat("set", o.set, stageDur),
			Get:         stat("get", o.get, stageDur),
			Errs:        o.errs,
			Mismatch:    o.mism,
			Hits:        o.hits,
			Misses:      o.misses,
			TStartMs:    t0.UnixMilli(),
			TEndMs:      t1.UnixMilli(),
		}
		res.Stages = append(res.Stages, st)
		// tail across whichever op dominates the mix
		p99 := st.Get.P99us
		p999 := st.Get.P999us
		if st.Set.Count > st.Get.Count {
			p99, p999 = st.Set.P99us, st.Set.P999us
		}
		verdict := ""
		if o.mism > 0 {
			verdict = fmt.Sprintf("  *** %d MISMATCHES ***", o.mism)
		}
		log.Printf("[%s] offered=%-8d achieved=%-9.0f p99=%.0fus p99.9=%.0fus hit=%.1f%% errs=%d%s",
			target, rate, o.achieved, p99, p999, hr, o.errs, verdict)
	}

	b, _ := json.MarshalIndent(res, "", "  ")
	if err := os.WriteFile(out, b, 0o644); err != nil {
		log.Fatalf("write %s: %v", out, err)
	}
	fmt.Printf("wrote %s (%d rate stages)\n", out, len(res.Stages))
}

func mustFloat(s string) float64 {
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		log.Fatalf("bad float %q: %v", s, err)
	}
	return f
}
