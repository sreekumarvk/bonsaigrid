// Staged closed-loop load generator. For each concurrency level, `level` workers
// loop Set(user)+Get(user) for a fixed duration; per-op latencies feed p50/p90/p99
// and achieved throughput. Results are written as JSON for graphing. TARGET picks
// the backend; the same code path runs every backend for a fair comparison.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type OpStat struct {
	Op    string  `json:"op"`
	Count int64   `json:"count"`
	RPS   float64 `json:"rps"`
	P50us float64 `json:"p50_us"`
	P90us float64 `json:"p90_us"`
	P99us float64 `json:"p99_us"`
}

type Stage struct {
	Level    int    `json:"level"`
	Set      OpStat `json:"set"`
	Get      OpStat `json:"get"`
	Errs     int64  `json:"errors"`
	TStartMs int64  `json:"t_start_ms"` // wall-clock stage window, for aligning
	TEndMs   int64  `json:"t_end_ms"`   // externally-sampled server CPU/mem
}

type Result struct {
	Target string  `json:"target"`
	Stages []Stage `json:"stages"`
}

func pctile(s []int64, p float64) float64 {
	if len(s) == 0 {
		return 0
	}
	i := int(p * float64(len(s)))
	if i >= len(s) {
		i = len(s) - 1
	}
	return float64(s[i])
}

func levels() []int {
	raw := env("LEVELS", "1,2,4,8,16,32,64,128")
	var out []int
	for _, p := range strings.Split(raw, ",") {
		if n, err := strconv.Atoi(strings.TrimSpace(p)); err == nil {
			out = append(out, n)
		}
	}
	return out
}

// runStage runs `level` closed-loop workers for `dur`, returning merged latency
// samples (microseconds) for set and get plus an error count.
func runStage(ctx context.Context, s Store, level int, dur time.Duration) ([]int64, []int64, int64) {
	deadline := time.Now().Add(dur)
	var wg sync.WaitGroup
	sets := make([][]int64, level)
	gets := make([][]int64, level)
	errs := make([]int64, level)
	ttl := 60 * time.Second
	for w := 0; w < level; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for time.Now().Before(deadline) {
				u := NewUser()
				val := u.JSON()
				t0 := time.Now()
				if err := s.Set(ctx, u.Uuid, val, ttl); err != nil {
					errs[w]++
					continue
				}
				sets[w] = append(sets[w], time.Since(t0).Microseconds())
				t1 := time.Now()
				if _, err := s.Get(ctx, u.Uuid); err != nil {
					errs[w]++
					continue
				}
				gets[w] = append(gets[w], time.Since(t1).Microseconds())
			}
		}(w)
	}
	wg.Wait()
	var setAll, getAll []int64
	var errTot int64
	for w := 0; w < level; w++ {
		setAll = append(setAll, sets[w]...)
		getAll = append(getAll, gets[w]...)
		errTot += errs[w]
	}
	return setAll, getAll, errTot
}

func stat(op string, lat []int64, dur time.Duration) OpStat {
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	return OpStat{
		Op:    op,
		Count: int64(len(lat)),
		RPS:   float64(len(lat)) / dur.Seconds(),
		P50us: pctile(lat, 0.50),
		P90us: pctile(lat, 0.90),
		P99us: pctile(lat, 0.99),
	}
}

func main() {
	target := os.Getenv("TARGET")
	if target == "" {
		log.Fatalln("set TARGET (bonsaigrid|hazelcast|redis|memcached)")
	}
	stageDur := time.Duration(mustInt(env("STAGE_SECS", "4"))) * time.Second
	warmup := time.Duration(mustInt(env("WARMUP_SECS", "2"))) * time.Second
	out := env("OUT", "results-"+target+".json")

	ctx := context.Background()
	s, err := NewStore(ctx, target)
	if err != nil {
		log.Fatalf("connect %s: %v", target, err)
	}
	defer s.Close()

	log.Printf("[%s] warmup %s ...", target, warmup)
	runStage(ctx, s, 16, warmup) // discarded

	res := Result{Target: target}
	for _, level := range levels() {
		log.Printf("[%s] level=%d for %s ...", target, level, stageDur)
		t0 := time.Now()
		setL, getL, errs := runStage(ctx, s, level, stageDur)
		t1 := time.Now()
		res.Stages = append(res.Stages, Stage{
			Level:    level,
			Set:      stat("set", setL, stageDur),
			Get:      stat("get", getL, stageDur),
			Errs:     errs,
			TStartMs: t0.UnixMilli(),
			TEndMs:   t1.UnixMilli(),
		})
		st := res.Stages[len(res.Stages)-1]
		log.Printf("[%s] level=%d set: %.0f rps p99=%.0fus | get: %.0f rps p99=%.0fus | errs=%d",
			target, level, st.Set.RPS, st.Set.P99us, st.Get.RPS, st.Get.P99us, errs)
	}

	b, _ := json.MarshalIndent(res, "", "  ")
	if err := os.WriteFile(out, b, 0o644); err != nil {
		log.Fatalf("write %s: %v", out, err)
	}
	fmt.Printf("wrote %s (%d stages)\n", out, len(res.Stages))
}

func mustInt(s string) int {
	n, err := strconv.Atoi(s)
	if err != nil {
		log.Fatalf("bad int %q: %v", s, err)
	}
	return n
}
