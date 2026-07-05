// Realistic workload model for the open-loop load generator: a Zipfian keyspace
// (hotspot access, so GETs of not-yet-written keys miss — a real hit/miss ratio),
// a configurable read/write ratio, and fixed-size, per-key-deterministic values
// (so every hit is still verifiable). Each worker holds its own *WL (its own RNG),
// so no goroutine shares a random source.
package main

import (
	"math/rand"
	"strconv"
	"strings"
)

// WLCfg is the shared, immutable workload configuration; per-worker instances are
// spun from it with New(seed).
type WLCfg struct {
	KeyMax   uint64  // number of distinct keys (the keyspace)
	ZipfS    float64 // Zipf exponent (>1); higher = hotter skew
	DataSize int     // value size in bytes
	ReadFrac float64 // fraction of ops that are GETs
}

// WL is a single worker's workload instance (owns its RNG — not goroutine-safe).
type WL struct {
	cfg  WLCfg
	rng  *rand.Rand
	zipf *rand.Zipf
}

func (c WLCfg) New(seed int64) *WL {
	r := rand.New(rand.NewSource(seed))
	s := c.ZipfS
	if s <= 1 {
		s = 1.1
	}
	kmax := c.KeyMax
	if kmax < 2 {
		kmax = 2
	}
	// rand.NewZipf draws from [0, imax]; use KeyMax-1 so keys stay in [0, KeyMax).
	z := rand.NewZipf(r, s, 1, kmax-1)
	return &WL{cfg: c, rng: r, zipf: z}
}

// NextKey returns a Zipf-distributed key index in [0, KeyMax).
func (w *WL) NextKey() uint64 { return w.zipf.Uint64() }

// IsRead reports whether the next op should be a GET (vs a SET), by ReadFrac.
func (w *WL) IsRead() bool { return w.rng.Float64() < w.cfg.ReadFrac }

// KeyStr is the string key for a key index (stable across workers).
func KeyStr(n uint64) string { return "k:" + strconv.FormatUint(n, 10) }

// Value is the deterministic, DataSize-long value for a key. Because it depends
// only on the key, a GET that hits can be verified against it — the benchmark
// stays self-validating even with a shared keyspace.
func (w *WL) Value(n uint64) []byte { return valueOf(w.cfg.DataSize, n) }

// valueOf is the pure, RNG-free form of Value — safe to call from any worker.
func valueOf(size int, n uint64) []byte {
	b := make([]byte, size)
	h := []byte("val:" + strconv.FormatUint(n, 10) + ":")
	for i := range b {
		b[i] = h[i%len(h)]
	}
	return b
}

// readFraction parses a memtier-style "set:get" ratio into the GET fraction.
// Malformed input defaults to 0.5 (an even split).
func readFraction(s string) float64 {
	parts := strings.Split(s, ":")
	if len(parts) != 2 {
		return 0.5
	}
	set, err1 := strconv.ParseFloat(strings.TrimSpace(parts[0]), 64)
	get, err2 := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
	if err1 != nil || err2 != nil || set+get <= 0 {
		return 0.5
	}
	return get / (set + get)
}
