package main

import (
	"bytes"
	"testing"
)

func TestReadFraction(t *testing.T) {
	cases := map[string]float64{
		"1:1": 0.5, // set:get
		"1:9": 0.9, // 1 set : 9 get  → 90% reads
		"9:1": 0.1,
		"0:1": 1.0,
		"1:0": 0.0,
		"bad": 0.5, // malformed → default
	}
	for in, want := range cases {
		if got := readFraction(in); got != want {
			t.Errorf("readFraction(%q)=%v want %v", in, got, want)
		}
	}
}

func TestValueDeterministicAndSized(t *testing.T) {
	c := WLCfg{KeyMax: 1000, ZipfS: 1.1, DataSize: 64, ReadFrac: 0.5}
	w := c.New(1)
	a, b := w.Value(42), w.Value(42)
	if !bytes.Equal(a, b) {
		t.Fatal("Value not deterministic for the same key")
	}
	if len(a) != 64 {
		t.Fatalf("Value len=%d want 64 (== DataSize)", len(a))
	}
	if bytes.Equal(w.Value(42), w.Value(43)) {
		t.Fatal("Value should differ across keys")
	}
	// tiny payloads still honor DataSize exactly
	if got := len((WLCfg{DataSize: 3}).New(1).Value(7)); got != 3 {
		t.Fatalf("tiny Value len=%d want 3", got)
	}
}

func TestNextKeyInRange(t *testing.T) {
	c := WLCfg{KeyMax: 100, ZipfS: 1.2, DataSize: 16, ReadFrac: 0.5}
	w := c.New(7)
	for i := 0; i < 20000; i++ {
		if k := w.NextKey(); k >= 100 {
			t.Fatalf("NextKey returned %d, out of [0,100)", k)
		}
	}
}

func TestZipfIsSkewed(t *testing.T) {
	// A skewed distribution must make low keys far more frequent than a
	// uniform draw would — the whole point of a hotspot workload.
	c := WLCfg{KeyMax: 10000, ZipfS: 1.3, DataSize: 16, ReadFrac: 0.5}
	w := c.New(3)
	const N = 100000
	hot := 0
	for i := 0; i < N; i++ {
		if w.NextKey() < 100 { // top 1% of the keyspace
			hot++
		}
	}
	// Uniform would put ~1% here; a hot Zipf must be well above that.
	if frac := float64(hot) / N; frac < 0.20 {
		t.Fatalf("Zipf not skewed: only %.1f%% of draws hit the hot 1%%", frac*100)
	}
}

func TestIsReadHonorsFraction(t *testing.T) {
	w := (WLCfg{KeyMax: 10, ZipfS: 1.1, DataSize: 8, ReadFrac: 0.8}).New(5)
	reads := 0
	const N = 100000
	for i := 0; i < N; i++ {
		if w.IsRead() {
			reads++
		}
	}
	if frac := float64(reads) / N; frac < 0.78 || frac > 0.82 {
		t.Fatalf("IsRead fraction %.3f, want ~0.80", frac)
	}
}
