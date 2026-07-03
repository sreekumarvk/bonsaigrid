#!/usr/bin/env python3
"""Convert the loadgen's combined.json into Bencher Metric Format (BMF).

BMF is Bencher's language-agnostic ingest format:

    { "<benchmark>": { "<measure>": { "value": N, "lower_value": .., "upper_value": .. } } }

We emit one benchmark per (backend, concurrency level) and four measures:
throughput (ops/s, higher is better), set/get p99 latency (us, lower is better),
memory working-set (MB, lower is better) and cpu (% of budget). Bencher tracks
each (benchmark, measure) series per branch+commit and can gate regressions.

    python3 bench/to_bmf.py [combined.json] > bmf.json
"""
import json
import sys


def to_bmf(combined: dict) -> dict:
    bmf: dict = {}
    for backend, stages in combined.items():
        for s in stages or []:
            name = f"{backend}/level_{s['level']}"
            m = {
                "throughput": {"value": s["set"]["rps"]},
                "latency-set-p99": {"value": s["set"]["p99_us"]},
                "latency-get-p99": {"value": s["get"]["p99_us"]},
            }
            if isinstance(s.get("cpu"), dict):
                m["cpu"] = {"value": s["cpu"]["avg_pct"]}
            if isinstance(s.get("mem"), dict):
                m["memory"] = {"value": s["mem"]["avg_mb"]}
            # Surface correctness: a nonzero mismatch means the run is invalid.
            if s.get("mismatch"):
                m["mismatch"] = {"value": s["mismatch"]}
            bmf[name] = m
    return bmf


def main() -> None:
    path = sys.argv[1] if len(sys.argv) > 1 else "bench/loadgen/combined.json"
    with open(path) as f:
        combined = json.load(f)
    json.dump(to_bmf(combined), sys.stdout, indent=2)
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
