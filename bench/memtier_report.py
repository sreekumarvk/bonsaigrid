#!/usr/bin/env python3
"""Merge memtier_benchmark per-level JSON into one summary + a Bencher Metric Format
file, and print a table. memtier gives the industry metrics our loadgen doesn't:
p99.9 tail latency, hit/miss ratio, and network throughput.

    python3 bench/memtier_report.py <memtier-json-dir> <backend...>
"""
import glob
import json
import os
import re
import sys


def level_of(path):
    m = re.search(r"-(\d+)\.json$", path)
    return int(m.group(1)) if m else 0


def parse(path):
    d = json.load(open(path))["ALL STATS"]
    T, G = d["Totals"], d["Gets"]
    p = T.get("Percentile Latencies", {})
    h, m = G.get("Hits/sec", 0.0), G.get("Misses/sec", 0.0)
    return {
        "ops": round(T["Ops/sec"], 1),
        "p50_us": round(p.get("p50.00", 0) * 1000, 1),
        "p99_us": round(p.get("p99.00", 0) * 1000, 1),
        "p999_us": round(p.get("p99.90", 0) * 1000, 1),
        "hit_ratio": round(100 * h / (h + m), 1) if (h + m) > 0 else 100.0,
        "kb_s": round(T.get("KB/sec RX", 0) + T.get("KB/sec TX", 0), 0),
    }


def main():
    mtdir, backends = sys.argv[1], sys.argv[2:]
    combined, bmf = {}, {}
    for b in backends:
        stages = []
        for f in sorted(glob.glob(os.path.join(mtdir, f"{b}-*.json")), key=level_of):
            lvl = level_of(f)
            try:
                s = parse(f)
            except Exception as e:
                print(f"  (skip {b} level {lvl}: {e})")
                continue
            s["level"] = lvl
            stages.append(s)
            bmf[f"{b}/level_{lvl}"] = {
                "throughput": {"value": s["ops"]},
                "latency-p50": {"value": s["p50_us"]},
                "latency-p99": {"value": s["p99_us"]},
                "latency-p999": {"value": s["p999_us"]},
                "hit-ratio": {"value": s["hit_ratio"]},
                "network-kbps": {"value": s["kb_s"]},
            }
        if stages:
            combined[b] = stages

    out = os.path.join(os.path.dirname(mtdir), "memtier-combined.json")
    json.dump(combined, open(out, "w"), indent=2)
    json.dump(bmf, open(os.path.join(os.path.dirname(mtdir), "memtier-bmf.json"), "w"), indent=2)
    print(f"    wrote {os.path.relpath(out)} + memtier-bmf.json ({len(bmf)} benchmarks)")
    bake_dashboard(combined)


def bake_dashboard(combined):
    """Regenerate the self-contained memtier dashboard from this run's data."""
    here = os.path.dirname(os.path.abspath(__file__))
    html_path = os.path.join(here, "deploy", "memtier.html")
    if not os.path.exists(html_path):
        return
    levels = combined[next(iter(combined))] if combined else []
    levels = [s["level"] for s in levels] if levels else [1, 2, 4, 8, 16, 32, 64, 128]
    lines = ["// __MEMTIER_DATA_START__", f"let LEVELS={json.dumps(levels)};", "const D={"]
    for k, st in combined.items():
        ops = [int(s["ops"]) for s in st]
        p999 = [s["p999_us"] for s in st]
        hit = [s["hit_ratio"] for s in st]
        net = [int(s["kb_s"]) for s in st]
        lines.append(f' "{k}":{{ops:{ops},p999:{p999},hit:{hit},net:{net}}},'.replace(" ", ""))
    lines.append("};")
    lines.append("// __MEMTIER_DATA_END__")
    block = "\n".join(lines)
    html = open(html_path).read()
    i = html.find("// __MEMTIER_DATA_START__")
    j = html.find("// __MEMTIER_DATA_END__")
    if i < 0 or j < 0:
        return
    html = html[:i] + block + html[j + len("// __MEMTIER_DATA_END__"):]
    open(html_path, "w").write(html)
    print(f"    baked {os.path.relpath(html_path)}")

    # peak-level table
    print("\n    memtier — peak stage (highest level):")
    print(f"    {'backend':<18}{'conns':>6}{'ops/s':>12}{'p50':>9}{'p99':>9}{'p99.9':>9}{'hit%':>7}{'MB/s':>8}")
    rows = [(b, st[-1]) for b, st in combined.items() if st]
    for b, s in sorted(rows, key=lambda r: -r[1]["ops"]):
        def us(v):
            return f"{v/1000:.1f}ms" if v >= 1000 else f"{int(v)}us"
        print(f"    {b:<18}{s['level']:>6}{int(s['ops']):>12,}{us(s['p50_us']):>9}"
              f"{us(s['p99_us']):>9}{us(s['p999_us']):>9}{s['hit_ratio']:>6.1f}{s['kb_s']/1024:>8.0f}")


if __name__ == "__main__":
    main()
