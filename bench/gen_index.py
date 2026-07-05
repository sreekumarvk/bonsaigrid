#!/usr/bin/env python3
"""Bake the combined report page (bench/deploy/index.html) from whatever benchmark
results are present. Reads each benchmark's *-combined.json (+ gc.json) and writes an
executive summary; the page links to each full interactive dashboard.

    python3 bench/gen_index.py           # run automatically at the end of run-all.sh
"""
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
LOAD = os.path.join(HERE, "loadgen")
HTML = os.path.join(HERE, "deploy", "index.html")
SLO_US = 10000.0


def load(name):
    p = os.path.join(LOAD, name)
    try:
        return json.load(open(p))
    except Exception:
        return None


def build():
    report, present = {}, []

    cj = load("combined.json")
    if cj:
        peak = {b: round(st[-1]["set"]["rps"]) for b, st in cj.items() if st}
        if peak:
            report["closed"] = {"peak": peak, "gc": (load("gc.json") or {})}
            present.append("closed")

    mt = load("memtier-combined.json")
    if mt:
        report["memtier"] = {b: {"ops": int(st[-1]["ops"]), "p999": st[-1]["p999_us"],
                                 "hit": st[-1]["hit_ratio"]} for b, st in mt.items() if st}
        present.append("memtier")

    ol = load("openloop-combined.json")
    if ol:
        def usable(st):
            xs = [s["achieved"] for s in st if s["p99"] <= SLO_US and s["achieved"] >= 0.9 * s["rate"]]
            return int(max(xs)) if xs else 0
        report["openloop"] = {"slo_ms": int(SLO_US / 1000),
                              "b": {b: {"usable": usable(st), "hit": st[-1]["hit"]} for b, st in ol.items() if st}}
        present.append("openloop")

    yc = load("ycsb-combined.json")
    if yc:
        report["ycsb"] = {b: {w: int(v["ops"]) for w, v in wl.items()} for b, wl in yc.items()}
        present.append("ycsb")

    return report, present


def main():
    report, present = build()
    if not present:
        sys.exit("no benchmark results found under bench/loadgen/ — run some benchmarks first")
    block = ("// __REPORT_START__\nconst REPORT=" + json.dumps(report)
             + ";\nconst PRESENT=" + json.dumps(present) + ";\n// __REPORT_END__")
    s = open(HTML).read()
    i, j = s.find("// __REPORT_START__"), s.find("// __REPORT_END__")
    if i < 0 or j < 0:
        sys.exit(f"markers not found in {HTML}")
    open(HTML, "w").write(s[:i] + block + s[j + len("// __REPORT_END__"):])
    print(f"    baked {os.path.relpath(HTML)} with: {', '.join(present)}")


if __name__ == "__main__":
    main()
