#!/usr/bin/env python3
"""Merge open-loop (coordinated-omission-correct) results into one summary and bake
the elbow dashboard. The headline number is USABLE THROUGHPUT: the highest offered
rate the server sustains while keeping p99 under an SLO — what a closed-loop test
cannot measure.

    python3 bench/openloop_report.py <loadgen-dir> <backend...>
"""
import json
import os
import sys

SLO_US = float(os.environ.get("SLO_US", "10000"))  # p99 must stay under this (default 10ms)


def load(loaddir, b):
    p = os.path.join(loaddir, f"results-open-{b}.json")
    if not os.path.exists(p):
        return None
    stages = json.load(open(p)).get("stages", [])
    out = []
    for s in stages:
        # the tail of whichever op dominates the mix
        op = s["set"] if s["set"]["count"] > s["get"]["count"] else s["get"]
        hits, misses = s.get("hits", 0), s.get("misses", 0)
        out.append({
            "rate": s["target_rate"],
            "achieved": round(s.get("achieved_rps", 0), 0),
            "p50": op["p50_us"], "p99": op["p99_us"], "p999": op["p999_us"],
            "hit": round(100 * hits / (hits + misses), 1) if (hits + misses) else 100.0,
            "errs": s.get("errors", 0),
        })
    return out or None


def usable(stages):
    """Highest offered rate with p99 <= SLO and achieved >= 90% of offered."""
    best = 0.0
    for s in stages:
        if s["p99"] <= SLO_US and s["achieved"] >= 0.9 * s["rate"]:
            best = max(best, s["achieved"])
    return best


def main():
    loaddir, backends = sys.argv[1], sys.argv[2:]
    combined = {}
    for b in backends:
        st = load(loaddir, b)
        if st:
            combined[b] = st
    json.dump(combined, open(os.path.join(loaddir, "openloop-combined.json"), "w"), indent=2)
    bake(combined)

    print(f"\n    open-loop — usable throughput (p99 <= {SLO_US/1000:.0f}ms, achieved >= 90% of offered):")
    print(f"    {'backend':<20}{'usable ops/s':>14}{'hit%':>7}")
    for b, st in sorted(combined.items(), key=lambda kv: -usable(kv[1])):
        print(f"    {b:<20}{int(usable(st)):>14,}{st[-1]['hit']:>7.1f}")


def bake(combined):
    here = os.path.dirname(os.path.abspath(__file__))
    html = os.path.join(here, "deploy", "openloop.html")
    if not os.path.exists(html):
        return
    rates = combined[next(iter(combined))] if combined else []
    rates = [s["rate"] for s in rates] if rates else []
    lines = ["// __OL_DATA_START__", f"let RATES={json.dumps(rates)};", "const D={"]
    for k, st in combined.items():
        f = lambda key: [s[key] for s in st]  # noqa: E731
        lines.append(f' "{k}":{{achieved:{[int(x) for x in f("achieved")]},p99:{f("p99")},p999:{f("p999")},hit:{f("hit")}}},'.replace(" ", ""))
    lines.append("};")
    lines.append("// __OL_DATA_END__")
    block = "\n".join(lines)
    s = open(html).read()
    i, j = s.find("// __OL_DATA_START__"), s.find("// __OL_DATA_END__")
    if i < 0 or j < 0:
        return
    open(html, "w").write(s[:i] + block + s[j + len("// __OL_DATA_END__"):])
    print(f"    baked {os.path.relpath(html)}")


if __name__ == "__main__":
    main()
