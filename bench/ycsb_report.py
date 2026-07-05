#!/usr/bin/env python3
"""Parse go-ycsb run output into the YCSB workload matrix, print it, and bake the
dashboard. Reads the TOTAL line of each <backend>-<workload>.txt.

    python3 bench/ycsb_report.py <ycsb-out-dir> <backend...>
"""
import glob
import json
import os
import re
import sys

WL_NAMES = {"a": "A · update-heavy", "b": "B · read-heavy", "c": "C · read-only",
            "d": "D · read-latest", "e": "E · scan", "f": "F · read-modify-write"}
TOTAL = re.compile(
    r"^TOTAL\s*-.*OPS:\s*([\d.]+).*\s99th\(us\):\s*(\d+),\s*99\.9th\(us\):\s*(\d+)", re.M)


def parse(path):
    m = TOTAL.search(open(path).read())
    if not m:
        return None
    return {"ops": round(float(m.group(1)), 0), "p99": int(m.group(2)), "p999": int(m.group(3))}


def main():
    ydir, backends = sys.argv[1], sys.argv[2:]
    combined = {}
    for b in backends:
        wls = {}
        for f in sorted(glob.glob(os.path.join(ydir, f"{b}-*.txt"))):
            w = os.path.basename(f)[len(b) + 1:-4]
            r = parse(f)
            if r:
                wls[w] = r
        if wls:
            combined[b] = wls
    out = os.path.join(os.path.dirname(ydir), "ycsb-combined.json")
    json.dump(combined, open(out, "w"), indent=2)
    print(f"    wrote {os.path.relpath(out)}")
    bake(combined)

    workloads = sorted({w for wl in combined.values() for w in wl})
    print("\n    YCSB matrix — throughput (ops/s), p99 (us):")
    hdr = "    {:<18}".format("backend") + "".join(f"{WL_NAMES.get(w, w).split(' ')[0]:>10}" for w in workloads)
    print(hdr)
    for b, wl in sorted(combined.items(), key=lambda kv: -sum(x["ops"] for x in kv[1].values())):
        row = "    {:<18}".format(b) + "".join(f"{int(wl[w]['ops']):>10,}" if w in wl else f"{'-':>10}" for w in workloads)
        print(row)


def bake(combined):
    here = os.path.dirname(os.path.abspath(__file__))
    html = os.path.join(here, "deploy", "ycsb.html")
    if not os.path.exists(html):
        return
    s = open(html).read()
    block = "// __YCSB_DATA_START__\nconst D=" + json.dumps(combined) + ";\n// __YCSB_DATA_END__"
    i, j = s.find("// __YCSB_DATA_START__"), s.find("// __YCSB_DATA_END__")
    if i < 0 or j < 0:
        return
    open(html, "w").write(s[:i] + block + s[j + len("// __YCSB_DATA_END__"):])
    print(f"    baked {os.path.relpath(html)}")


if __name__ == "__main__":
    main()
