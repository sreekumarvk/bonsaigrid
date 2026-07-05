#!/usr/bin/env python3
"""Bake the one-page benchmark report (bench/deploy/index.html) from whatever results
are present. Dumps each benchmark's raw *-combined.json (+ gc.json) into the page; the
page renders the summary AND the charts inline, so it is fully self-contained (works
opened directly, served, or published as a single artifact — no sibling files needed).

    python3 bench/gen_index.py           # run automatically at the end of benchmark-all.sh
"""
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
LOAD = os.path.join(HERE, "loadgen")
HTML = os.path.join(HERE, "deploy", "index.html")


def load(name):
    try:
        return json.load(open(os.path.join(LOAD, name)))
    except Exception:
        return None


def main():
    data = {
        "closed": load("combined.json"),
        "gc": load("gc.json"),
        "memtier": load("memtier-combined.json"),
        "openloop": load("openloop-combined.json"),
        "ycsb": load("ycsb-combined.json"),
    }
    present = [k for k in ("closed", "memtier", "openloop", "ycsb") if data.get(k)]
    if not present:
        sys.exit("no benchmark results found under bench/loadgen/ — run some benchmarks first")
    block = ("// __REPORT_START__\nconst DATA=" + json.dumps(data)
             + ";\nconst PRESENT=" + json.dumps(present) + ";\n// __REPORT_END__")
    s = open(HTML).read()
    i, j = s.find("// __REPORT_START__"), s.find("// __REPORT_END__")
    if i < 0 or j < 0:
        sys.exit(f"markers not found in {HTML}")
    open(HTML, "w").write(s[:i] + block + s[j + len("// __REPORT_END__"):])
    print(f"    baked {os.path.relpath(HTML)} with: {', '.join(present)}")


if __name__ == "__main__":
    main()
