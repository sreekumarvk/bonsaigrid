# Python conformance smoke test

Runs an unmodified `hazelcast-python-client` against BonsaiGrid as a fast,
JVM-free liveness/compat check.

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
# in the repo root: cargo run -p server &
.venv/bin/python smoke.py    # prints PYTHON SMOKE OK
```

This is the increment-0 success oracle alongside the Java parity harness
(`../conformance-java`), which is the canonical check but needs JDK 17+.
