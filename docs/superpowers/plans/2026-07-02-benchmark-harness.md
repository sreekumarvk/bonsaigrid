# Benchmark Harness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A four-backend (BonsaiGrid, Hazelcast, Redis, Memcached) staged-load Go loadgen with a client-side latency histogram, plus single-box deploy + reporting — modeled on Anton Putra's lesson 225.

**Architecture:** `bench/loadgen/` is a Go program: a `Store` interface with four implementations (`hazelcast-go-client` for both `bonsaigrid` and `hazelcast`; `go-redis`; `gomemcache`), a staged closed-loop ramp in `main.go`, and a Prometheus `request_duration_seconds{op,target}` histogram. `bench/deploy/` holds docker-compose + Prometheus + Grafana + run/report scripts. The Go client doubles as a BonsaiGrid Go-client conformance check — de-risked in Task 1 before anything else.

**Tech Stack:** Go 1.22+; `github.com/hazelcast/hazelcast-go-client`, `github.com/redis/go-redis/v9`, `github.com/bradfitz/gomemcache/memcache`, `github.com/prometheus/client_golang`, `github.com/google/uuid`, `gopkg.in/yaml.v3`. Docker Compose; Prometheus; Grafana.

**Spec:** `docs/superpowers/specs/2026-07-02-benchmark-harness-design.md`

## Global Constraints

- **Fairness:** identical op sequence + timing per backend; no backend special-cased in the hot loop. Latency is measured **client-side** around each `Set`/`Get`.
- **Baseline topology:** single node, no backups, no persistence, for every backend (matches single-instance Redis/Memcached). Extra-capability lanes are Phase 4.
- **Value:** send a `[]byte` (JSON `User`) as-is to every backend — no per-backend serialization tricks; identical for `bonsaigrid` and `hazelcast`.
- **`TARGET`** env selects the backend: `bonsaigrid` | `hazelcast` | `redis` | `memcached`.
- **Go-client smoke gates the build:** Task 1 must pass (Go client ↔ BonsaiGrid) before the ramp is built.
- **Cluster name** for the Hazelcast protocol is `dev` (BonsaiGrid default `BONSAI_CLUSTER=dev`).

---

## File Structure

- `bench/loadgen/go.mod`, `go.sum` — module `bonsaigrid/bench`.
- `bench/loadgen/config.go` — `Config` struct + `LoadConfig(path)`.
- `bench/loadgen/config.yaml` — hosts/ports, ttl, ramp params.
- `bench/loadgen/store.go` — `Store` interface + `NewStore(target, cfg)`.
- `bench/loadgen/store_hz.go` — `hzStore` (hazelcast-go-client).
- `bench/loadgen/store_redis.go` — `redisStore`.
- `bench/loadgen/store_mc.go` — `mcStore`.
- `bench/loadgen/user.go` — `User` payload + `NewUser()`.
- `bench/loadgen/metrics.go` — Prometheus registry, gauge, histogram, `/metrics`.
- `bench/loadgen/main.go` — staged ramp driver.
- `bench/loadgen/ramp_test.go`, `config_test.go`, `store_test.go` — unit tests.
- `bench/loadgen/cmd/smoke/main.go` — Task-1 Go-client smoke check.
- `bench/deploy/docker-compose.yml`, `prometheus.yml`, `grafana-dashboard.json`,
  `run.sh`, `report.sh`, `Makefile`, `README.md`, `RESULTS.md`.

---

## Phase 1 — loadgen

### Task 1: Module scaffold + Go-client smoke check (the de-risk)

**Files:** Create `bench/loadgen/go.mod`, `bench/loadgen/cmd/smoke/main.go`.

**Interfaces — Produces:** a runnable `cmd/smoke` that connects the
`hazelcast-go-client` to a host and does one `Set`/`Get`, exiting non-zero on
failure. This is the conformance gate for BonsaiGrid.

- [ ] **Step 1: Init the module.** Create `bench/loadgen/go.mod`:

```
module bonsaigrid/bench

go 1.22
```

Then `cd bench/loadgen && go get github.com/hazelcast/hazelcast-go-client@latest`.

- [ ] **Step 2: Write the smoke program.** Create `bench/loadgen/cmd/smoke/main.go`:

```go
// Smoke check: connect the Hazelcast Go client to HOST and do one Set/Get.
// Used first against BonsaiGrid to confirm Go-client conformance before the ramp
// is built. Exit non-zero on any failure.
package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/hazelcast/hazelcast-go-client"
)

func main() {
	host := os.Getenv("HOST") // e.g. 127.0.0.1:5701
	if host == "" {
		host = "127.0.0.1:5701"
	}
	ctx := context.Background()

	cfg := hazelcast.NewConfig()
	cfg.Cluster.Name = "dev"
	cfg.Cluster.Network.SetAddresses(host)
	cfg.Cluster.Unisocket = true // one connection; don't require full partition table

	client, err := hazelcast.StartNewClientWithConfig(ctx, cfg)
	if err != nil {
		log.Fatalf("connect %s failed: %v", host, err)
	}
	defer client.Shutdown(ctx)

	m, err := client.GetMap(ctx, "bench")
	if err != nil {
		log.Fatalf("GetMap failed: %v", err)
	}
	if err := m.SetWithTTL(ctx, "smoke-key", []byte("hello"), 30*time.Second); err != nil {
		log.Fatalf("Set failed: %v", err)
	}
	v, err := m.Get(ctx, "smoke-key")
	if err != nil {
		log.Fatalf("Get failed: %v", err)
	}
	log.Printf("OK: got %q from %s", v, host)
}
```

- [ ] **Step 3: Build it.** Run: `cd bench/loadgen && go build ./cmd/smoke` — Expected: compiles (downloads the Go client). `go vet ./...`.

- [ ] **Step 4: Run against BonsaiGrid (the gate).** In one shell: build + start the server — `cargo build -p server && BONSAI_CLUSTER=dev ./target/debug/server` (binds `5701`). In another: `cd bench/loadgen && HOST=127.0.0.1:5701 go run ./cmd/smoke`. Expected: `OK: got "hello" from 127.0.0.1:5701`.
  - **If it fails:** the Go client hit a BonsaiGrid gap (connect/auth handshake or a codec). Capture the exact error and the `BONSAI_DEBUG`/server log; open a tracked sub-task to close that specific gap (typically the client-connection or a `Client*`/`MapSet` codec). Do NOT proceed to Task 2 until the smoke passes — the harness has surfaced its first finding.

- [ ] **Step 5: Commit.**

```bash
git add bench/loadgen/go.mod bench/loadgen/go.sum bench/loadgen/cmd
git commit -m "bench: Go-client smoke check (Hazelcast Go client -> BonsaiGrid)"
```

### Task 2: `Store` interface + four backends

**Files:** Create `bench/loadgen/store.go`, `store_hz.go`, `store_redis.go`, `store_mc.go`, `store_test.go`; add deps.

**Interfaces:**
- Consumes: `Config` (defined here as a minimal struct; fleshed out in Task 3 — keep field names stable: `HzHost, RedisHost, McHost, MapName string`).
- Produces:
  - `type Store interface { Set(ctx, key string, val []byte, ttl time.Duration) error; Get(ctx, key string) ([]byte, error); Close() error }`
  - `func NewStore(target string, cfg Config) (Store, error)` — `bonsaigrid`/`hazelcast` → `hzStore` (host from the matching config field); `redis` → `redisStore`; `memcached` → `mcStore`.

- [ ] **Step 1: Add deps.** `cd bench/loadgen && go get github.com/redis/go-redis/v9 github.com/bradfitz/gomemcache/memcache`.

- [ ] **Step 2: Write the interface + selector.** Create `bench/loadgen/store.go`:

```go
package main

import (
	"context"
	"fmt"
	"time"
)

// Store is the backend-agnostic key/value surface the workload drives. Every
// backend implements the SAME two timed operations so the comparison is fair.
type Store interface {
	Set(ctx context.Context, key string, val []byte, ttl time.Duration) error
	Get(ctx context.Context, key string) ([]byte, error)
	Close() error
}

// NewStore builds the backend selected by target. The two Hazelcast-protocol
// targets share one implementation (only the host differs).
func NewStore(ctx context.Context, target string, cfg Config) (Store, error) {
	switch target {
	case "bonsaigrid":
		return newHzStore(ctx, cfg.BonsaiHost, cfg.MapName)
	case "hazelcast":
		return newHzStore(ctx, cfg.HzHost, cfg.MapName)
	case "redis":
		return newRedisStore(cfg.RedisHost), nil
	case "memcached":
		return newMcStore(cfg.McHost), nil
	default:
		return nil, fmt.Errorf("unknown TARGET %q", target)
	}
}
```

- [ ] **Step 3: Implement the three backends.** Create `bench/loadgen/store_hz.go`:

```go
package main

import (
	"context"
	"time"

	"github.com/hazelcast/hazelcast-go-client"
)

type hzStore struct {
	client *hazelcast.Client
	m      *hazelcast.Map
}

func newHzStore(ctx context.Context, host, mapName string) (Store, error) {
	cfg := hazelcast.NewConfig()
	cfg.Cluster.Name = "dev"
	cfg.Cluster.Network.SetAddresses(host)
	c, err := hazelcast.StartNewClientWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	m, err := c.GetMap(ctx, mapName)
	if err != nil {
		return nil, err
	}
	return &hzStore{client: c, m: m}, nil
}

func (s *hzStore) Set(ctx context.Context, key string, val []byte, ttl time.Duration) error {
	return s.m.SetWithTTL(ctx, key, val, ttl)
}
func (s *hzStore) Get(ctx context.Context, key string) ([]byte, error) {
	v, err := s.m.Get(ctx, key)
	if err != nil || v == nil {
		return nil, err
	}
	if b, ok := v.([]byte); ok {
		return b, nil
	}
	return []byte(v.(string)), nil
}
func (s *hzStore) Close() error { return s.client.Shutdown(context.Background()) }
```

Create `bench/loadgen/store_redis.go`:

```go
package main

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

type redisStore struct{ rdb *redis.Client }

func newRedisStore(host string) Store {
	return &redisStore{rdb: redis.NewClient(&redis.Options{Addr: host, PoolSize: 500})}
}
func (s *redisStore) Set(ctx context.Context, key string, val []byte, ttl time.Duration) error {
	return s.rdb.Set(ctx, key, val, ttl).Err()
}
func (s *redisStore) Get(ctx context.Context, key string) ([]byte, error) {
	return s.rdb.Get(ctx, key).Bytes()
}
func (s *redisStore) Close() error { return s.rdb.Close() }
```

Create `bench/loadgen/store_mc.go`:

```go
package main

import (
	"context"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
)

type mcStore struct{ mc *memcache.Client }

func newMcStore(host string) Store {
	c := memcache.New(host)
	c.MaxIdleConns = 500
	return &mcStore{mc: c}
}
func (s *mcStore) Set(_ context.Context, key string, val []byte, ttl time.Duration) error {
	return s.mc.Set(&memcache.Item{Key: key, Value: val, Expiration: int32(ttl.Seconds())})
}
func (s *mcStore) Get(_ context.Context, key string) ([]byte, error) {
	it, err := s.mc.Get(key)
	if err != nil {
		return nil, err
	}
	return it.Value, nil
}
func (s *mcStore) Close() error { return nil }
```

- [ ] **Step 4: Write the selector test.** Create `bench/loadgen/store_test.go`:

```go
package main

import (
	"context"
	"testing"
)

func TestNewStoreSelectsByTarget(t *testing.T) {
	// redis/memcached construct without a live server (lazy connections).
	cfg := Config{RedisHost: "127.0.0.1:6379", McHost: "127.0.0.1:11211"}
	for _, target := range []string{"redis", "memcached"} {
		s, err := NewStore(context.Background(), target, cfg)
		if err != nil {
			t.Fatalf("%s: %v", target, err)
		}
		_ = s.Close()
	}
	if _, err := NewStore(context.Background(), "nope", cfg); err == nil {
		t.Fatal("unknown target must error")
	}
}
```

- [ ] **Step 5: Run + verify.** Run: `cd bench/loadgen && go test -run TestNewStore ./...` — Expected: PASS (hz targets need a live server, so they're covered by Task 1's smoke, not this unit test). `go build ./...`.

- [ ] **Step 6: Commit.**

```bash
git add bench/loadgen
git commit -m "bench: Store interface + hazelcast/redis/memcached backends"
```

### Task 3: Config, User payload, Prometheus metrics

**Files:** Create `bench/loadgen/config.go`, `config.yaml`, `user.go`, `metrics.go`, `config_test.go`; add deps.

**Interfaces:**
- Produces: `Config` (full struct + `LoadConfig(path) (Config, error)`); `NewUser() *User` + `(*User).JSON() []byte`; `metrics{ stage Gauge; duration *HistogramVec }` + `NewMetrics(reg)`; `StartMetricsServer(port, reg)`.

- [ ] **Step 1: Add deps.** `cd bench/loadgen && go get github.com/prometheus/client_golang/prometheus github.com/prometheus/client_golang/prometheus/promhttp github.com/google/uuid gopkg.in/yaml.v3`.

- [ ] **Step 2: Config.** Create `bench/loadgen/config.go`:

```go
package main

import (
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	MetricsPort int    `yaml:"metricsPort"`
	Target      string `yaml:"target"`
	BonsaiHost  string `yaml:"bonsaiHost"`
	HzHost      string `yaml:"hzHost"`
	RedisHost   string `yaml:"redisHost"`
	McHost      string `yaml:"mcHost"`
	MapName     string `yaml:"mapName"`
	TTLSeconds  int    `yaml:"ttlSeconds"`
	Test        struct {
		MinClients     int `yaml:"minClients"`
		MaxClients     int `yaml:"maxClients"`
		StageIntervalS int `yaml:"stageIntervalS"`
		RequestDelayMs int `yaml:"requestDelayMs"`
		WarmupStages   int `yaml:"warmupStages"`
	} `yaml:"test"`
}

func LoadConfig(path string) (Config, error) {
	var c Config
	b, err := os.ReadFile(path)
	if err != nil {
		return c, err
	}
	if err := yaml.Unmarshal(b, &c); err != nil {
		return c, err
	}
	if t := os.Getenv("TARGET"); t != "" {
		c.Target = t
	}
	return c, nil
}
```

Create `bench/loadgen/config.yaml`:

```yaml
metricsPort: 8081
target: bonsaigrid
bonsaiHost: 127.0.0.1:5701
hzHost: 127.0.0.1:5702
redisHost: 127.0.0.1:6379
mcHost: 127.0.0.1:11211
mapName: bench
ttlSeconds: 60
test:
  minClients: 1
  maxClients: 200
  stageIntervalS: 20
  requestDelayMs: 0
  warmupStages: 2
```

- [ ] **Step 3: Config test.** Create `bench/loadgen/config_test.go`:

```go
package main

import (
	"os"
	"testing"
)

func TestLoadConfigAndEnvOverride(t *testing.T) {
	c, err := LoadConfig("config.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if c.MapName != "bench" || c.Test.MaxClients != 200 {
		t.Fatalf("unexpected config: %+v", c)
	}
	os.Setenv("TARGET", "redis")
	defer os.Unsetenv("TARGET")
	c2, _ := LoadConfig("config.yaml")
	if c2.Target != "redis" {
		t.Fatalf("TARGET env should override, got %q", c2.Target)
	}
}
```

Run: `go test -run TestLoadConfig ./...` — Expected: PASS.

- [ ] **Step 4: User payload.** Create `bench/loadgen/user.go`:

```go
package main

import (
	"encoding/json"
	"math/rand"

	"github.com/google/uuid"
)

type User struct {
	Uuid, Username, FirstName, LastName, Address string
}

func NewUser() *User {
	return &User{
		Uuid:      uuid.NewString(),
		Username:  randStr(10),
		FirstName: randStr(5),
		LastName:  randStr(10),
		Address:   randStr(20),
	}
}

func (u *User) JSON() []byte {
	b, _ := json.Marshal(u)
	return b
}

const letters = "abcdefghijklmnopqrstuvwxyz"

func randStr(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
```

- [ ] **Step 5: Metrics.** Create `bench/loadgen/metrics.go` — port the reference's `metrics.go` verbatim, but label the histogram `{op, target}` (not `db`) and keep the full fine bucket list from the reference (`0.00001 … 5.0`). Provide `NewMetrics(reg) *metrics`, `StartMetricsServer(port int, reg *prometheus.Registry)`, and the `metrics{ stage prometheus.Gauge; duration *prometheus.HistogramVec }` struct. (Copy the bucket slice exactly from the spec's reference — do not shorten it; the fineness is what makes p99 accurate.)

- [ ] **Step 6: Build + commit.** Run: `go build ./... && go test ./...` (unit tests only) — Expected: PASS.

```bash
git add bench/loadgen
git commit -m "bench: config, User payload, Prometheus metrics (op,target histogram)"
```

### Task 4: Staged ramp driver (`main.go`)

**Files:** Create `bench/loadgen/main.go`, `bench/loadgen/ramp_test.go`.

**Interfaces:**
- Consumes: `Config`, `NewStore`, `NewUser`, `metrics`.
- Produces: `func runRamp(ctx, s Store, m *metrics, target string, t TestCfg, now func() time.Time)` — the pure scheduler (injectable clock + `Store` for testing); `main()` wires config → store → metrics → `runRamp`.

- [ ] **Step 1: Write the ramp test with a stub store.** Create `bench/loadgen/ramp_test.go`:

```go
package main

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type stubStore struct{ ops int64 }

func (s *stubStore) Set(context.Context, string, []byte, time.Duration) error {
	atomic.AddInt64(&s.ops, 1)
	return nil
}
func (s *stubStore) Get(context.Context, string) ([]byte, error) { return []byte("x"), nil }
func (s *stubStore) Close() error                                { return nil }

func TestRampReachesMaxAndDoesWork(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)
	st := &stubStore{}
	tc := TestCfg{MinClients: 1, MaxClients: 3, StageIntervalS: 0, RequestDelayMs: 0, WarmupStages: 0}
	runRamp(context.Background(), st, m, "stub", tc, time.Now)
	if atomic.LoadInt64(&st.ops) == 0 {
		t.Fatal("ramp performed no operations")
	}
}
```

- [ ] **Step 2: Run it to verify it fails.** Run: `go test -run TestRamp ./...` — Expected: FAIL to compile (`TestCfg`, `runRamp`, `NewMetrics` names).

- [ ] **Step 3: Implement `main.go`.** Create `bench/loadgen/main.go`:

```go
// Staged closed-loop load generator: concurrency ramps minClients..maxClients,
// +1 per stage; each stage runs stageIntervalS; per request Set(user) then
// Get(user), both timed into the Prometheus histogram. TARGET picks the backend.
package main

import (
	"context"
	"log"
	"time"
)

type TestCfg struct {
	MinClients, MaxClients, StageIntervalS, RequestDelayMs, WarmupStages int
}

func main() {
	cfg, err := LoadConfig("config.yaml")
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	ctx := context.Background()
	reg := newRegistry()
	m := NewMetrics(reg)
	StartMetricsServer(cfg.MetricsPort, reg)

	s, err := NewStore(ctx, cfg.Target, cfg)
	if err != nil {
		log.Fatalf("connect %s: %v", cfg.Target, err)
	}
	defer s.Close()

	log.Printf("running ramp: target=%s max=%d", cfg.Target, cfg.Test.MaxClients)
	runRamp(ctx, s, m, cfg.Target, TestCfg(cfg.Test), time.Now)
}

func runRamp(ctx context.Context, s Store, m *metrics, target string, t TestCfg, now func() time.Time) {
	ttl := 60 * time.Second
	clients := t.MinClients
	stage := 0
	for {
		m.stage.Set(float64(clients))
		sem := make(chan struct{}, clients)
		start := now()
		for {
			sem <- struct{}{}
			go func() {
				defer func() { <-sem }()
				if t.RequestDelayMs > 0 {
					time.Sleep(time.Duration(t.RequestDelayMs) * time.Millisecond)
				}
				u := NewUser()
				val := u.JSON()

				st := now()
				if err := s.Set(ctx, u.Uuid, val, ttl); err == nil {
					m.duration.WithLabelValues("set", target).Observe(now().Sub(st).Seconds())
				}
				st = now()
				if _, err := s.Get(ctx, u.Uuid); err == nil {
					m.duration.WithLabelValues("get", target).Observe(now().Sub(st).Seconds())
				}
			}()
			if t.StageIntervalS == 0 || now().Sub(start).Seconds() >= float64(t.StageIntervalS) {
				break
			}
		}
		stage++
		_ = stage
		if clients >= t.MaxClients {
			break
		}
		clients++
	}
}
```

Add a `newRegistry()` helper in `metrics.go` (`return prometheus.NewRegistry()`), and confirm `NewMetrics`/`StartMetricsServer` signatures match. (The `WarmupStages` field is honored by discarding the first N stages in the report query — see Task 6 — not by resetting metrics mid-run.)

- [ ] **Step 4: Run to verify pass.** Run: `go test ./...` — Expected: PASS (ramp + config + store selector). `go build ./... && go vet ./...`.

- [ ] **Step 5: End-to-end smoke against BonsaiGrid.** With the server running (Task 1): `TARGET=bonsaigrid go run .` for a few seconds; `curl -s localhost:8081/metrics | grep request_duration_seconds | head`. Expected: non-empty histogram samples with `target="bonsaigrid"`. Ctrl-C.

- [ ] **Step 6: Commit.**

```bash
git add bench/loadgen
git commit -m "bench: staged closed-loop ramp driver + main wiring"
```

---

## Phase 2 — deploy

### Task 5: docker-compose + Prometheus + run scripts

**Files:** Create `bench/deploy/docker-compose.yml`, `prometheus.yml`, `run.sh`, `Makefile`, `README.md`.

**Interfaces:** No Go code. Produces the reproducible run environment.

- [ ] **Step 1: Compose the backends + Prometheus.** Create `bench/deploy/docker-compose.yml` with services: `hazelcast` (`hazelcast/hazelcast:latest`, env `HZ_CLUSTERNAME=dev`, port `5702:5701`), `redis` (`redis:7`, `6379:6379`), `memcached` (`memcached:1.6`, `11211:11211`), `prometheus` (`prom/prometheus`, mount `./prometheus.yml`, `9090:9090`), and optional `grafana` (`grafana/grafana`, `3000:3000`). BonsaiGrid is NOT in compose (run the freshly-built binary on `5701`).

- [ ] **Step 2: Prometheus scrape.** Create `bench/deploy/prometheus.yml` scraping the loadgen host `host.docker.internal:8081` (job `loadgen`) every `1s`.

- [ ] **Step 3: Run script.** Create `bench/deploy/run.sh` taking `TARGET`: brings up the matching backend (compose service, or reminds to start BonsaiGrid via `cargo build -p server && BONSAI_CLUSTER=dev ./target/debug/server`), then runs `cd bench/loadgen && TARGET=$TARGET go run .`. Create a `Makefile` with `make up`, `make down`, `make bench TARGET=…`, `make bonsai` (build+run the server).

- [ ] **Step 4: Document.** Create `bench/deploy/README.md`: prerequisites (Docker, Go, a JDK-free Hazelcast via the image), the exact commands to run each of the four targets, and how to open Prometheus/Grafana.

- [ ] **Step 5: Verify.** Run: `cd bench/deploy && docker compose up -d hazelcast redis memcached prometheus`; then `TARGET=hazelcast`, `TARGET=redis`, `TARGET=memcached` runs each produce metrics; `curl -s localhost:9090/api/v1/query?query=myapp_stage` returns data. `docker compose down`.

- [ ] **Step 6: Commit.**

```bash
git add bench/deploy
git commit -m "bench: docker-compose backends + Prometheus + run scripts"
```

---

## Phase 3 — reporting

### Task 6: Grafana dashboard + report script + RESULTS template

**Files:** Create `bench/deploy/grafana-dashboard.json`, `bench/deploy/report.sh`, `bench/deploy/RESULTS.md`.

- [ ] **Step 1: Report script.** Create `bench/deploy/report.sh`: for each `target` and `op`, query Prometheus for p50/p90/p99 via `histogram_quantile(0.99, sum by (le,target,op)(rate(myapp_request_duration_seconds_bucket[1m])))` and achieved throughput via `sum by (target,op)(rate(myapp_request_duration_seconds_count[1m]))`, and print a markdown table (skip the first `warmupStages` of samples by starting the range after warm-up). Take the Prometheus URL as `$1` (default `localhost:9090`).

- [ ] **Step 2: Grafana dashboard.** Create `bench/deploy/grafana-dashboard.json`: panels for p50/p90/p99 `set` and `get` latency and RPS, each faceted by `target`, x-axis time (aligned to the `myapp_stage` gauge). Keep it minimal and importable.

- [ ] **Step 3: RESULTS template.** Create `bench/deploy/RESULTS.md`: a table skeleton (target × op × {p50,p90,p99,RPS}) plus an **Environment** block (CPU model + cores, kernel, io_uring, backend versions) and a **Methodology/Caveats** section lifted from the spec (single-node/no-backups baseline; Redis single-threaded; client-side latency).

- [ ] **Step 4: Verify.** Run a short bench for two targets, then `bash bench/deploy/report.sh` — Expected: a markdown table with numbers for both. Import the dashboard into Grafana and confirm panels render.

- [ ] **Step 5: Commit.**

```bash
git add bench/deploy
git commit -m "bench: Grafana dashboard + report script + RESULTS template"
```

---

## Phase 4 — BonsaiGrid capability lanes

### Task 7: Extra-capability runs (backups / persistence / CP)

**Files:** Modify `bench/deploy/run.sh`, `bench/deploy/README.md`; optionally add a `bench/loadgen/cmd/cplane/` for the CP `AtomicLong` lane.

- [ ] **Step 1: Persistence + backups lanes.** Add `run.sh` variants that start BonsaiGrid with `BONSAI_PERSISTENCE=async BONSAI_PERSISTENCE_DIR=…` and with `BONSAI_MEMBERS=3` (backups), and run `TARGET=bonsaigrid` against each. Record results under distinct Prometheus `target` labels (e.g. `bonsaigrid-persist`, `bonsaigrid-3node`) by passing a `LABEL` env the loadgen appends to `target`.
- [ ] **Step 2: CP AtomicLong lane.** Add `bench/loadgen/cmd/cplane/main.go` — the Go client's `CPSubsystem().GetAtomicLong` doing `AddAndGet` in the same ramp shape, recording `op="incr"`, `target="bonsaigrid-cp"`. (Requires the server started with `BONSAI_CP`.)
- [ ] **Step 3: Document + record.** Update `RESULTS.md` with the extra lanes and what they quantify (the cost of durability / backups / linearizability).
- [ ] **Step 4: Commit.**

```bash
git add bench
git commit -m "bench: BonsaiGrid capability lanes (persistence, backups, CP)"
```

---

## Self-Review

**Spec coverage:** Store interface + 4 backends (Task 2), hazelcast-go-client for both HZ targets (Task 2 `newHzStore`), User payload + fine-bucket histogram `{op,target}` (Task 3), staged closed-loop ramp (Task 4), Go-client conformance de-risk FIRST (Task 1), docker-compose single-box deploy + Prometheus (Task 5), Grafana + report + methodology/env capture (Task 6), BonsaiGrid capability lanes (Task 7). Fairness controls and the `[]byte` value rule are enforced in the code (identical `runRamp`/`Store` per target). All spec sections map to a task.

**Placeholder scan:** the two "port verbatim" items (the metrics bucket slice in Task 3; the Grafana JSON in Task 6) reference concrete sources (the spec's reference bucket list / a minimal importable dashboard) rather than "TBD" — acceptable, but the implementer must copy the full bucket slice, not shorten it (called out inline).

**Type consistency:** `Config` fields (`BonsaiHost/HzHost/RedisHost/McHost/MapName/Test.*`) are defined in Task 3 and consumed unchanged in Tasks 2/4; `Store{Set,Get,Close}` (Task 2) is used by `runRamp` (Task 4) and the stub (test); `NewStore(ctx, target, cfg)`, `NewMetrics(reg)`, `StartMetricsServer(port, reg)`, `TestCfg`, and `runRamp(ctx, s, m, target, t, now)` are consistent across their definition and call sites. (Note: Task 2 introduces a minimal `Config` for compilation; Task 3 replaces it with the full struct — the implementer should define the full `Config` in Task 3 and not duplicate it in Task 2, i.e. write Task 2's `store.go` referencing the fields Task 3 will declare, and land both before `go build`.)
