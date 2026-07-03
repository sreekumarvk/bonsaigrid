package main

import (
	"context"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/hazelcast/hazelcast-go-client"
)

// hzStore drives the Hazelcast client wire protocol (BonsaiGrid and stock
// Hazelcast both speak it). The official Go client is a *smart* client that opens
// a single connection per member and multiplexes every invocation over it via
// correlation ids. Against a single member that funnels all worker goroutines
// through one TCP connection — and therefore one server reactor core — which
// caps throughput regardless of how many cores the server has. The pooled
// memcached/redis drivers, by contrast, open one connection per active worker.
//
// To make the comparison fair we hold a pool of independent clients (each its own
// connection) and round-robin operations across them, so concurrent workers land
// on different connections and, on BonsaiGrid, different reactor cores. Size the
// pool with HZ_CONNS (default 64); set it >= the top concurrency level.
type hzStore struct {
	clients []*hazelcast.Client
	maps    []*hazelcast.Map
	ctr     atomic.Uint64
}

func newHzStore(ctx context.Context, host, mapName string) (Store, error) {
	n, err := strconv.Atoi(env("HZ_CONNS", "64"))
	if err != nil || n < 1 {
		n = 64
	}
	s := &hzStore{}
	for i := 0; i < n; i++ {
		cfg := hazelcast.NewConfig()
		cfg.Cluster.Name = env("HZ_CLUSTER", "dev")
		cfg.Cluster.Network.SetAddresses(host)
		c, err := hazelcast.StartNewClientWithConfig(ctx, cfg)
		if err != nil {
			s.Close() // tear down any partial pool
			return nil, err
		}
		m, err := c.GetMap(ctx, mapName)
		if err != nil {
			s.Close()
			return nil, err
		}
		s.clients = append(s.clients, c)
		s.maps = append(s.maps, m)
	}
	return s, nil
}

// pick returns the next map in round-robin order. Atomic so concurrent workers
// spread deterministically across the pool without a lock.
func (s *hzStore) pick() *hazelcast.Map {
	i := s.ctr.Add(1) - 1
	return s.maps[int(i%uint64(len(s.maps)))]
}

func (s *hzStore) Set(ctx context.Context, key string, val []byte, ttl time.Duration) error {
	return s.pick().SetWithTTL(ctx, key, val, ttl)
}

func (s *hzStore) Get(ctx context.Context, key string) ([]byte, error) {
	v, err := s.pick().Get(ctx, key)
	if err != nil || v == nil {
		return nil, err
	}
	switch x := v.(type) {
	case []byte:
		return x, nil
	case string:
		return []byte(x), nil
	default:
		return nil, nil
	}
}

func (s *hzStore) Close() error {
	for _, c := range s.clients {
		if c != nil {
			_ = c.Shutdown(context.Background())
		}
	}
	return nil
}
