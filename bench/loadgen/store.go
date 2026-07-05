package main

import (
	"context"
	"fmt"
	"os"
	"time"
)

// Store is the backend-agnostic key/value surface. Every backend implements the
// same two timed operations so the comparison is fair.
type Store interface {
	Set(ctx context.Context, key string, val []byte, ttl time.Duration) error
	Get(ctx context.Context, key string) ([]byte, error)
	Close() error
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// NewStore builds the backend selected by target. The two Hazelcast-protocol
// targets share one client implementation (only the host differs).
func NewStore(ctx context.Context, target string) (Store, error) {
	mapName := env("MAP_NAME", "bench")
	switch target {
	case "bonsaigrid":
		return newHzStore(ctx, env("BONSAI_HOST", "127.0.0.1:5701"), mapName)
	case "hazelcast":
		return newHzStore(ctx, env("HZ_HOST", "127.0.0.1:5702"), mapName)
	case "redis":
		return newRedisStore(env("REDIS_HOST", "127.0.0.1:6379")), nil
	case "memcached":
		return newMcStore(env("MC_HOST", "127.0.0.1:11211")), nil
	case "bonsaigrid-mc":
		// BonsaiGrid driven through its memcached ASCII protocol with the thin
		// gomemcache client — the apples-to-apples number vs real Memcached.
		return newMcStore(env("BGMC_HOST", "127.0.0.1:5701")), nil
	case "bonsaigrid-redis":
		// BonsaiGrid driven through its RESP protocol with the thin go-redis
		// client — the apples-to-apples number vs real Redis.
		return newRedisStore(env("BGREDIS_HOST", "127.0.0.1:5701")), nil
	default:
		return nil, fmt.Errorf("unknown TARGET %q", target)
	}
}
