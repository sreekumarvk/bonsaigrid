package main

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

type redisStore struct{ rdb *redis.Client }

func newRedisStore(host string) Store {
	return &redisStore{rdb: redis.NewClient(&redis.Options{Addr: host, PoolSize: 512})}
}
func (s *redisStore) Set(ctx context.Context, key string, val []byte, ttl time.Duration) error {
	return s.rdb.Set(ctx, key, val, ttl).Err()
}
func (s *redisStore) Get(ctx context.Context, key string) ([]byte, error) {
	return s.rdb.Get(ctx, key).Bytes()
}
func (s *redisStore) Close() error { return s.rdb.Close() }
