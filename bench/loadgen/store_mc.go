package main

import (
	"context"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
)

type mcStore struct{ mc *memcache.Client }

func newMcStore(host string) Store {
	c := memcache.New(host)
	c.MaxIdleConns = 512
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
