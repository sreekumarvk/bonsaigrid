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
	cfg.Cluster.Name = env("HZ_CLUSTER", "dev")
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
	switch x := v.(type) {
	case []byte:
		return x, nil
	case string:
		return []byte(x), nil
	default:
		return nil, nil
	}
}
func (s *hzStore) Close() error { return s.client.Shutdown(context.Background()) }
