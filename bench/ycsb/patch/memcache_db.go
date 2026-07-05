// Package memcache is a go-ycsb DB driver for the memcached text protocol
// (via gomemcache). go-ycsb ships no memcache driver, so without this the YCSB
// matrix can only target RESP backends. This mirrors go-ycsb's redis STRING
// datatype: each record is a JSON {field:value} blob stored under "table/key".
// It is dropped into the go-ycsb source tree by bench/run-ycsb.sh at build time,
// so the matrix can also target memcached and BonsaiGrid's memcached protocol.
package memcache

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

type mc struct {
	client     *memcache.Client
	fieldcount int64
}

func rowKey(table, key string) string { return table + "/" + key }

func (m *mc) Close() error                                                   { return nil }
func (m *mc) InitThread(ctx context.Context, _ int, _ int) context.Context   { return ctx }
func (m *mc) CleanupThread(_ context.Context)                                {}

func (m *mc) Read(_ context.Context, table, key string, _ []string) (map[string][]byte, error) {
	it, err := m.client.Get(rowKey(table, key))
	if err != nil {
		return nil, err // ErrCacheMiss surfaces as a read miss, like redis.Nil
	}
	data := make(map[string][]byte)
	if err := json.Unmarshal(it.Value, &data); err != nil {
		return nil, err
	}
	return data, nil
}

func (m *mc) Scan(_ context.Context, _ string, _ string, _ int, _ []string) ([]map[string][]byte, error) {
	return nil, fmt.Errorf("scan is not supported by the memcache driver")
}

func (m *mc) Insert(_ context.Context, table, key string, values map[string][]byte) error {
	data, err := json.Marshal(values)
	if err != nil {
		return err
	}
	return m.client.Set(&memcache.Item{Key: rowKey(table, key), Value: data})
}

func (m *mc) Update(_ context.Context, table, key string, values map[string][]byte) error {
	// Partial update: read-modify-write the JSON blob (a full update overwrites it).
	if int64(len(values)) < m.fieldcount {
		if it, err := m.client.Get(rowKey(table, key)); err == nil {
			cur := make(map[string][]byte)
			if json.Unmarshal(it.Value, &cur) == nil {
				for f, v := range values {
					cur[f] = v
				}
				values = cur
			}
		}
	}
	data, err := json.Marshal(values)
	if err != nil {
		return err
	}
	return m.client.Set(&memcache.Item{Key: rowKey(table, key), Value: data})
}

func (m *mc) Delete(_ context.Context, table, key string) error {
	if err := m.client.Delete(rowKey(table, key)); err != nil && err != memcache.ErrCacheMiss {
		return err
	}
	return nil
}

type creator struct{}

func (creator) Create(p *properties.Properties) (ycsb.DB, error) {
	hosts := p.GetString("memcache.hosts", "127.0.0.1:11211")
	c := memcache.New(strings.Split(hosts, ",")...)
	c.MaxIdleConns = int(p.GetInt64(prop.ThreadCount, prop.ThreadCountDefault)) + 8
	return &mc{client: c, fieldcount: p.GetInt64(prop.FieldCount, prop.FieldCountDefault)}, nil
}

func init() {
	ycsb.RegisterDBCreator("memcache", creator{})
}
