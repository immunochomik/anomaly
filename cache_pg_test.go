package main

import (
	"context"
	"os"
	"testing"
)

// Run with: PG_TEST_DSN=postgres://user:pass@host/db go test -run Ring ./...
func TestPgRingEvictsOldest(t *testing.T) {
	dsn := os.Getenv("PG_TEST_DSN")
	if dsn == "" {
		t.Skip("PG_TEST_DSN not set")
	}
	ctx := context.Background()
	c, err := newPgCache(ctx, cacheConfig{DSN: dsn, Capacity: 3})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.pool.Exec(ctx, `TRUNCATE anomaly_ring; ALTER SEQUENCE anomaly_ring_slot RESTART`); err != nil {
		t.Fatal(err)
	}

	for _, k := range []string{"k1", "k2", "k3"} {
		if err := c.Put(ctx, k, []byte(`"`+k+`"`)); err != nil {
			t.Fatal(err)
		}
	}
	// update in place must not consume a slot
	if err := c.Put(ctx, "k2", []byte(`"k2b"`)); err != nil {
		t.Fatal(err)
	}
	// 4th key wraps to slot 1 and evicts k1
	if err := c.Put(ctx, "k4", []byte(`"k4"`)); err != nil {
		t.Fatal(err)
	}

	if _, ok, _ := c.Get(ctx, "k1"); ok {
		t.Fatal("k1 should be evicted")
	}
	if v, ok, _ := c.Get(ctx, "k2"); !ok || string(v) != `"k2b"` {
		t.Fatalf("k2 = %s %v", v, ok)
	}
	keys, err := c.Keys(ctx, "k")
	if err != nil || len(keys) != 3 {
		t.Fatalf("keys = %v %v", keys, err)
	}
	// shrinking capacity below current slot restarts the sequence instead of failing
	c2, err := newPgCache(ctx, cacheConfig{DSN: dsn, Capacity: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	if err := c2.Put(ctx, "k5", []byte(`"k5"`)); err != nil {
		t.Fatal(err)
	}
}
