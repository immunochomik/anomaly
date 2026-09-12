package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Cache stores fetched windows. Past windows are immutable, so entries never expire.
type Cache interface {
	Get(ctx context.Context, key string) (windowValues, bool, error)
	Put(ctx context.Context, key string, v windowValues) error
	Close() error
}

func cacheKey(cfg config, from time.Time) string {
	return fmt.Sprintf("%s|%s|%s", cfg.hash, from.UTC().Format(time.RFC3339), cfg.Window)
}

func newCache(ctx context.Context, cc cacheConfig) (Cache, error) {
	switch cc.Type {
	case "memory":
		return &memCache{m: map[string]windowValues{}}, nil
	case "disk":
		return newDiskCache(cc.Path)
	case "postgres":
		return newPgCache(ctx, cc.DSN)
	}
	return nil, fmt.Errorf("unknown cache type %q", cc.Type)
}

type memCache struct {
	mu sync.RWMutex
	m  map[string]windowValues
}

func (c *memCache) Get(_ context.Context, key string) (windowValues, bool, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	v, ok := c.m[key]
	return v, ok, nil
}

func (c *memCache) Put(_ context.Context, key string, v windowValues) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.m[key] = v
	return nil
}

func (c *memCache) Close() error { return nil }

type diskCache struct{ dir string }

func newDiskCache(dir string) (*diskCache, error) {
	if dir == "" {
		dir = ".cache"
	}
	return &diskCache{dir: dir}, os.MkdirAll(dir, 0o755)
}

func (c *diskCache) path(key string) string {
	sum := sha256.Sum256([]byte(key))
	return filepath.Join(c.dir, hex.EncodeToString(sum[:8])+".json")
}

func (c *diskCache) Get(_ context.Context, key string) (windowValues, bool, error) {
	raw, err := os.ReadFile(c.path(key))
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	var v windowValues
	if err := json.Unmarshal(raw, &v); err != nil {
		return nil, false, err
	}
	return v, true, nil
}

func (c *diskCache) Put(_ context.Context, key string, v windowValues) error {
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	tmp := c.path(key) + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, c.path(key))
}

func (c *diskCache) Close() error { return nil }
