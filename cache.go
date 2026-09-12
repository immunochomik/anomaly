package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// Cache is a key/value store. Windows ("w|...") are immutable; runs ("r|...") hold verdict pages.
type Cache interface {
	Get(ctx context.Context, key string) ([]byte, bool, error)
	Put(ctx context.Context, key string, v []byte) error
	Keys(ctx context.Context, prefix string) ([]string, error) // sorted descending
	Close() error
}

func windowKey(cfg config, from time.Time) string {
	return fmt.Sprintf("w|%s|%s|%s", cfg.hash, from.UTC().Format(time.RFC3339), cfg.Window)
}

func runPrefix(cfg config) string { return "r|" + cfg.hash + "|" }

func runKey(cfg config, window time.Time) string {
	return runPrefix(cfg) + window.UTC().Format(time.RFC3339)
}

func getJSON(ctx context.Context, c Cache, key string, v any) (bool, error) {
	raw, ok, err := c.Get(ctx, key)
	if err != nil || !ok {
		return ok, err
	}
	return true, json.Unmarshal(raw, v)
}

func putJSON(ctx context.Context, c Cache, key string, v any) error {
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return c.Put(ctx, key, raw)
}

func newCache(ctx context.Context, cc cacheConfig) (Cache, error) {
	switch cc.Type {
	case "memory":
		return &memCache{m: map[string][]byte{}}, nil
	case "disk":
		return newDiskCache(cc.Path)
	case "postgres":
		return newPgCache(ctx, cc.DSN)
	}
	return nil, fmt.Errorf("unknown cache type %q", cc.Type)
}

type memCache struct {
	mu sync.RWMutex
	m  map[string][]byte
}

func (c *memCache) Get(_ context.Context, key string) ([]byte, bool, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	v, ok := c.m[key]
	return v, ok, nil
}

func (c *memCache) Put(_ context.Context, key string, v []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.m[key] = v
	return nil
}

func (c *memCache) Keys(_ context.Context, prefix string) ([]string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var out []string
	for k := range c.m {
		if strings.HasPrefix(k, prefix) {
			out = append(out, k)
		}
	}
	sort.Sort(sort.Reverse(sort.StringSlice(out)))
	return out, nil
}

func (c *memCache) Close() error { return nil }

// diskCache stores one file per key; file name is the hex-encoded key so it can be listed back.
type diskCache struct{ dir string }

func newDiskCache(dir string) (*diskCache, error) {
	if dir == "" {
		dir = ".cache"
	}
	return &diskCache{dir: dir}, os.MkdirAll(dir, 0o755)
}

func (c *diskCache) path(key string) string {
	return filepath.Join(c.dir, hex.EncodeToString([]byte(key))+".json")
}

func (c *diskCache) Get(_ context.Context, key string) ([]byte, bool, error) {
	raw, err := os.ReadFile(c.path(key))
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	return raw, err == nil, err
}

func (c *diskCache) Put(_ context.Context, key string, v []byte) error {
	tmp := c.path(key) + ".tmp"
	if err := os.WriteFile(tmp, v, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, c.path(key))
}

func (c *diskCache) Keys(_ context.Context, prefix string) ([]string, error) {
	entries, err := os.ReadDir(c.dir)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, e := range entries {
		name, ok := strings.CutSuffix(e.Name(), ".json")
		if !ok {
			continue
		}
		b, err := hex.DecodeString(name)
		if err == nil && strings.HasPrefix(string(b), prefix) {
			out = append(out, string(b))
		}
	}
	sort.Sort(sort.Reverse(sort.StringSlice(out)))
	return out, nil
}

func (c *diskCache) Close() error { return nil }
