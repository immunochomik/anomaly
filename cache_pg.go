package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type pgCache struct{ pool *pgxpool.Pool }

func newPgCache(ctx context.Context, dsn string) (*pgCache, error) {
	if dsn == "" {
		return nil, fmt.Errorf("postgres cache: dsn (or CACHE_DSN env) required")
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	_, err = pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS anomaly_cache (
		key text PRIMARY KEY, value jsonb NOT NULL, created_at timestamptz NOT NULL DEFAULT now())`)
	if err != nil {
		pool.Close()
		return nil, err
	}
	return &pgCache{pool: pool}, nil
}

func (c *pgCache) Get(ctx context.Context, key string) (windowValues, bool, error) {
	var raw []byte
	err := c.pool.QueryRow(ctx, `SELECT value FROM anomaly_cache WHERE key = $1`, key).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	var v windowValues
	return v, true, json.Unmarshal(raw, &v)
}

func (c *pgCache) Put(ctx context.Context, key string, v windowValues) error {
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = c.pool.Exec(ctx,
		`INSERT INTO anomaly_cache (key, value) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value`,
		key, raw)
	return err
}

func (c *pgCache) Close() error { c.pool.Close(); return nil }
