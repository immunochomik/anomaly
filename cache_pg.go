package main

import (
	"context"
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

func (c *pgCache) Get(ctx context.Context, key string) ([]byte, bool, error) {
	var raw []byte
	err := c.pool.QueryRow(ctx, `SELECT value FROM anomaly_cache WHERE key = $1`, key).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	}
	return raw, err == nil, err
}

func (c *pgCache) Put(ctx context.Context, key string, v []byte) error {
	_, err := c.pool.Exec(ctx,
		`INSERT INTO anomaly_cache (key, value) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value`,
		key, v)
	return err
}

func (c *pgCache) Keys(ctx context.Context, prefix string) ([]string, error) {
	rows, err := c.pool.Query(ctx, `SELECT key FROM anomaly_cache WHERE key LIKE $1 || '%' ORDER BY key DESC`, prefix)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			return nil, err
		}
		out = append(out, k)
	}
	return out, rows.Err()
}

func (c *pgCache) Close() error { c.pool.Close(); return nil }
