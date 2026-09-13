package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// pgCache is a fixed-size ring: a cycling sequence assigns slots, so once capacity is reached
// each new key overwrites the oldest row. Existing keys are updated in place.
type pgCache struct{ pool *pgxpool.Pool }

const pgSchema = `
CREATE TABLE IF NOT EXISTS anomaly_ring (
	slot       integer PRIMARY KEY,
	key        text NOT NULL UNIQUE,
	value      jsonb NOT NULL,
	created_at timestamptz NOT NULL DEFAULT now()
);
CREATE SEQUENCE IF NOT EXISTS anomaly_ring_slot MINVALUE 1 MAXVALUE %d CYCLE;
ALTER SEQUENCE anomaly_ring_slot MAXVALUE %d;
DELETE FROM anomaly_ring WHERE slot > %d;`

func newPgCache(ctx context.Context, cc cacheConfig) (*pgCache, error) {
	if cc.DSN == "" {
		return nil, fmt.Errorf("postgres cache: dsn (or CACHE_DSN env) required")
	}
	pool, err := pgxpool.New(ctx, cc.DSN)
	if err != nil {
		return nil, err
	}
	n := cc.Capacity
	if _, err := pool.Exec(ctx, fmt.Sprintf(pgSchema, n, n, n)); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres cache schema: %w", err)
	}
	return &pgCache{pool: pool}, nil
}

func (c *pgCache) Get(ctx context.Context, key string) ([]byte, bool, error) {
	var raw []byte
	err := c.pool.QueryRow(ctx, `SELECT value FROM anomaly_ring WHERE key = $1`, key).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	}
	return raw, err == nil, err
}

func (c *pgCache) Put(ctx context.Context, key string, v []byte) error {
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	tag, err := tx.Exec(ctx, `UPDATE anomaly_ring SET value = $2, created_at = now() WHERE key = $1`, key, v)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		// New key: take the next slot, evicting whatever row held it.
		_, err = tx.Exec(ctx, `
			INSERT INTO anomaly_ring (slot, key, value) VALUES (nextval('anomaly_ring_slot'), $1, $2)
			ON CONFLICT (slot) DO UPDATE SET key = EXCLUDED.key, value = EXCLUDED.value, created_at = now()`,
			key, v)
		if err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (c *pgCache) Keys(ctx context.Context, prefix string) ([]string, error) {
	rows, err := c.pool.Query(ctx, `SELECT key FROM anomaly_ring WHERE key LIKE $1 || '%' ORDER BY key DESC`, prefix)
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
