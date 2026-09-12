// POC: per-user log metrics vs same time-of-week / weekday history (Datadog Logs aggregation API).
package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"
)

func main() {
	cfgPath := flag.String("config", "config.yaml", "config file")
	flag.Parse()

	cfg, err := loadConfig(*cfgPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	c := &ddClient{
		apiKey: os.Getenv("DD_API_KEY"),
		appKey: os.Getenv("DD_APP_KEY"),
		site:   os.Getenv("DD_SITE"),
		http:   &http.Client{Timeout: 30 * time.Second},
		minGap: cfg.Gap,
	}
	if c.apiKey == "" || c.appKey == "" {
		fmt.Fprintln(os.Stderr, "DD_API_KEY and DD_APP_KEY required")
		os.Exit(2)
	}
	if c.site == "" {
		c.site = "datadoghq.com"
	}

	ctx := context.Background()
	cache, err := newCache(ctx, cfg.Cache)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	defer cache.Close()

	if err := run(ctx, cfg, c, cache); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, cfg config, c *ddClient, cache Cache) error {
	// Align to window boundary so windows are reusable as history by later runs.
	to := time.Now().UTC().Add(-cfg.Lag).Truncate(cfg.Window)
	from := to.Add(-cfg.Window)

	cur, err := getWindow(ctx, cfg, c, cache, from)
	if err != nil {
		return err
	}
	starts := sampleStarts(from, cfg)
	hist := make([]windowValues, 0, len(starts))
	for _, s := range starts {
		w, err := getWindow(ctx, cfg, c, cache, s)
		if err != nil {
			return err
		}
		hist = append(hist, w)
	}

	anomalies := 0
	for _, m := range cfg.Metrics {
		for _, u := range cfg.Users {
			v := verdict{user: u, metric: m.Name}
			v.current, v.hasCurrent = cur[m.Name][u]
			if !v.hasCurrent && m.Aggregation == "count" {
				v.current, v.hasCurrent = 0, true
			}
			for _, h := range hist {
				x, ok := h[m.Name][u]
				if !ok && m.Aggregation == "count" {
					x, ok = 0, true
				}
				if ok {
					v.samples = append(v.samples, x)
				}
			}
			judge(&v, m, cfg)
			printVerdict(v)
			if v.anomaly {
				anomalies++
			}
		}
	}
	if anomalies > 0 {
		return fmt.Errorf("%d anomalies", anomalies)
	}
	return nil
}

func getWindow(ctx context.Context, cfg config, c *ddClient, cache Cache, from time.Time) (windowValues, error) {
	key := cacheKey(cfg, from)
	if w, ok, err := cache.Get(ctx, key); err != nil {
		return nil, fmt.Errorf("cache get: %w", err)
	} else if ok {
		return w, nil
	}
	w, err := c.fetchWindow(ctx, cfg, from, from.Add(cfg.Window))
	if err != nil {
		return nil, err
	}
	if err := cache.Put(ctx, key, w); err != nil {
		return nil, fmt.Errorf("cache put: %w", err)
	}
	return w, nil
}
