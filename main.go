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
	addr := flag.String("serve", "", "run collector in background and serve web UI on this address, e.g. :8080")
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

	if *addr != "" {
		st := newState(cfg, cache, c.site)
		if err := st.loadHistory(ctx); err != nil {
			fmt.Fprintln(os.Stderr, "load history:", err)
		}
		go collectLoop(ctx, cfg, c, cache, st)
		fmt.Fprintf(os.Stderr, "listening on %s\n", *addr)
		if err := http.ListenAndServe(*addr, newServer(st)); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}

	verdicts, err := run(ctx, cfg, c, cache)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	anomalies := 0
	for _, v := range verdicts {
		printVerdict(v)
		if v.Anomaly {
			anomalies++
		}
	}
	if anomalies > 0 {
		os.Exit(1)
	}
}

func collectLoop(ctx context.Context, cfg config, c *ddClient, cache Cache, st *state) {
	for {
		verdicts, err := run(ctx, cfg, c, cache)
		if err == nil && len(verdicts) > 0 {
			err = putJSON(ctx, cache, runKey(cfg, verdicts[0].Window), verdicts)
		}
		st.set(verdicts, err)
		if err != nil {
			fmt.Fprintln(os.Stderr, "collect:", err)
		}
		time.Sleep(cfg.Interval)
	}
}

func run(ctx context.Context, cfg config, c *ddClient, cache Cache) ([]verdict, error) {
	// Align to window boundary so windows are reusable as history by later runs.
	to := time.Now().UTC().Add(-cfg.Lag).Truncate(cfg.Window)
	from := to.Add(-cfg.Window)

	cur, err := getWindow(ctx, cfg, c, cache, from)
	if err != nil {
		return nil, err
	}
	starts := sampleStarts(from, cfg)
	hist := make([]windowValues, 0, len(starts))
	for _, s := range starts {
		w, err := getWindow(ctx, cfg, c, cache, s)
		if err != nil {
			return nil, err
		}
		hist = append(hist, w)
	}

	var out []verdict
	for _, m := range cfg.Metrics {
		for _, u := range cfg.Users {
			v := verdict{User: u, Metric: m.Name}
			v.Current, v.HasCurrent = cur[m.Name][u]
			if !v.HasCurrent && m.Aggregation == "count" {
				v.Current, v.HasCurrent = 0, true
			}
			for _, h := range hist {
				x, ok := h[m.Name][u]
				if !ok && m.Aggregation == "count" {
					x, ok = 0, true
				}
				if ok {
					v.Samples = append(v.Samples, x)
				}
			}
			v.Window = from
			judge(&v, m, cfg)
			out = append(out, v)
		}
	}
	return out, nil
}

func getWindow(ctx context.Context, cfg config, c *ddClient, cache Cache, from time.Time) (windowValues, error) {
	key := windowKey(cfg, from)
	var w windowValues
	if ok, err := getJSON(ctx, cache, key, &w); err != nil {
		return nil, fmt.Errorf("cache get: %w", err)
	} else if ok {
		return w, nil
	}
	w, err := c.fetchWindow(ctx, cfg, from, from.Add(cfg.Window))
	if err != nil {
		return nil, err
	}
	if err := putJSON(ctx, cache, key, w); err != nil {
		return nil, fmt.Errorf("cache put: %w", err)
	}
	return w, nil
}
