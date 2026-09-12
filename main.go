// POC: per-user log metrics vs same time-of-week / weekday history (Datadog Logs aggregation API).
package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"
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

	users, err := resolveUsers(ctx, cfg, c, nil, time.Time{})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	verdicts, err := run(ctx, cfg, users, c, cache)
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
	var users []string
	var refreshed time.Time
	for {
		u, err := resolveUsers(ctx, cfg, c, users, refreshed)
		if err == nil {
			if len(u) > 0 && !equalStrings(u, users) {
				fmt.Fprintf(os.Stderr, "users: %s\n", strings.Join(u, ","))
			}
			users, refreshed = u, time.Now()
			st.setUsers(users)
		}
		var verdicts []verdict
		if err == nil {
			verdicts, err = run(ctx, cfg, users, c, cache)
		}
		st.set(verdicts, err)
		if err != nil {
			fmt.Fprintln(os.Stderr, "collect:", err)
		} else if err := putJSON(ctx, cache, runKey(cfg, verdicts[0].Window), verdicts); err != nil {
			fmt.Fprintln(os.Stderr, "store run:", err)
		}
		time.Sleep(cfg.Interval)
	}
}

// resolveUsers returns the static list, or refreshes the top-users list when stale.
func resolveUsers(ctx context.Context, cfg config, c *ddClient, current []string, refreshed time.Time) ([]string, error) {
	if len(cfg.Users) > 0 {
		return cfg.Users, nil
	}
	if len(current) > 0 && time.Since(refreshed) < cfg.TopUsers.Refresh {
		return current, nil
	}
	return c.topUsers(ctx, cfg, time.Now().UTC())
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func run(ctx context.Context, cfg config, users []string, c *ddClient, cache Cache) ([]verdict, error) {
	// Align to window boundary so windows are reusable as history by later runs.
	to := time.Now().UTC().Add(-cfg.Lag).Truncate(cfg.Window)
	from := to.Add(-cfg.Window)

	cur, err := getWindow(ctx, cfg, users, c, cache, from)
	if err != nil {
		return nil, err
	}
	starts := sampleStarts(from, cfg)
	hist := make([]windowValues, 0, len(starts))
	for _, s := range starts {
		w, err := getWindow(ctx, cfg, users, c, cache, s)
		if err != nil {
			return nil, err
		}
		hist = append(hist, w)
	}

	var out []verdict
	for _, m := range cfg.Metrics {
		for _, u := range users {
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

// storedWindow is a cached window plus the users it was fetched for.
type storedWindow struct {
	Users  []string     `json:"users"`
	Values windowValues `json:"values"`
}

// getWindow returns cached values, fetching only users the cached window does not cover.
func getWindow(ctx context.Context, cfg config, users []string, c *ddClient, cache Cache, from time.Time) (windowValues, error) {
	key := windowKey(cfg, from)
	var sw storedWindow
	if _, err := getJSON(ctx, cache, key, &sw); err != nil {
		return nil, fmt.Errorf("cache get: %w", err)
	}
	have := map[string]bool{}
	for _, u := range sw.Users {
		have[u] = true
	}
	var missing []string
	for _, u := range users {
		if !have[u] {
			missing = append(missing, u)
		}
	}
	if len(missing) == 0 {
		return sw.Values, nil
	}
	w, err := c.fetchWindow(ctx, cfg, missing, from, from.Add(cfg.Window))
	if err != nil {
		return nil, err
	}
	if sw.Values == nil {
		sw.Values = windowValues{}
	}
	for name, byUser := range w {
		if sw.Values[name] == nil {
			sw.Values[name] = map[string]float64{}
		}
		for u, v := range byUser {
			sw.Values[name][u] = v
		}
	}
	sw.Users = append(sw.Users, missing...)
	if err := putJSON(ctx, cache, key, sw); err != nil {
		return nil, fmt.Errorf("cache put: %w", err)
	}
	return sw.Values, nil
}
