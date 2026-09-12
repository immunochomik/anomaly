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
		go collectLoop(ctx, newCollector(cfg, c, cache), st)
		fmt.Fprintf(os.Stderr, "listening on %s\n", *addr)
		if err := http.ListenAndServe(*addr, newServer(st)); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}

	col := newCollector(cfg, c, cache)
	verdicts, err := col.collect(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
	anomalies := 0
	for _, v := range verdicts {
		printVerdict(v)
		if v.Anomaly {
			anomalies++
		}
	}
	if anomalies > 0 || err != nil {
		os.Exit(1)
	}
}

type collector struct {
	cfg       config
	c         *ddClient
	cache     Cache
	users     map[string][]string // per scope
	refreshed map[string]time.Time
}

func newCollector(cfg config, c *ddClient, cache Cache) *collector {
	return &collector{cfg: cfg, c: c, cache: cache, users: map[string][]string{}, refreshed: map[string]time.Time{}}
}

func collectLoop(ctx context.Context, col *collector, st *state) {
	for {
		verdicts, err := col.collect(ctx)
		st.set(verdicts, err)
		if err != nil {
			fmt.Fprintln(os.Stderr, "collect:", err)
		}
		if len(verdicts) > 0 {
			if err := putJSON(ctx, col.cache, runKey(col.cfg, verdicts[0].Window), verdicts); err != nil {
				fmt.Fprintln(os.Stderr, "store run:", err)
			}
		}
		time.Sleep(col.cfg.Interval)
	}
}

// collect runs every scope; a failing scope is reported but does not block the others.
func (col *collector) collect(ctx context.Context) ([]verdict, error) {
	var all []verdict
	var errs []string
	for _, name := range col.cfg.scopeNames() {
		scfg := col.cfg.scoped(name)
		users, err := col.resolveUsers(ctx, scfg, name)
		if err == nil {
			var vs []verdict
			vs, err = run(ctx, scfg, users, col.c, col.cache)
			for i := range vs {
				vs[i].Scope = name
			}
			all = append(all, vs...)
		}
		if err != nil {
			errs = append(errs, fmt.Sprintf("scope %q: %v", name, err))
		}
	}
	if len(errs) > 0 {
		return all, fmt.Errorf("%s", strings.Join(errs, "; "))
	}
	return all, nil
}

// resolveUsers returns the static list, or refreshes the scope's top-users list when stale.
func (col *collector) resolveUsers(ctx context.Context, scfg config, scope string) ([]string, error) {
	if len(scfg.Users) > 0 {
		return scfg.Users, nil
	}
	if cur := col.users[scope]; len(cur) > 0 && time.Since(col.refreshed[scope]) < scfg.TopUsers.Refresh {
		return cur, nil
	}
	u, err := col.c.topUsers(ctx, scfg, time.Now().UTC())
	if err != nil {
		return nil, err
	}
	fmt.Fprintf(os.Stderr, "users[%s] top %d: %s\n", scope, len(u), strings.Join(u, ","))
	col.users[scope], col.refreshed[scope] = u, time.Now()
	return u, nil
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
