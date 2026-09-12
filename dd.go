package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// windowValues is metric -> user -> value for one time window. Missing user = no logs.
type windowValues map[string]map[string]float64

type ddClient struct {
	apiKey, appKey, site string
	baseURL              string // overrides https://api.<site> (tests)
	http                 *http.Client
	minGap               time.Duration
	lastCall             time.Time
}

type aggregateReq struct {
	Filter  filter    `json:"filter"`
	Compute []compute `json:"compute"`
	GroupBy []groupBy `json:"group_by"`
}

type filter struct {
	Query   string   `json:"query"`
	From    string   `json:"from"`
	To      string   `json:"to"`
	Indexes []string `json:"indexes"`
}

type compute struct {
	Aggregation string `json:"aggregation"`
	Metric      string `json:"metric,omitempty"`
	Type        string `json:"type"`
}

type groupBy struct {
	Facet string     `json:"facet"`
	Limit int        `json:"limit"`
	Sort  *groupSort `json:"sort,omitempty"`
}

type groupSort struct {
	Type        string `json:"type"`
	Aggregation string `json:"aggregation"`
	Order       string `json:"order"`
}

type aggregateResp struct {
	Data struct {
		Buckets []struct {
			By       map[string]string          `json:"by"`
			Computes map[string]json.RawMessage `json:"computes"`
		} `json:"buckets"`
	} `json:"data"`
}

// fetchWindow fetches all metrics for one window. Metrics sharing the same match facets go in
// one call, grouped by user + those facets; facet limits equal the matched values, so bucket
// counts stay small and exact.
func (c *ddClient) fetchWindow(ctx context.Context, cfg config, users []string, from, to time.Time) (windowValues, error) {
	groups := map[string][]metric{}
	var order []string
	for _, m := range cfg.Metrics {
		k := strings.Join(matchFacets([]metric{m}), ",")
		if _, ok := groups[k]; !ok {
			order = append(order, k)
		}
		groups[k] = append(groups[k], m)
	}
	w := windowValues{}
	for _, k := range order {
		part, err := c.fetchGroup(ctx, cfg, users, groups[k], from, to)
		if err != nil {
			return nil, err
		}
		for name, users := range part {
			w[name] = users
		}
	}
	return w, nil
}

func (c *ddClient) fetchGroup(ctx context.Context, cfg config, users []string, metrics []metric, from, to time.Time) (windowValues, error) {
	facets := matchFacets(metrics)
	computes, idx := buildComputes(metrics)
	countIdx := idx["count|"]

	var parts []string
	for _, m := range metrics {
		parts = append(parts, "("+matchQuery(m.Match)+")")
	}
	query := fmt.Sprintf("%s %s:(%s) (%s)", cfg.BaseQuery, cfg.UserFacet,
		strings.Join(users, " OR "), strings.Join(parts, " OR "))

	gb := []groupBy{{Facet: cfg.UserFacet, Limit: len(users)}}
	for _, f := range facets {
		gb = append(gb, groupBy{Facet: f, Limit: facetValues(metrics, f)})
	}
	out, err := c.aggregate(ctx, aggregateReq{
		Filter:  filter{Query: strings.TrimSpace(query), From: from.Format(time.RFC3339), To: to.Format(time.RFC3339), Indexes: []string{"*"}},
		Compute: computes,
		GroupBy: gb,
	})
	if err != nil {
		return nil, err
	}

	type acc struct{ sum, weight float64 }
	accs := map[string]map[string]*acc{}
	for _, m := range metrics {
		accs[m.Name] = map[string]*acc{}
	}
	for _, b := range out.Data.Buckets {
		user := b.By[cfg.UserFacet]
		cnt := computeValue(b.Computes, countIdx)
		for _, m := range metrics {
			if !bucketMatches(b.By, m.Match) {
				continue
			}
			val := computeValue(b.Computes, idx[computeKey(m)])
			a := accs[m.Name][user]
			if a == nil {
				a = &acc{}
				accs[m.Name][user] = a
			}
			switch m.Aggregation {
			case "count", "sum":
				a.sum += val
				a.weight = 1
			default: // count-weighted mean over matching buckets; exact for avg, approximate for percentiles
				a.sum += val * cnt
				a.weight += cnt
			}
		}
	}
	w := windowValues{}
	for name, users := range accs {
		w[name] = map[string]float64{}
		for u, a := range users {
			if a.weight > 0 {
				w[name][u] = a.sum / a.weight
			}
		}
	}
	return w, nil
}

// topUsers returns the most active users over the sampling range by TopUsers.Match count.
func (c *ddClient) topUsers(ctx context.Context, cfg config, now time.Time) ([]string, error) {
	from := now.AddDate(0, 0, -7*cfg.Weeks)
	query := strings.TrimSpace(cfg.BaseQuery + " " + matchQuery(cfg.TopUsers.Match))
	out, err := c.aggregate(ctx, aggregateReq{
		Filter:  filter{Query: query, From: from.Format(time.RFC3339), To: now.Format(time.RFC3339), Indexes: []string{"*"}},
		Compute: []compute{{Aggregation: "count", Type: "total"}},
		GroupBy: []groupBy{{Facet: cfg.UserFacet, Limit: cfg.TopUsers.Count,
			Sort: &groupSort{Type: "measure", Aggregation: "count", Order: "desc"}}},
	})
	if err != nil {
		return nil, err
	}
	var users []string
	for _, b := range out.Data.Buckets {
		if u := b.By[cfg.UserFacet]; u != "" {
			users = append(users, u)
		}
	}
	if len(users) == 0 {
		return nil, fmt.Errorf("top users: no users found for %q", query)
	}
	return users, nil
}

func (c *ddClient) aggregate(ctx context.Context, req aggregateReq) (aggregateResp, error) {
	var out aggregateResp
	body, err := json.Marshal(req)
	if err != nil {
		return out, err
	}
	base := c.baseURL
	if base == "" {
		base = "https://api." + c.site
	}
	raw, err := c.doWithRetry(ctx, base+"/api/v2/logs/analytics/aggregate", body)
	if err != nil {
		return out, err
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return out, fmt.Errorf("decode: %w", err)
	}
	return out, nil
}

// facetValues counts distinct matched values of a facet across metrics (case-insensitive).
func facetValues(ms []metric, facet string) int {
	set := map[string]bool{}
	for _, m := range ms {
		for _, v := range m.Match[facet] {
			set[strings.ToLower(v)] = true
		}
	}
	return len(set)
}

func matchFacets(ms []metric) []string {
	set := map[string]bool{}
	for _, m := range ms {
		for f := range m.Match {
			set[f] = true
		}
	}
	out := make([]string, 0, len(set))
	for f := range set {
		out = append(out, f)
	}
	sort.Strings(out)
	return out
}

func computeKey(m metric) string {
	if m.Aggregation == "count" {
		return "count|"
	}
	return m.Aggregation + "|" + m.Measure
}

// buildComputes returns distinct computes (count always first) and key -> index.
func buildComputes(ms []metric) ([]compute, map[string]int) {
	idx := map[string]int{"count|": 0}
	cs := []compute{{Aggregation: "count", Type: "total"}}
	for _, m := range ms {
		k := computeKey(m)
		if _, ok := idx[k]; ok {
			continue
		}
		idx[k] = len(cs)
		cs = append(cs, compute{Aggregation: m.Aggregation, Metric: m.Measure, Type: "total"})
	}
	return cs, idx
}

func computeValue(computes map[string]json.RawMessage, i int) float64 {
	var v float64
	_ = json.Unmarshal(computes["c"+strconv.Itoa(i)], &v) // null -> 0
	return v
}

func matchQuery(match map[string][]string) string {
	keys := make([]string, 0, len(match))
	for k := range match {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var parts []string
	for _, k := range keys {
		vals := make([]string, len(match[k]))
		for i, v := range match[k] {
			if strings.ContainsAny(v, " :") {
				v = strconv.Quote(v)
			}
			vals[i] = v
		}
		parts = append(parts, k+":("+strings.Join(vals, " OR ")+")")
	}
	return strings.Join(parts, " ")
}

func bucketMatches(by map[string]string, match map[string][]string) bool {
	for facet, vals := range match {
		got, ok := by[facet]
		if !ok {
			return false
		}
		found := false
		for _, v := range vals {
			if strings.EqualFold(v, got) {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func (c *ddClient) throttle(ctx context.Context) error {
	if wait := c.minGap - time.Since(c.lastCall); wait > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
	}
	c.lastCall = time.Now()
	return nil
}

// doWithRetry retries 429/5xx, waiting for Retry-After / X-RateLimit-Reset when present.
func (c *ddClient) doWithRetry(ctx context.Context, url string, body []byte) ([]byte, error) {
	const maxAttempts = 8
	backoff := time.Second
	for attempt := 1; ; attempt++ {
		if err := c.throttle(ctx); err != nil {
			return nil, err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("DD-API-KEY", c.apiKey)
		req.Header.Set("DD-APPLICATION-KEY", c.appKey)

		resp, err := c.http.Do(req)
		if err != nil {
			return nil, err
		}
		raw, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return nil, err
		}
		if resp.StatusCode/100 == 2 {
			return raw, nil
		}
		retryable := resp.StatusCode == 429 || resp.StatusCode/100 == 5
		if !retryable || attempt == maxAttempts {
			return nil, fmt.Errorf("datadog %d after %d attempts: %s", resp.StatusCode, attempt, truncate(string(raw), 300))
		}
		wait := retryDelay(resp.Header, backoff)
		fmt.Fprintf(os.Stderr, "datadog %d, retry %d/%d in %s\n", resp.StatusCode, attempt, maxAttempts, wait)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(wait):
		}
		backoff = min(backoff*2, 60*time.Second)
	}
}

func retryDelay(h http.Header, fallback time.Duration) time.Duration {
	for _, k := range []string{"Retry-After", "X-RateLimit-Reset"} {
		if secs, err := strconv.Atoi(h.Get(k)); err == nil && secs > 0 {
			return time.Duration(secs)*time.Second + 500*time.Millisecond
		}
	}
	return fallback
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
