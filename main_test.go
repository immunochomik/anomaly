package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestGetWindowFetchesOnlyMissingUsers(t *testing.T) {
	var queries []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req aggregateReq
		_ = json.NewDecoder(r.Body).Decode(&req)
		queries = append(queries, req.Filter.Query)
		// one bucket per requested user with count 5
		var buckets []map[string]any
		for _, u := range []string{"a", "b", "c"} {
			if strings.Contains(req.Filter.Query, u) {
				buckets = append(buckets, map[string]any{
					"by":       map[string]string{"@userid": u, "@msg": "SESSION_CONFIG"},
					"computes": map[string]any{"c0": 5},
				})
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"buckets": buckets}})
	}))
	defer srv.Close()

	cfg := config{UserFacet: "@userid", Window: 10 * time.Minute, hash: "h",
		Metrics: []metric{{Name: "m", Aggregation: "count", Match: map[string][]string{"@msg": {"SESSION_CONFIG"}}}}}
	c := &ddClient{http: srv.Client(), baseURL: srv.URL}
	cache := &memCache{m: map[string][]byte{}}
	from := time.Date(2026, 9, 12, 10, 0, 0, 0, time.UTC)
	ctx := context.Background()

	w, err := getWindow(ctx, cfg, []string{"a", "b"}, c, cache, from)
	if err != nil || w["m"]["a"] != 5 || w["m"]["b"] != 5 {
		t.Fatalf("first fetch: %v %v", w, err)
	}
	w, err = getWindow(ctx, cfg, []string{"a", "b", "c"}, c, cache, from)
	if err != nil || w["m"]["c"] != 5 || w["m"]["a"] != 5 {
		t.Fatalf("second fetch: %v %v", w, err)
	}
	if _, err = getWindow(ctx, cfg, []string{"a", "c"}, c, cache, from); err != nil {
		t.Fatal(err)
	}
	if len(queries) != 2 || !strings.Contains(queries[1], "@userid:(c)") || strings.Contains(queries[1], "(a") {
		t.Fatalf("expected 2 calls, second only for user c: %q", queries)
	}
}
