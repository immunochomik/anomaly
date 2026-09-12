package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestPageHistoryNavigation(t *testing.T) {
	cfg := config{Users: []string{"u1"}, Window: 10 * time.Minute, Interval: 10 * time.Minute, hash: "abc",
		UserFacet: "@userid", BaseQuery: "sm-env:prod-rt",
		Metrics: []metric{{Name: "m", Match: map[string][]string{"@msg": {"SESSION_CONFIG"}}}}}
	cache := &memCache{m: map[string][]byte{}}
	ctx := context.Background()
	w1 := time.Date(2026, 9, 12, 10, 0, 0, 0, time.UTC)
	w2 := w1.Add(10 * time.Minute)
	seed := func(w time.Time, anomaly bool) {
		vs := []verdict{{User: "u1", Metric: "m", Window: w, Ratio: 1, Anomaly: anomaly, Reason: "x"}}
		if err := putJSON(ctx, cache, runKey(cfg, w), vs); err != nil {
			t.Fatal(err)
		}
	}
	seed(w1, true)
	seed(w2, false)

	st := newState(cfg, cache, "datadoghq.eu")
	if err := st.loadHistory(ctx); err != nil {
		t.Fatal(err)
	}
	srv := newServer(st)

	body := get(t, srv, "/")
	if !strings.Contains(body, "/?at=2026-09-12T10%3a00%3a00Z") || strings.Contains(body, "newer") {
		t.Fatalf("latest page should link only to older run:\n%s", body)
	}
	body = get(t, srv, "/?at=2026-09-12T10:00:00Z")
	if !strings.Contains(body, "/?at=2026-09-12T10%3a10%3a00Z") || !strings.Contains(body, `class="anomaly"`) {
		t.Fatalf("older page should link to newer run and show anomaly:\n%s", body)
	}
	if !strings.Contains(body, "https://app.datadoghq.eu/logs?query=sm-env%3Aprod-rt%20%40userid%3Au1%20%40msg%3A%28SESSION_CONFIG%29&amp;from_ts=") {
		t.Fatalf("missing DD logs link:\n%s", body)
	}
	body = get(t, srv, "/?metric=other")
	if strings.Contains(body, `class="anomaly"`) || strings.Contains(body, `class="ok"`) {
		t.Fatalf("metric filter should hide all rows:\n%s", body)
	}
	body = get(t, srv, "/?metric=m&user=u1&sort=ratio")
	if !strings.Contains(body, `<td>m</td>`) && !strings.Contains(body, `>m</a></td>`) {
		t.Fatalf("metric+user filter should keep row:\n%s", body)
	}
	body = get(t, srv, "/runs")
	if strings.Count(body, "/?at=") != 2 {
		t.Fatalf("runs page should list 2 runs:\n%s", body)
	}
	if rec := do(srv, "/?at=2020-01-01T00:00:00Z"); rec.Code != http.StatusNotFound {
		t.Fatalf("unknown run: want 404, got %d", rec.Code)
	}
}

func get(t *testing.T, h http.Handler, url string) string {
	t.Helper()
	rec := do(h, url)
	if rec.Code != http.StatusOK {
		t.Fatalf("%s: status %d", url, rec.Code)
	}
	return rec.Body.String()
}

func do(h http.Handler, url string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, url, nil))
	return rec
}
