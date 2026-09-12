package main

import (
	"context"
	"encoding/json"
	"fmt"
	"html/template"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const trendLen = 24

type state struct {
	cfg   config
	cache Cache
	site  string // DD site for explorer links

	mu       sync.RWMutex
	verdicts []verdict
	err      error
	updated  time.Time
	trend    map[string][]float64 // user|metric -> recent ratios
}

func newState(cfg config, cache Cache, site string) *state {
	return &state{cfg: cfg, cache: cache, site: site, trend: map[string][]float64{}}
}

// loadHistory seeds latest verdicts and trend from stored runs.
func (s *state) loadHistory(ctx context.Context) error {
	keys, err := s.cache.Keys(ctx, runPrefix(s.cfg))
	if err != nil {
		return err
	}
	if len(keys) > trendLen {
		keys = keys[:trendLen]
	}
	for i := len(keys) - 1; i >= 0; i-- { // oldest first so trend is in order
		var vs []verdict
		if ok, err := getJSON(ctx, s.cache, keys[i], &vs); err != nil || !ok {
			continue
		}
		s.set(vs, nil)
	}
	return nil
}

func (s *state) set(vs []verdict, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.err, s.updated = err, time.Now()
	if err != nil {
		return
	}
	s.verdicts = vs
	for _, v := range vs {
		k := v.User + "|" + v.Metric
		t := append(s.trend[k], v.Ratio)
		if len(t) > trendLen {
			t = t[len(t)-trendLen:]
		}
		s.trend[k] = t
	}
}

func (s *state) latest() ([]verdict, error, time.Time) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.verdicts, s.err, s.updated
}

func (s *state) trendFor(user, metric string) []float64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.trend[user+"|"+metric]
}

type row struct {
	Status, User, Metric, Reason, Trend string
	DDLink                              string
	Now, Median, Ratio                  float64
	Samples                             []float64
	Window                              time.Time
	Anomaly                             bool
}

type page struct {
	Updated    time.Time
	Err        string
	Rows       []row
	Anomalies  int
	Users      int
	Interval   time.Duration
	Sort, Dir  string
	UserQ      string
	Window     time.Time // window shown
	Live       bool      // showing latest run
	Prev, Next string    // RFC3339 of neighbouring runs, "" if none
	RunCount   int
}

type runList struct {
	Runs []runSummary
}

type runSummary struct {
	Window    time.Time
	Anomalies int
	Users     []string
}

func (s *state) rows(vs []verdict, withTrend bool) ([]row, int) {
	var rows []row
	anomalies := 0
	for _, v := range vs {
		r := row{User: v.User, Metric: v.Metric, Reason: v.Reason, Now: v.Current, Median: v.Median,
			Ratio: v.Ratio, Samples: v.Samples, Window: v.Window, Anomaly: v.Anomaly, Status: "ok"}
		if v.Anomaly {
			r.Status = "anomaly"
			anomalies++
		}
		if withTrend {
			r.Trend = sparkline(s.trendFor(v.User, v.Metric))
		}
		r.DDLink = s.ddLogsURL(v)
		rows = append(rows, r)
	}
	return rows, anomalies
}

// ddLogsURL links to Logs Explorer for this user, metric filter and window.
func (s *state) ddLogsURL(v verdict) string {
	var m *metric
	for i := range s.cfg.Metrics {
		if s.cfg.Metrics[i].Name == v.Metric {
			m = &s.cfg.Metrics[i]
		}
	}
	if m == nil || v.Window.IsZero() {
		return ""
	}
	q := strings.TrimSpace(s.cfg.BaseQuery + " " + s.cfg.UserFacet + ":" + v.User + " " + matchQuery(m.Match))
	site := s.site
	if site == "" {
		site = "datadoghq.com"
	}
	host := "app." + site
	if site == "datadoghq.com" {
		host = "app.datadoghq.com"
	}
	from, to := v.Window.UnixMilli(), v.Window.Add(s.cfg.Window).UnixMilli()
	return fmt.Sprintf("https://%s/logs?query=%s&from_ts=%d&to_ts=%d&live=false",
		host, strings.ReplaceAll(url.QueryEscape(q), "+", "%20"), from, to)
}

func sortRows(rows []row, key, dir string) {
	less := func(a, b row) bool {
		switch key {
		case "user":
			return a.User < b.User
		case "metric":
			return a.Metric < b.Metric
		case "ratio":
			return a.Ratio < b.Ratio
		default: // status: anomalies first
			return a.Anomaly && !b.Anomaly
		}
	}
	sort.SliceStable(rows, func(i, j int) bool {
		if dir == "desc" {
			return less(rows[j], rows[i])
		}
		return less(rows[i], rows[j])
	})
}

// sparkline renders ratios as block chars; 1.0 sits mid-scale, clipped to [0, 2].
func sparkline(xs []float64) string {
	const blocks = "▁▂▃▄▅▆▇█"
	out := make([]rune, 0, len(xs))
	for _, x := range xs {
		x = min(max(x, 0), 2)
		i := int(x / 2 * float64(len([]rune(blocks))-1))
		out = append(out, []rune(blocks)[i])
	}
	return string(out)
}

func newServer(st *state) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", st.handlePage)
	mux.HandleFunc("GET /runs", st.handleRuns)
	mux.HandleFunc("GET /api/results", func(w http.ResponseWriter, r *http.Request) {
		vs, err, _ := st.latest()
		if at := r.URL.Query().Get("at"); at != "" {
			vs, err = st.runAt(r.Context(), at)
		}
		w.Header().Set("Content-Type", "application/json")
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		_ = json.NewEncoder(w).Encode(vs)
	})
	return mux
}

func (s *state) runAt(ctx context.Context, at string) ([]verdict, error) {
	var vs []verdict
	ok, err := getJSON(ctx, s.cache, runPrefix(s.cfg)+at, &vs)
	if err == nil && !ok {
		err = errNotFound
	}
	return vs, err
}

var errNotFound = &notFound{}

type notFound struct{}

func (*notFound) Error() string { return "run not found" }

func (s *state) handlePage(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	q := r.URL.Query()
	p := page{Interval: s.cfg.Interval, Users: len(s.cfg.Users), Live: true}

	keys, err := s.cache.Keys(ctx, runPrefix(s.cfg))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	p.RunCount = len(keys)
	var vs []verdict
	if at := q.Get("at"); at != "" {
		p.Live = false
		if vs, err = s.runAt(ctx, at); err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		p.Window, _ = time.Parse(time.RFC3339, at)
		p.Updated = p.Window
	} else {
		var e error
		vs, e, p.Updated = s.latest()
		if e != nil {
			p.Err = e.Error()
		}
		if len(vs) > 0 {
			p.Window = vs[0].Window
		}
	}
	p.Prev, p.Next = neighbours(keys, runKey(s.cfg, p.Window), len(runPrefix(s.cfg)))

	p.Rows, p.Anomalies = s.rows(vs, p.Live)
	if u := q.Get("user"); u != "" {
		p.UserQ = u
		var rows []row
		for _, x := range p.Rows {
			if x.User == u {
				rows = append(rows, x)
			}
		}
		p.Rows = rows
	}
	p.Sort, p.Dir = q.Get("sort"), q.Get("dir")
	// Default: anomalies first, then user, then metric. Explicit sort applies on top of that.
	sortRows(p.Rows, "metric", "asc")
	sortRows(p.Rows, "user", "asc")
	sortRows(p.Rows, "status", "asc")
	if p.Sort != "" {
		sortRows(p.Rows, p.Sort, p.Dir)
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_ = tmpl.Execute(w, p)
}

// neighbours returns the older and newer run timestamps around key; keys are sorted descending.
func neighbours(keys []string, key string, prefixLen int) (prev, next string) {
	for i, k := range keys {
		if k != key {
			continue
		}
		if i+1 < len(keys) {
			prev = keys[i+1][prefixLen:]
		}
		if i > 0 {
			next = keys[i-1][prefixLen:]
		}
	}
	return prev, next
}

func (s *state) handleRuns(w http.ResponseWriter, r *http.Request) {
	keys, err := s.cache.Keys(r.Context(), runPrefix(s.cfg))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if len(keys) > 500 {
		keys = keys[:500]
	}
	var list runList
	for _, k := range keys {
		var vs []verdict
		if ok, err := getJSON(r.Context(), s.cache, k, &vs); err != nil || !ok {
			continue
		}
		rs := runSummary{}
		rs.Window, _ = time.Parse(time.RFC3339, k[len(runPrefix(s.cfg)):])
		seen := map[string]bool{}
		for _, v := range vs {
			if v.Anomaly {
				rs.Anomalies++
				if !seen[v.User] {
					seen[v.User] = true
					rs.Users = append(rs.Users, v.User)
				}
			}
		}
		list.Runs = append(list.Runs, rs)
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_ = runsTmpl.Execute(w, list)
}

var funcs = template.FuncMap{
	"f":   func(x float64) string { return trimFloat(x) },
	"ts":  func(t time.Time) string { return t.UTC().Format(time.RFC3339) },
	"fmt": func(t time.Time) string { return t.UTC().Format("2006-01-02 15:04") },
	"th": func(p page, key, label string) template.HTML {
		dir, mark := "asc", ""
		if p.Sort == key {
			if p.Dir != "desc" {
				dir, mark = "desc", " ▲"
			} else {
				mark = " ▼"
			}
		}
		u := "/?sort=" + key + "&dir=" + dir
		if p.UserQ != "" {
			u += "&user=" + template.URLQueryEscaper(p.UserQ)
		}
		if !p.Live {
			u += "&at=" + template.URLQueryEscaper(p.Window.UTC().Format(time.RFC3339))
		}
		return template.HTML(`<th><a href="` + u + `">` + template.HTMLEscapeString(label) + mark + `</a></th>`)
	},
	"join": func(xs []string) string { return strings.Join(xs, ", ") },
}

const css = `<style>
body{font:14px system-ui,sans-serif;margin:1.5rem;color:#222}
table{border-collapse:collapse;width:100%}
th,td{padding:.3rem .6rem;border-bottom:1px solid #ddd;text-align:left;white-space:nowrap}
th{background:#f4f4f4;position:sticky;top:0}
th a{color:inherit;text-decoration:none}
tr.anomaly{background:#fde8e8}
.status{font-weight:600;text-transform:uppercase;font-size:.8em}
tr.anomaly .status{color:#b00}
.trend{font-family:monospace;letter-spacing:1px;color:#555}
.muted{color:#777}
.err{background:#fff3cd;padding:.5rem;border:1px solid #e0c060;margin-bottom:1rem}
.nav a{margin-right:1rem}
</style>`

var tmpl = template.Must(template.New("").Funcs(funcs).Parse(`<!doctype html>
<html><head><meta charset="utf-8">{{if .Live}}<meta http-equiv="refresh" content="60">{{end}}
<title>anomaly</title>` + css + `</head><body>
<h2>User anomaly POC {{if not .Live}}<span class="muted">· {{fmt .Window}} UTC</span>{{end}}</h2>
<p class="nav">
{{if .Prev}}<a href="/?at={{.Prev}}">← older</a>{{end}}
{{if .Next}}<a href="/?at={{.Next}}">newer →</a>{{end}}
{{if not .Live}}<a href="/">latest</a>{{end}}
<a href="/runs">all runs ({{.RunCount}})</a>
<a href="/api/results{{if not .Live}}?at={{ts .Window}}{{end}}">json</a>
</p>
<p class="muted">window {{if .Window.IsZero}}-{{else}}{{fmt .Window}} UTC{{end}}
 · updated {{if .Updated.IsZero}}never{{else}}{{.Updated.Format "15:04:05"}}{{end}}
 · every {{.Interval}} · {{.Users}} users · <b>{{.Anomalies}} anomalies</b></p>
{{if .Err}}<div class="err">last collection failed: {{.Err}}</div>{{end}}
<table><tr>{{th . "status" "status"}}{{th . "user" "user"}}{{th . "metric" "metric"}}<th>now</th><th>median</th>{{th . "ratio" "ratio"}}{{if .Live}}<th>trend</th>{{end}}<th>samples</th><th>reason</th><th></th></tr>
{{range .Rows}}<tr class="{{.Status}}">
<td class="status">{{.Status}}</td>
<td><a href="/?user={{.User}}{{if not $.Live}}&at={{ts $.Window}}{{end}}">{{.User}}</a></td><td>{{.Metric}}</td>
<td>{{f .Now}}</td><td>{{f .Median}}</td><td>{{f .Ratio}}</td>
{{if $.Live}}<td class="trend">{{.Trend}}</td>{{end}}
<td class="muted">{{range .Samples}}{{f .}} {{end}}</td>
<td>{{.Reason}}</td>
<td>{{if .DDLink}}<a href="{{.DDLink}}" target="_blank">logs ↗</a>{{end}}</td></tr>
{{else}}<tr><td colspan="10" class="muted">no results yet</td></tr>{{end}}
</table></body></html>`))

var runsTmpl = template.Must(template.New("").Funcs(funcs).Parse(`<!doctype html>
<html><head><meta charset="utf-8"><title>anomaly runs</title>` + css + `</head><body>
<h2>Runs</h2><p class="nav"><a href="/">latest</a></p>
<table><tr><th>window (UTC)</th><th>anomalies</th><th>users</th></tr>
{{range .Runs}}<tr class="{{if .Anomalies}}anomaly{{else}}ok{{end}}">
<td><a href="/?at={{ts .Window}}">{{fmt .Window}}</a></td><td>{{.Anomalies}}</td><td>{{join .Users}}</td></tr>
{{else}}<tr><td colspan="3" class="muted">no runs stored</td></tr>{{end}}
</table></body></html>`))

func trimFloat(x float64) string {
	if math.IsInf(x, 0) {
		return "inf"
	}
	if x == math.Trunc(x) {
		return strconv.FormatFloat(x, 'f', 0, 64)
	}
	return strconv.FormatFloat(x, 'f', 2, 64)
}
