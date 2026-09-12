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

	mu        sync.RWMutex
	verdicts  []verdict
	discovery map[string]discovery // per scope
	err       error
	updated   time.Time
	trend     map[string][]float64 // user|metric -> recent ratios
}

func newState(cfg config, cache Cache, site string) *state {
	return &state{cfg: cfg, cache: cache, site: site, trend: map[string][]float64{}, discovery: map[string]discovery{}}
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

func (s *state) setDiscovery(scope string, d discovery) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.discovery[scope] = d
}

func (s *state) set(vs []verdict, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.err, s.updated = err, time.Now()
	if len(vs) == 0 {
		return
	}
	s.verdicts = vs
	for _, v := range vs {
		k := v.Scope + "|" + v.User + "|" + v.Metric
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

func (s *state) trendFor(scope, user, metric string) []float64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.trend[scope+"|"+user+"|"+metric]
}

type row struct {
	Status, Scope, User, Metric, Reason, Trend string
	DDLink                                     string
	Now, Median, Ratio                         float64
	Samples                                    []float64
	Window                                     time.Time
	Anomaly                                    bool
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
	MetricQ    string
	ScopeQ     string
	Scopes     []string  // all configured scopes, for the filter links
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
		r := row{Scope: v.Scope, User: v.User, Metric: v.Metric, Reason: v.Reason, Now: v.Current, Median: v.Median,
			Ratio: v.Ratio, Samples: v.Samples, Window: v.Window, Anomaly: v.Anomaly, Status: "ok"}
		if v.Anomaly {
			r.Status = "anomaly"
			anomalies++
		}
		if withTrend {
			r.Trend = sparkline(s.trendFor(v.Scope, v.User, v.Metric))
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
	q := strings.TrimSpace(s.cfg.scoped(v.Scope).BaseQuery + " " + s.cfg.UserFacet + ":" + v.User + " " + matchQuery(m.Match))
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
		case "scope":
			return a.Scope < b.Scope
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
	mux.HandleFunc("GET /users", st.handleUsers)
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
	p := page{Interval: s.cfg.Interval, Live: true, Scopes: s.cfg.scopeNames()}
	if len(p.Scopes) == 1 && p.Scopes[0] == "" {
		p.Scopes = nil
	}

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
	p.UserQ, p.MetricQ, p.ScopeQ = q.Get("user"), q.Get("metric"), q.Get("scope")
	if p.UserQ != "" || p.MetricQ != "" || p.ScopeQ != "" {
		var rows []row
		for _, x := range p.Rows {
			if (p.UserQ == "" || x.User == p.UserQ) && (p.MetricQ == "" || x.Metric == p.MetricQ) && (p.ScopeQ == "" || x.Scope == p.ScopeQ) {
				rows = append(rows, x)
			}
		}
		p.Rows = rows
	}
	seen := map[string]bool{}
	p.Anomalies = 0
	for _, x := range p.Rows {
		seen[x.Scope+"|"+x.User] = true
		if x.Anomaly {
			p.Anomalies++
		}
	}
	p.Users = len(seen)
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

type usersPage struct {
	Scopes []scopeUsers
	Static []string
	Weeks  int
}

type scopeUsers struct {
	Name string
	discovery
}

func (s *state) handleUsers(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	p := usersPage{Static: s.cfg.Users, Weeks: s.cfg.Weeks}
	for _, name := range s.cfg.scopeNames() {
		if d, ok := s.discovery[name]; ok {
			p.Scopes = append(p.Scopes, scopeUsers{Name: name, discovery: d})
		}
	}
	s.mu.RUnlock()
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_ = usersTmpl.Execute(w, p)
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

// link builds a page URL preserving the current filters and viewed run, with overrides.
func (p page) link(overrides map[string]string) string {
	q := url.Values{}
	set := func(k, v string) {
		if v != "" {
			q.Set(k, v)
		}
	}
	set("user", p.UserQ)
	set("metric", p.MetricQ)
	set("scope", p.ScopeQ)
	set("sort", p.Sort)
	set("dir", p.Dir)
	if !p.Live {
		set("at", p.Window.UTC().Format(time.RFC3339))
	}
	for k, v := range overrides {
		q.Del(k)
		set(k, v)
	}
	if len(q) == 0 {
		return "/"
	}
	return "/?" + q.Encode()
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
		u := p.link(map[string]string{"sort": key, "dir": dir})
		return template.HTML(`<th><a href="` + template.HTMLEscapeString(u) + `">` + template.HTMLEscapeString(label) + mark + `</a></th>`)
	},
	"userLink":   func(p page, u string) string { return p.link(map[string]string{"user": u}) },
	"metricLink": func(p page, m string) string { return p.link(map[string]string{"metric": m}) },
	"scopeLink":  func(p page, sc string) string { return p.link(map[string]string{"scope": sc}) },
	"clear":      func(p page) string { return p.link(map[string]string{"user": "", "metric": "", "scope": ""}) },
	"join":       func(xs []string) string { return strings.Join(xs, ", ") },
	"inc":        func(i int) int { return i + 1 },
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
{{if or .UserQ .MetricQ .ScopeQ}}<a href="{{clear .}}">clear filter{{if .ScopeQ}} scope={{.ScopeQ}}{{end}}{{if .UserQ}} user={{.UserQ}}{{end}}{{if .MetricQ}} metric={{.MetricQ}}{{end}}</a>{{end}}
<a href="/runs">all runs ({{.RunCount}})</a>
<a href="/users">users</a>
<a href="/api/results{{if not .Live}}?at={{ts .Window}}{{end}}">json</a>
</p>
<p class="muted">window {{if .Window.IsZero}}-{{else}}{{fmt .Window}} UTC{{end}}
 · updated {{if .Updated.IsZero}}never{{else}}{{.Updated.Format "15:04:05"}}{{end}}
 · every {{.Interval}} · {{.Users}} users · <b>{{.Anomalies}} anomalies</b>
{{if .Scopes}} · scope: {{if .ScopeQ}}<a href="{{scopeLink . ""}}">all</a>{{else}}<b>all</b>{{end}}{{range .Scopes}} | {{if eq . $.ScopeQ}}<b>{{.}}</b>{{else}}<a href="{{scopeLink $ .}}">{{.}}</a>{{end}}{{end}}{{end}}</p>
{{if .Err}}<div class="err">last collection failed: {{.Err}}</div>{{end}}
<table><tr>{{th . "status" "status"}}{{if .Scopes}}{{th . "scope" "scope"}}{{end}}{{th . "user" "user"}}{{th . "metric" "metric"}}<th>now</th><th>median</th>{{th . "ratio" "ratio"}}{{if .Live}}<th>trend</th>{{end}}<th>samples</th><th>reason</th><th></th></tr>
{{range .Rows}}<tr class="{{.Status}}">
<td class="status">{{.Status}}</td>
{{if $.Scopes}}<td><a href="{{scopeLink $ .Scope}}">{{.Scope}}</a></td>{{end}}
<td><a href="{{userLink $ .User}}">{{.User}}</a></td><td><a href="{{metricLink $ .Metric}}">{{.Metric}}</a></td>
<td>{{f .Now}}</td><td>{{f .Median}}</td><td>{{f .Ratio}}</td>
{{if $.Live}}<td class="trend">{{.Trend}}</td>{{end}}
<td class="muted">{{range .Samples}}{{f .}} {{end}}</td>
<td>{{.Reason}}</td>
<td>{{if .DDLink}}<a href="{{.DDLink}}" target="_blank">logs ↗</a>{{end}}</td></tr>
{{else}}<tr><td colspan="11" class="muted">no results yet</td></tr>{{end}}
</table></body></html>`))

var runsTmpl = template.Must(template.New("").Funcs(funcs).Parse(`<!doctype html>
<html><head><meta charset="utf-8"><title>anomaly runs</title>` + css + `</head><body>
<h2>Runs</h2><p class="nav"><a href="/">latest</a></p>
<table><tr><th>window (UTC)</th><th>anomalies</th><th>users</th></tr>
{{range .Runs}}<tr class="{{if .Anomalies}}anomaly{{else}}ok{{end}}">
<td><a href="/?at={{ts .Window}}">{{fmt .Window}}</a></td><td>{{.Anomalies}}</td><td>{{join .Users}}</td></tr>
{{else}}<tr><td colspan="3" class="muted">no runs stored</td></tr>{{end}}
</table></body></html>`))

var usersTmpl = template.Must(template.New("").Funcs(funcs).Parse(`<!doctype html>
<html><head><meta charset="utf-8"><title>anomaly users</title>` + css + `</head><body>
<h2>Users</h2><p class="nav"><a href="/">latest</a></p>
{{if .Static}}<p>static list from config/USERS: {{join .Static}}</p>{{end}}
{{range $s := .Scopes}}
<h3>{{if $s.Name}}{{$s.Name}}{{else}}default scope{{end}} <span class="muted">· discovered {{$s.At.Format "2006-01-02 15:04"}} · top {{$s.Used}} monitored</span></h3>
<p class="muted">query: <code>{{$s.Query}}</code> (last {{$.Weeks}} weeks, grouped by userid)</p>
<table><tr><th>#</th><th>user</th><th>count</th><th></th></tr>
{{range $i, $u := $s.Users}}<tr class="{{if lt $i $s.Used}}ok{{else}}muted{{end}}">
<td>{{inc $i}}</td><td><a href="/?user={{$u.User}}&scope={{$s.Name}}">{{$u.User}}</a></td><td>{{f $u.Count}}</td>
<td>{{if lt $i $s.Used}}monitored{{else}}<span class="muted">outside top {{$s.Used}}</span>{{end}}</td></tr>
{{end}}</table>
{{else}}<p class="muted">no discovery yet (static users, or first collection not finished)</p>{{end}}
</body></html>`))

func trimFloat(x float64) string {
	if math.IsInf(x, 0) {
		return "inf"
	}
	if x == math.Trunc(x) {
		return strconv.FormatFloat(x, 'f', 0, 64)
	}
	return strconv.FormatFloat(x, 'f', 2, 64)
}
