package main

import (
	"encoding/json"
	"html/template"
	"math"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"
)

const trendLen = 24

type state struct {
	mu       sync.RWMutex
	verdicts []verdict
	err      error
	updated  time.Time
	trend    map[string][]float64 // user|metric -> recent ratios
}

func newState() *state { return &state{trend: map[string][]float64{}} }

func (s *state) set(vs []verdict, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.err, s.updated = err, time.Now()
	if err != nil {
		return
	}
	s.verdicts = vs
	for _, v := range vs {
		k := v.user + "|" + v.metric
		t := append(s.trend[k], v.ratio)
		if len(t) > trendLen {
			t = t[len(t)-trendLen:]
		}
		s.trend[k] = t
	}
}

type row struct {
	Status, User, Metric, Reason, Trend string
	Now, Median, Ratio                  float64
	Samples                             []float64
	Window                              time.Time
	Anomaly                             bool
}

type page struct {
	Updated   time.Time
	Err       string
	Rows      []row
	Anomalies int
	Users     int
	Interval  time.Duration
}

func (s *state) snapshot(cfg config) page {
	s.mu.RLock()
	defer s.mu.RUnlock()
	p := page{Updated: s.updated, Interval: cfg.Interval, Users: len(cfg.Users)}
	if s.err != nil {
		p.Err = s.err.Error()
	}
	for _, v := range s.verdicts {
		r := row{User: v.user, Metric: v.metric, Reason: v.reason, Now: v.current, Median: v.median,
			Ratio: v.ratio, Samples: v.samples, Window: v.window, Anomaly: v.anomaly, Status: "ok"}
		if v.anomaly {
			r.Status = "anomaly"
			p.Anomalies++
		}
		r.Trend = sparkline(s.trend[v.user+"|"+v.metric])
		p.Rows = append(p.Rows, r)
	}
	sort.SliceStable(p.Rows, func(i, j int) bool {
		if p.Rows[i].Anomaly != p.Rows[j].Anomaly {
			return p.Rows[i].Anomaly
		}
		if p.Rows[i].User != p.Rows[j].User {
			return p.Rows[i].User < p.Rows[j].User
		}
		return p.Rows[i].Metric < p.Rows[j].Metric
	})
	return p
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

func newServer(st *state, cfg config) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		p := st.snapshot(cfg)
		if u := r.URL.Query().Get("user"); u != "" {
			var rows []row
			for _, x := range p.Rows {
				if x.User == u {
					rows = append(rows, x)
				}
			}
			p.Rows = rows
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_ = tmpl.Execute(w, p)
	})
	mux.HandleFunc("GET /api/results", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(st.snapshot(cfg))
	})
	return mux
}

var tmpl = template.Must(template.New("").Funcs(template.FuncMap{
	"f": func(x float64) string { return template.HTMLEscapeString(trimFloat(x)) },
}).Parse(`<!doctype html>
<html><head><meta charset="utf-8"><meta http-equiv="refresh" content="60">
<title>anomaly</title>
<style>
body{font:14px system-ui,sans-serif;margin:1.5rem;color:#222}
table{border-collapse:collapse;width:100%}
th,td{padding:.3rem .6rem;border-bottom:1px solid #ddd;text-align:left;white-space:nowrap}
th{background:#f4f4f4;position:sticky;top:0}
tr.anomaly{background:#fde8e8}
.status{font-weight:600;text-transform:uppercase;font-size:.8em}
tr.anomaly .status{color:#b00}
.trend{font-family:monospace;letter-spacing:1px;color:#555}
.muted{color:#777}
.err{background:#fff3cd;padding:.5rem;border:1px solid #e0c060;margin-bottom:1rem}
</style></head><body>
<h2>User anomaly POC</h2>
<p class="muted">updated {{if .Updated.IsZero}}never{{else}}{{.Updated.Format "2006-01-02 15:04:05"}}{{end}}
 · every {{.Interval}} · {{.Users}} users · <b>{{.Anomalies}} anomalies</b> · <a href="/api/results">json</a></p>
{{if .Err}}<div class="err">last collection failed: {{.Err}}</div>{{end}}
<table><tr><th></th><th>user</th><th>metric</th><th>now</th><th>median</th><th>ratio</th><th>trend</th><th>samples</th><th>reason</th></tr>
{{range .Rows}}<tr class="{{.Status}}">
<td class="status">{{.Status}}</td>
<td><a href="/?user={{.User}}">{{.User}}</a></td><td>{{.Metric}}</td>
<td>{{f .Now}}</td><td>{{f .Median}}</td><td>{{f .Ratio}}</td>
<td class="trend">{{.Trend}}</td>
<td class="muted">{{range .Samples}}{{f .}} {{end}}</td>
<td>{{.Reason}}</td></tr>
{{else}}<tr><td colspan="9" class="muted">no results yet</td></tr>{{end}}
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
