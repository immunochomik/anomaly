package main

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

type verdict struct {
	Scope      string    `json:"scope"`
	User       string    `json:"user"`
	Metric     string    `json:"metric"`
	Window     time.Time `json:"window"`
	Current    float64   `json:"current"`
	HasCurrent bool      `json:"has_current"`
	Samples    []float64 `json:"samples"`
	Median     float64   `json:"median"`
	Ratio      float64   `json:"ratio"`
	Anomaly    bool      `json:"anomaly"`
	Reason     string    `json:"reason"`
}

// sampleStarts returns history window starts: same weekday for past weeks, plus on Mon-Fri
// the same time on recent weekdays. Newest first.
func sampleStarts(from time.Time, cfg config) []time.Time {
	seen := map[time.Time]bool{}
	var out []time.Time
	add := func(t time.Time) {
		if !seen[t] {
			seen[t] = true
			out = append(out, t)
		}
	}
	for w := 1; w <= cfg.Weeks; w++ {
		add(from.AddDate(0, 0, -7*w))
	}
	if isWeekday(from) {
		for d, n := 1, 0; n < cfg.WeekdaySamples && d <= 7*cfg.Weeks; d++ {
			t := from.AddDate(0, 0, -d)
			if isWeekday(t) {
				add(t)
				n++
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].After(out[j]) })
	return out
}

func isWeekday(t time.Time) bool {
	return t.Weekday() != time.Saturday && t.Weekday() != time.Sunday
}

func judge(v *verdict, m metric, cfg config) {
	if len(v.Samples) == 0 {
		v.Reason = "no history"
		return
	}
	if !v.HasCurrent {
		v.Anomaly = true
		v.Reason = "no data now, history present"
		return
	}
	v.Median = median(v.Samples)
	switch {
	case v.Current < m.MinValue && v.Median < m.MinValue:
		v.Ratio = v.Current / math.Max(v.Median, 1)
		v.Reason = fmt.Sprintf("below min_value %.0f, not judged", m.MinValue)
		return
	case v.Median == 0 && v.Current == 0:
		v.Reason = "zero now and in history"
		return
	case v.Median == 0:
		v.Ratio = 0 // undefined; +Inf is not JSON-encodable
		v.Anomaly = m.AlertIfNew
		v.Reason = "data now, none in history (new traffic)"
		return
	}
	v.Ratio = v.Current / v.Median
	outsideRatio := v.Ratio < m.Low || v.Ratio > m.High
	if len(v.Samples) >= cfg.MinSamplesMAD {
		mad := madScaled(v.Samples, v.Median)
		dev := math.Abs(v.Current - v.Median)
		if outsideRatio && dev > cfg.MadK*mad {
			v.Anomaly = true
			v.Reason = fmt.Sprintf("%.1f MADs from median and outside ratio bounds", dev/math.Max(mad, 1e-9))
		} else {
			v.Reason = fmt.Sprintf("within range (mad=%.2f, n=%d)", mad, len(v.Samples))
		}
		return
	}
	lo, hi := minMax(v.Samples)
	switch {
	case v.Ratio < m.Low && v.Current < lo:
		v.Anomaly = true
		v.Reason = fmt.Sprintf("below %.0f%% of median and below historic min", m.Low*100)
	case v.Ratio > m.High && v.Current > hi:
		v.Anomaly = true
		v.Reason = fmt.Sprintf("above %.0f%% of median and above historic max", m.High*100)
	default:
		v.Reason = fmt.Sprintf("within range (n=%d)", len(v.Samples))
	}
}

func printVerdict(v verdict) {
	status := "OK     "
	if v.Anomaly {
		status = "ANOMALY"
	}
	fmt.Printf("%s %-12s %-14s %-24s now=%.2f median=%.2f ratio=%.2f samples=[%s] (%s)\n",
		status, v.Scope, v.User, v.Metric, v.Current, v.Median, v.Ratio, fmtSamples(v.Samples), v.Reason)
}

func fmtSamples(xs []float64) string {
	parts := make([]string, len(xs))
	for i, x := range xs {
		parts[i] = fmt.Sprintf("%.0f", x)
	}
	return strings.Join(parts, " ")
}

func median(xs []float64) float64 {
	s := append([]float64(nil), xs...)
	sort.Float64s(s)
	n := len(s)
	if n%2 == 1 {
		return s[n/2]
	}
	return (s[n/2-1] + s[n/2]) / 2
}

// madScaled is MAD * 1.4826, comparable to a standard deviation for normal data.
func madScaled(xs []float64, med float64) float64 {
	dev := make([]float64, len(xs))
	for i, x := range xs {
		dev[i] = math.Abs(x - med)
	}
	return median(dev) * 1.4826
}

func minMax(xs []float64) (lo, hi float64) {
	lo, hi = math.Inf(1), math.Inf(-1)
	for _, x := range xs {
		lo = math.Min(lo, x)
		hi = math.Max(hi, x)
	}
	return lo, hi
}
