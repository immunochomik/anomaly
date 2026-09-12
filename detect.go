package main

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

type verdict struct {
	user, metric string
	window       time.Time
	current      float64
	hasCurrent   bool
	samples      []float64
	median       float64
	ratio        float64
	anomaly      bool
	reason       string
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
	if len(v.samples) == 0 {
		v.reason = "no history"
		return
	}
	if !v.hasCurrent {
		v.anomaly = true
		v.reason = "no data now, history present"
		return
	}
	v.median = median(v.samples)
	switch {
	case v.current < m.MinValue && v.median < m.MinValue:
		v.ratio = v.current / math.Max(v.median, 1)
		v.reason = fmt.Sprintf("below min_value %.0f, not judged", m.MinValue)
		return
	case v.median == 0 && v.current == 0:
		v.reason = "zero now and in history"
		return
	case v.median == 0:
		v.ratio = math.Inf(1)
		v.anomaly = m.AlertIfNew
		v.reason = "data now, none in history (new traffic)"
		return
	}
	v.ratio = v.current / v.median
	outsideRatio := v.ratio < m.Low || v.ratio > m.High
	if len(v.samples) >= cfg.MinSamplesMAD {
		mad := madScaled(v.samples, v.median)
		dev := math.Abs(v.current - v.median)
		if outsideRatio && dev > cfg.MadK*mad {
			v.anomaly = true
			v.reason = fmt.Sprintf("%.1f MADs from median and outside ratio bounds", dev/math.Max(mad, 1e-9))
		} else {
			v.reason = fmt.Sprintf("within range (mad=%.2f, n=%d)", mad, len(v.samples))
		}
		return
	}
	lo, hi := minMax(v.samples)
	switch {
	case v.ratio < m.Low && v.current < lo:
		v.anomaly = true
		v.reason = fmt.Sprintf("below %.0f%% of median and below historic min", m.Low*100)
	case v.ratio > m.High && v.current > hi:
		v.anomaly = true
		v.reason = fmt.Sprintf("above %.0f%% of median and above historic max", m.High*100)
	default:
		v.reason = fmt.Sprintf("within range (n=%d)", len(v.samples))
	}
}

func printVerdict(v verdict) {
	status := "OK     "
	if v.anomaly {
		status = "ANOMALY"
	}
	fmt.Printf("%s %-14s %-24s now=%.2f median=%.2f ratio=%.2f samples=[%s] (%s)\n",
		status, v.user, v.metric, v.current, v.median, v.ratio, fmtSamples(v.samples), v.reason)
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
