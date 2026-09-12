package main

import (
	"testing"
	"time"
)

func TestSampleStartsWeekday(t *testing.T) {
	cfg := config{Weeks: 4, WeekdaySamples: 10}
	from := time.Date(2026, 9, 9, 20, 20, 0, 0, time.UTC) // Wednesday
	got := sampleStarts(from, cfg)
	if len(got) != 12 { // 10 weekdays + weeks 3 and 4 (weeks 1,2 overlap)
		t.Fatalf("want 12 samples, got %d: %v", len(got), got)
	}
	for _, s := range got {
		if !isWeekday(s) || s.Hour() != 20 || s.Minute() != 20 {
			t.Errorf("bad sample %v", s)
		}
	}
}

func TestSampleStartsWeekend(t *testing.T) {
	cfg := config{Weeks: 4, WeekdaySamples: 10}
	from := time.Date(2026, 9, 12, 20, 20, 0, 0, time.UTC) // Saturday
	if got := sampleStarts(from, cfg); len(got) != 4 {
		t.Fatalf("want 4 samples, got %d", len(got))
	}
}

func TestMatchQuery(t *testing.T) {
	q := matchQuery(map[string][]string{"@msg": {"connection usage stats"}, "status": {"warn", "error"}})
	want := `@msg:("connection usage stats") status:(warn OR error)`
	if q != want {
		t.Fatalf("got %q want %q", q, want)
	}
}

func TestJudgeMAD(t *testing.T) {
	cfg := config{MinSamplesMAD: 6, MadK: 3}
	m := metric{Low: 0.5, High: 2}
	v := verdict{Current: 10, HasCurrent: true, Samples: []float64{100, 110, 90, 105, 95, 100, 98}}
	judge(&v, m, cfg)
	if !v.Anomaly {
		t.Fatalf("expected anomaly: %+v", v)
	}
	v = verdict{Current: 80, HasCurrent: true, Samples: []float64{100, 110, 90, 105, 95, 100, 98}}
	judge(&v, m, cfg)
	if v.Anomaly {
		t.Fatalf("unexpected anomaly: %+v", v)
	}
}
