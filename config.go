package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type config struct {
	Users          []string      `yaml:"users"`
	UserFacet      string        `yaml:"user_facet"`
	BaseQuery      string        `yaml:"base_query"`
	Window         time.Duration `yaml:"window"`
	Lag            time.Duration `yaml:"lag"`      // ingestion lag; window ends this long before now
	Interval       time.Duration `yaml:"interval"` // collection period in -serve mode
	Weeks          int           `yaml:"weeks"`
	WeekdaySamples int           `yaml:"weekday_samples"` // previous weekdays used on Mon-Fri
	MinSamplesMAD  int           `yaml:"min_samples_mad"`
	MadK           float64       `yaml:"mad_k"`
	Gap            time.Duration `yaml:"gap"`
	Cache          cacheConfig   `yaml:"cache"`
	Metrics        []metric      `yaml:"metrics"`

	hash string
}

type cacheConfig struct {
	Type string `yaml:"type"` // memory, disk, postgres
	Path string `yaml:"path"` // disk dir
	DSN  string `yaml:"dsn"`  // postgres; CACHE_DSN env overrides
}

type metric struct {
	Name        string              `yaml:"name"`
	Match       map[string][]string `yaml:"match"` // facet -> accepted values
	Aggregation string              `yaml:"aggregation"`
	Measure     string              `yaml:"measure"`
	Low         float64             `yaml:"low"`
	High        float64             `yaml:"high"`
	MinValue    float64             `yaml:"min_value"`
	AlertIfNew  bool                `yaml:"alert_if_no_history"`
}

func loadConfig(path string) (config, error) {
	var cfg config
	raw, err := os.ReadFile(path)
	if err != nil {
		return cfg, err
	}
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return cfg, fmt.Errorf("%s: %w", path, err)
	}
	if env := os.Getenv("USERS"); env != "" {
		cfg.Users = nil
		for _, u := range strings.Split(env, ",") {
			if u = strings.TrimSpace(u); u != "" {
				cfg.Users = append(cfg.Users, u)
			}
		}
	}
	if region := os.Getenv("SM_REGION"); region != "" {
		cfg.BaseQuery = strings.TrimSpace(cfg.BaseQuery + " sm-region:" + region)
	}
	if dsn := os.Getenv("CACHE_DSN"); dsn != "" {
		cfg.Cache.DSN = dsn
	}
	if len(cfg.Users) == 0 || len(cfg.Metrics) == 0 {
		return cfg, fmt.Errorf("%s: users (or USERS env) and metrics required", path)
	}
	def := func(d *time.Duration, v time.Duration) {
		if *d == 0 {
			*d = v
		}
	}
	def(&cfg.Window, 10*time.Minute)
	def(&cfg.Lag, time.Minute)
	def(&cfg.Interval, cfg.Window)
	def(&cfg.Gap, 4*time.Second)
	if cfg.UserFacet == "" {
		cfg.UserFacet = "@userid"
	}
	if cfg.Weeks == 0 {
		cfg.Weeks = 4
	}
	if cfg.WeekdaySamples == 0 {
		cfg.WeekdaySamples = 10
	}
	if cfg.MinSamplesMAD == 0 {
		cfg.MinSamplesMAD = 6
	}
	if cfg.MadK == 0 {
		cfg.MadK = 3
	}
	if cfg.Cache.Type == "" {
		cfg.Cache.Type = "memory"
	}
	for i := range cfg.Metrics {
		m := &cfg.Metrics[i]
		if m.Name == "" || len(m.Match) == 0 {
			return cfg, fmt.Errorf("metric %d: name and match required", i)
		}
		if m.Aggregation == "" {
			m.Aggregation = "count"
		}
		if m.Aggregation != "count" && m.Measure == "" {
			return cfg, fmt.Errorf("metric %q: measure required for %s", m.Name, m.Aggregation)
		}
		if m.Low == 0 {
			m.Low = 0.5
		}
		if m.High == 0 {
			m.High = 2.0
		}
	}
	cfg.hash = configHash(cfg)
	return cfg, nil
}

// configHash keys the cache; anything that changes fetched values must be in it.
func configHash(cfg config) string {
	users := append([]string(nil), cfg.Users...)
	sort.Strings(users)
	b, _ := json.Marshal(struct {
		Base, Facet string
		Users       []string
		Metrics     []metric
	}{cfg.BaseQuery, cfg.UserFacet, users, cfg.Metrics})
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:6])
}
