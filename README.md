# User anomaly detection POC

Per-user metrics from Datadog logs, compared with the same time of day in recent history.
One Datadog call per window for all users and metrics; past windows are cached.

## Run

```
export DD_API_KEY=... DD_APP_KEY=... DD_SITE=datadoghq.eu
export USERS=tomaszs,user2 SM_REGION=usa
go run . -config config.yaml
```

Exit code 1 if any anomaly. Run every 10 min (cron `*/10`) so windows line up with the cache.

Web mode: `go run . -serve :8080` collects every `interval` in the background and serves a
table at `/` (sortable, `?user=` filter, ratio trend, Datadog logs link per row) and JSON at
`/api/results`. Each run is stored in the cache; `/runs` lists them, `/?at=<RFC3339>` shows one,
and older/newer links step through history.

## How it works

- Window: last complete `window` (10m), ending `lag` before now, aligned to the window boundary.
- History: Mon–Fri uses the same time on the last `weekday_samples` weekdays plus the same weekday
  from the last `weeks` weeks. Sat/Sun uses only the same weekday from the last `weeks` weeks.
- Judgement per user and metric:
  - `min_value`: skip if current and median are both below it.
  - no history but data now: "new traffic", OK unless `alert_if_no_history`.
  - ≥ `min_samples_mad` samples: anomaly if outside `low`/`high` ratio of median **and**
    more than `mad_k` scaled MADs away.
  - fewer samples: anomaly if outside ratio bounds and beyond historic min/max.

## Config

`metrics[].match` is `{facet: [values]}`. All metrics share one request: filter is the OR of all
matches, grouped by `user_facet` plus match facets. `aggregation` is any Datadog compute
(`count`, `avg`, `sum`, `pcXX`); non-count needs `measure`.

`cache.type`: `memory`, `disk` (`path`), or `postgres` (`dsn` or `CACHE_DSN` env).
Changing users, `base_query`, or metrics invalidates the cache.

## Datadog limits

One call per window, `gap` (4s) between calls. Steady state is 1 call per run; a cold cache
needs ~13.
