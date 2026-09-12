# User anomaly detection POC

Per-user metrics from Datadog logs, compared with the same time of day in recent history.
Meant as something to watch during a release: some anomalies are always expected.

## Run

```
export DD_API_KEY=... DD_APP_KEY=... DD_SITE=datadoghq.eu
go run . -config config.yaml            # one shot, prints verdicts, exit 1 on anomaly
go run . -config config.yaml -serve :8080   # background collector + web UI
```

Docker: `docker build -t anomaly .`. Kubernetes: `deploy/k8s.yaml` (1 replica, Postgres cache;
see the comment for the secret).

## Web UI

- `/` latest run. Sortable columns, filters by scope / user / metric (click a cell), ratio trend
  over the last 24 runs, Datadog Logs Explorer link per row. Auto-refreshes.
- `/?at=<RFC3339>` a past run; older / newer links step through history. `/runs` lists all.
- `/users` discovery query and top 50 users with counts per scope, marking the monitored ones.
- `/api/results[?at=...]` JSON.

## How it works

- **Scopes**: each entry in `scopes` (e.g. `sm-env:prod-rt sm-region:usa`) is collected
  separately by one instance and appears as a filter. No scopes = single scope.
- **Users**: default is the top `top_users.count` by `top_users.match` count over the last
  `weeks`, discovered per scope and refreshed every `top_users.refresh`. A static `users` list
  in config or `USERS=a,b` env overrides it for all scopes.
- **Window**: last complete `window` (10m), ending `lag` before now, aligned to the boundary.
- **History**: Mon–Fri uses the same time on the last `weekday_samples` weekdays plus the same
  weekday from the last `weeks` weeks. Sat/Sun uses only the same weekday from past weeks.
- **Judgement** per scope, user and metric:
  - `min_value`: skip if current and median are both below it.
  - no history but data now: "new traffic", OK unless `alert_if_no_history`.
  - ≥ `min_samples_mad` samples: anomaly if outside `low`/`high` ratio of median **and**
    more than `mad_k` scaled MADs away.
  - fewer samples: anomaly if outside ratio bounds and beyond historic min/max.

## Config

`metrics[].match` is `{facet: [values]}`. Metrics sharing the same match facets go in one
Datadog request: filter is the OR of their matches, grouped by `user_facet` plus those facets.
`aggregation` is any Datadog compute (`count`, `avg`, `sum`, `pcXX`); non-count needs `measure`.

`cache.type`: `memory`, `disk` (`path`), or `postgres` (`dsn` or `CACHE_DSN` env). The cache
holds fetched windows (immutable) and each run's verdicts. Changing `base_query`, scopes or
metrics invalidates windows. Windows record which users they cover, so a changed user set only
fetches the missing users.

## Datadog limits

`gap` (4s) between calls. Per scope and 10-minute run: 2 calls at steady state (one per
match-facet group), ~26 on a cold cache, plus 1 discovery call per `top_users.refresh`.
