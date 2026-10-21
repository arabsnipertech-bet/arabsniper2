# Shared live score service

The frontend overlays observed scores without mutating prematch payloads. Gold
shows an amber border, score, minute and timestamp. Only provider-confirmed active
states observed within five minutes receive LIVE styling. Kickoff alone never
confirms live status. Suspended/interrupted/finished states are separate. Missing
scores remain missing; expired observations show an update warning.

## Backend

`workers/live-scores/worker.mjs` is a separate Cloudflare Worker with a single
SQLite Durable Object named `global`. A two-minute Cron Trigger invokes the
collector; public GET `/api/live-scores` only reads stored observations. Increasing
visitor counts therefore does not increase API-Football requests. No API key or
account metadata appears in the response. No live odds endpoint is used.

The collector reads `data/live_watchlist.json` from main every 30 minutes, merges
it with the prior 48 hours, and requests `/fixtures?date=...&timezone=Europe/Rome`
for dates containing pending tracked fixtures (from ten minutes before kickoff
until 24 hours after). Unlike a live-only endpoint, this also retrieves FT and
halftime outcomes when a match disappears from the in-play list. At most two dates
are polled per cycle, including matches spanning midnight. Final matches are
retained and are not independently polled. There is no forecast recalculation.

Default upper bound: 720 cycles/day, at most 1,440 fixture requests plus up to
24 hourly status checks, capped at 1,500 total outbound provider requests/day.
Typical one-date window: 360 fixture calls/12 hours or 540/18 hours, plus hourly
status checks and midnight overlap. Calls stop outside tracked match windows.
These are design bounds, not measurements of an already-running service.

`/status` establishes the available plan/quota; response headers update remaining
quota during polling. Stop at the greater of 1,000 reserved requests and 10% of
the plan. This guards this collector only: another concurrent application can
still consume the shared allowance between observations. Preserve the last good
score and its timestamp on failures. Never retry in a loop.

## Deployment (pending account access)

1. Deploy this Worker in the existing ArabSniper Cloudflare account. Use the
   checked-in `wrangler.jsonc`; route only `arabsniperbet.com/api/live-scores*`.
   Do not replace existing Stripe/payment workers or broader site routes.
2. Store the existing API-Football key as Worker secret `API_SPORTS_KEY` through
   secure account tooling. Never put it in Git, the browser frontend, or chat.
3. The config defaults to `LIVE_ENABLED=false`. After confirming plan headroom,
   enable it as a Worker variable and deploy. Confirm cron and Durable Object
   binding are present and an actual score observation reaches the endpoint.
4. Publish the `web` directory through the existing Cloudflare Pages project.
   No build and no repository-root output. Verify live-scores.js is served.
5. Verify a real active fixture, a finished fixture, stale handling and midnight.
   If login/secret access is unavailable, code in GitHub is NOT live deployment.

CLI (authenticated account, secret entry kept off command lines):

    npx wrangler deploy --config workers/live-scores/wrangler.jsonc
    npx wrangler secret put API_SPORTS_KEY --config workers/live-scores/wrangler.jsonc

For rollback, set LIVE_ENABLED=false; the dashboard continues to show the latest
scan observations with timestamps. Never replace prior forecast archives.

## Usage evidence

The latest available 21 October Fast Scan used 477 requests, including retries.
Fast runs are scheduled hourly at 06:00–23:00 Europe/Rome (18 scheduled runs/day),
Night scans once/day over five dates. A request-count counter is not the billing
counter: failed attempts, unpublished executions and other consumers matter.
The 17–18 October historical dashboards show explicit daily-limit errors; these
used v1.1.7/v1.1.8 and duplicate workflows, so they are not a valid current-system
weekend average. Current v3.4 has not yet run a complete weekend in this setup.

`Inspect API quota` reads `/status` with the existing GitHub Secret and prints only
plan, active flag, daily used/limit/remaining counters. It never publishes account
identity or the key. `src/scan_usage.py` keeps 30 days of per-run counters in
`data/arab_quote_usage_history.json` after future scans. This series excludes the
live collector and auditor and must not be mislabelled as account billing usage.

Current cost references (checked 21 October 2026):
- https://www.api-football.com/pricing : Pro $19 / 7,500 per day; Ultra $29 /
  75,000 per day; Mega $39 / 150,000 per day; all competitions/endpoints.
- https://www.football-data.org/pricing : live scores EUR12/month, 12
  competitions, 20 calls/minute; the free tier has delayed scores. VAT may apply.
- https://www.sportmonks.com/football-api/plans-pricing/ : Starter EUR29/month
  for five selected leagues, 2,000 calls/entity/hour; EUR24/month billed annually.

A second provider means a second subscription, fixture-ID matching and explicit
coverage checks; cheaper headline pricing is not necessarily a cheaper total.
