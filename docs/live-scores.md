# Gold live scores

The standalone Cloudflare Worker `arabsniper-live-scores` serves `GET /api/live-scores`.
A two-minute Cron Trigger invokes the SQLite Durable Object `SCORES`, named `global`.
The browser and Auditor read stored observations. Public requests do not trigger provider calls.

The worker imports tracked fixture IDs and kickoffs from `data/live_watchlist.json`
on `main` every 15 minutes. It keeps the prior watchlist through temporary failures.
With up to 20 live or soon-to-start fixtures, a single ID query covers them and
uses any available slots for overdue or incomplete results. With more live
fixtures, the collector queries the necessary Rome dates, including a match's
previous Rome date after midnight, and can send one supplementary ID query.
This covers fixtures omitted by the date feed, fixtures still reported as not
started after kickoff, and later corrections.
Unresolved fixtures receive slower retries after three hours, for up to 72 hours.
Final results are rechecked after one hour, then after eight hours, then daily
within the recovery window. A result missing the halftime score is rechecked
more often. The public feed covers four days. Confirmed finals remain in Durable
Object storage under their fixture IDs and can be read with `?id=<fixture-id>`.

The collector caps its own attempts at 1,500 per UTC day and stops at the greater
of 1,000 remaining requests or 10% of the provider's daily limit. It also observes
the per-minute allowance, honors Retry-After and backs off following errors.
The reserve covers this collector. Other clients using the same key can use the
shared allowance between observations.

Gold uses confirmed halftime scores for first-half markets, the score at 90 minutes
for full-time markets and the difference for second-half markets. Supplementary
time and penalties remain separate. Each saved proposal is evaluated from its
own market specification. An incomplete result is displayed as pending or
unverifiable. The display does not rewrite prematch selections or prices.

## Release and rollback

Use **Deploy Gold live scores Worker** from `main`. The workflow uses
`CLOUDFLARE_LIVE_SCORES_API_TOKEN` in GitHub Secrets. A Cloudflare token with edit
permission for the existing Worker is sufficient when its route and triggers
are already configured. Keep `API_SPORTS_KEY` in the existing Worker binding.
The workflow checks the bindings, runtime variables and Cron Trigger before
upload, records the active version, and uses Wrangler to deploy a new version.
It verifies the version on the public route and checks that the Cron Trigger
advances. A failed release verification rolls back to the previous version
when the failed version is still serving traffic. The workflow can also roll
back to a specified version UUID. Cloudflare Pages serves the Gold frontend
and is released separately by `publish-site.yml`.

## Production verification

**Verify Gold live results** checks the public response and timestamps after
score recovery and on its 15-minute schedule. It reports stalled collection,
provider failures, live scores missing or more than five minutes old, and
fixtures past their expected finish without a final result. The GitHub relay
prioritizes the stale live fixture IDs reported by the Worker. An automated
check can give a short provider rate limit up to five
minutes to clear when the cron and watchlist are healthy, all due scores are
present, and the collector has a retry within the window. The recheck reads
only the stored public feed. If the feed recovers, the workflow records a
warning and preserves the initial diagnostic. A persistent limit, exhausted
daily quota, overdue score or another fault still fails the check.
The monitor compares the two most recent closed Rome dates with the
checksummed Auditor archive after the nightly window. A manual run can compare
up to 20 fixture IDs against API-Football using the existing GitHub Secret.

For the November 13, 2026 incident, verify `1644690` and `1649517` and use
`1520978` as an independently verified control. Check the Worker response and
the three saved proposals in authenticated Gold. Check again after the next
scheduled cycle. HTTP 200 alone does not establish a healthy collector.
