# Weekend verification — 23–25 October 2026

Keep model context-goals-v3.4, selection thresholds, Gold conditions and movement confirmation rules unchanged during the observation window. Record any necessary corrective deployment separately; do not pool results across changed rules without identifying the versions.

## Repeatable release gate

Run **Selection policy tests** on the exact candidate commit (automatic on relevant changes, or Run workflow on main). It includes Python analysis/audit tests, public homepage and results renderer tests, dashboard/storage/live-score tests, and desktop/mobile dashboard checks. Synthetic renderer inputs must not depend on daily production results.

After a frontend release, **Pubblica sito** success means the Cloudflare hook accepted the request. Verify the deployed HTML/JS against main, then check the rendered homepage, current public selections, yesterday's results and the Gold sign-in screen. An authenticated Gold session still requires a real account check; synthetic browser tests do not establish login success.

## Data gate

- Fast/Night Scan must finish and publish, with current timestamps and the expected scan horizon.
- Inspect API usage, incomplete pagination, and targeted requests/books/markets recovered separately. Zero recovery is a valid result.
- Verify Freeze Audit Source and Run Auditor complete. Preserve captures made before kickoff and their forecast hashes; do not reconstruct missing prematch predictions after outcomes are known.
- Public result totals must use the entire eligible cohort, with pending matches excluded from the settled denominator. The showcase is only an excerpt.
- Inspect failed workflows before rerunning: an accepted hook or active writer may already be processing the work.

Existing schedules: Fast Scan hourly 06:00–23:00 Europe/Rome, Freeze Audit Source hourly at minute 37, Run Auditor daily at 09:07 Europe/Rome. GitHub schedules can be delayed; use actual completion timestamps. Night Scan retains its existing schedule and five-day horizon.

## Review at the beginning of next week

Separate primary proposals, Gold and confirmed movement combinations; include all qualifying wins, losses and pending outcomes. Compare exact market, league, bookmaker coverage, confirmation counts and available xG/shots. Report sample sizes and model versions. Treat weekend findings as preliminary; do not optimize thresholds on the same sample used to claim performance.
