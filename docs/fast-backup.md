# Hourly Fast recovery

The existing GitHub hourly schedule remains enabled. An independent cron-job.org
job dispatches `fast_scan_backup.yml` every ten minutes at minutes
`5,15,25,35,45,55`, hours `6-23`, timezone `Europe/Rome`, every day.

The lightweight workflow checks GitHub only. It makes no API-Football calls and
does not access private Gold data. It waits while any shared data writer is
active or queued. A full scan starts only when no successful collection from
the last 60 minutes has been published. Freshness is measured from collection
start, not publication time. Scheduled, external and normal manual starts use
the same checkpoint guard under the data-writer concurrency lock. Code release
refreshes remain allowed; cached refreshes and skipped jobs never renew the Fast
completion marker.

Failed scans/publications can retry after ten minutes, with no more than two
failed attempts in a rolling hour. Incomplete API responses fail closed. A long
or queued writer is never cancelled or replaced. This improves trigger recovery;
it cannot guarantee hourly delivery when scans, publication or another writer
take longer than an hour.

The completion marker is created after both expected dates are successfully
refreshed, then travels with the private checkpoint. Its pointer commits last.
Failed Gold uploads cannot publish a new completion marker. Public-mode
publication includes the marker and feeds in the same commit. The private Gold
publication remains valid if a later public-homepage push fails.

## cron-job.org configuration

- Method: POST
- URL: `https://api.github.com/repos/arabsnipertech-bet/arabsniper2/actions/workflows/fast_scan_backup.yml/dispatches`
- Headers: `Accept: application/vnd.github+json`, `Content-Type: application/json`,
  `X-GitHub-Api-Version: 2022-11-28`, and `Authorization: Bearer <existing limited Actions token>`.
- Body for connection test: `{"ref":"main","inputs":{"dry_run":true}}`.
- Body for operation: `{"ref":"main","inputs":{"dry_run":false}}`.
- Same repository and Actions permission as the Night backup; no new credentials
  or broader permissions are needed. Keep token values out of logs and commits.

An HTTP 204 confirms dispatch acceptance only. Check the backup workflow result
and the full Fast's publication steps separately. A dry run can report
`would_dispatch`, `writer_busy`, `already_published`, `retry_cooldown`,
`retry_limit`, or `outside_fast_window` without starting a scan.

Prediction criteria, ranking, whitelist, Gold, Platinum, Radar and Combo are
unchanged. No historical audit data is removed.
