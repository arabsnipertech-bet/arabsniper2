# Gold engine campaign — 11 November 2026

Campaign: `gold-engine-2026-11`. Requested by the owner: seven free days through the existing private Telegram /prova flow, including previous trial users.

## Production result

- Deployment commit: 9be7def3e5f924697d18a89dbc071b431e6c62be.
- Successful workflow: https://github.com/arabsnipertech-bet/arabsniper2/actions/runs/43080493369
- Telegram announcement: https://t.me/arabsniper_signals/173
- Production Worker SHA256: 6131f2e83def80f1cce4f2d959ddc7d7630ca01df33a862ec97137c2f19562dd.
- Original Worker SHA256: d6e904d6db2dab78d8339bf6b1da70d5ed32cdbe8c0e8f6d4de584bc54b6688f.
- Original 17 legacy trial claims preserved; new additive campaign claims and announcement tables.
- Private rollback snapshot in existing private R2 bucket: ops-backups/telegram-campaign/gold-engine-2026-11/43080493369.json. Contains customer data: never publish as an artifact or commit.
- Worker bindings, compatibility and other checked settings unchanged.

## Behavior

With TELEGRAM_TRIAL_ENABLED=true, the new campaign is the default when TELEGRAM_TRIAL_CAMPAIGN is absent. Setting TELEGRAM_TRIAL_CAMPAIGN=disabled returns to the legacy eligibility logic; it does not revoke granted access. Setting TELEGRAM_TRIAL_ENABLED=false disables new bot requests altogether.

One grant per email and Telegram account per campaign. Requires a fresh verified email OTP. Existing continuous access is preserved and extended by seven days. Suitable active Stripe trials are extended in place; paid subscriptions are not changed. Otherwise, a capped free Stripe trial provides the entitlement. Trial end and cancellation time must both match the immutable reserved expiry. Infinite VIP access remains unchanged.

Reservation, stable Stripe idempotency keys, and subscription metadata recovery prevent repeated grants after retries. No deletion/reset of legacy trial history. No automatic mass renewal: users request this campaign through /prova.

## Deployment boundaries

Production differed from the repository's complete production bundle. The launch replaced only the exact legacy Telegram module inside the downloaded live Worker, using Cloudflare's content-only API. The rest of the live Worker was preserved byte-for-byte. The repository bundle was regenerated for source consistency but was NOT deployed as a whole. In particular this launch does not certify or deploy the separate Smart Renewal work.

The launch workflow stops if the production baseline changed unexpectedly. Re-running after a different Worker deployment must be reviewed rather than bypassing its hash guard. Announcement delivery is reserved and recorded in D1; an uncertain delivery is stopped for inspection, not blindly repeated.

## Verification and limits

Ten automated tests passed, covering original eligibility, authenticity/private-chat guards, OTP, unexpected billing cancellation, returning users, active trial extension, paid coverage, concurrency, and recovery after D1 failure. The actual patched/downloaded module also passed a synthetic /prova integration test with network blocked. Production bot identity, configured webhook and unauthenticated 403 response were verified; no pending updates.

A real customer's email OTP delivery, Stripe grant, and subsequent website login were not exercised end-to-end during deployment. No real customer was created for testing.
