# Campaign publicity and conversion — 11 November 2026

Owner selected: seven days of advertising, twice daily, and 10% off in each participant's final 48 hours.

## Schedule

- Telegram channel: @arabsniper_signals, bot @ArabSniperBot.
- Advertising: 12–18 November 2026 inclusive, nominal 12:45 and 20:45 Europe/Rome.
- Native GitHub schedule: 11:45 and 19:45 UTC; fallback 12:15 and 20:15 UTC. Actual starts may be delayed by GitHub.
- Exactly 14 distinct slots; D1 records campaign/date/slot before sending and stores the Telegram receipt afterward.
- An uncertain send is not repeated automatically. Out-of-window and out-of-date runs send nothing.
- Existing Publisher V7.1 Telegram trial advertisements are suppressed for these dates to avoid overlap; Facebook's existing cadence is preserved.
- The campaign advertising ends on 18 November. This does not revoke grants or disable the personal conversion offer.
- An additional twice-daily check through 18 November verifies receipts and reports unrecoverable missed publications.

## Conversion

The separate Gold renewal service is now extended to granted claims for gold-engine-2026-11. The authenticated email and actual current entitlement deadline must match the campaign record, which must have a subscription ID. No general discount for arbitrary free trials was introduced.

The offer appears in the existing authenticated Gold access panel when 0 < remaining time <= 48 hours. Ordinary prices remain unchanged:
- 3 days: EUR 5.00 -> EUR 4.50.
- 14 days: EUR 15.00 -> EUR 13.50.
- Calendar month: EUR 29.00 -> EUR 26.10.

Coupon: arabsniper_gold_renewal_10, duration once. One promotion is consumed per current deadline; pending Checkout reuse, concurrency protections, webhook verification and expiry handling remain in place. New paid access starts after the remaining free access. No automatic subscription renewal.

The personal deadline is independent of the advertising end date. Checkout opened in the eligible window retains its existing one-hour lifetime, including if Gold expires during that hour. No unsolicited email or private Telegram reminders were added.

## Production and validation

- Commit: 29846138d935637cf31803b92aa502659482900f.
- Successful deployment/preflight: https://github.com/arabsnipertech-bet/arabsniper2/actions/runs/43083978435
- Live Worker SHA256: 37d64cfccfa667fb12b0791049d74d6250ef980880ec657c7094ce18b88eeca2.
- Content-only replacement of the exact renewal module; other Worker source, settings and bindings preserved.
- Private R2 rollback snapshot: ops-backups/telegram-campaign/gold-engine-2026-11/renewal-43083978435.json. Do not publish its contents.
- 28 Node tests and 3 bounded-schedule tests passed. The actual Worker downloaded before/after deployment also passed 18 isolated renewal tests with simulated Stripe/email.
- Production flags, coupon binding, bot identity, webhook authentication and channel posting permission verified. No new announcement was sent by the installation run.
- A real customer payment was not performed as part of this release.
