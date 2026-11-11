# Facebook extension — 11 November 2026

The owner requested the same new-engine Gold promotion on Facebook and confirmation of the existing editorial schedule.

The 12–18 November 2026 campaign now covers both @arabsniper_signals and Facebook Page 604216843376166. Each channel has 14 slots, nominally at 12:45 and 20:45 Europe/Rome, with fallback and no duplicate publication. The same offer applies: seven extra free days requested via @ArabSniperBot /prova, plus the personal 10% conversion discount in the final 48 hours.

Facebook uses the existing Meta Page credential directly. Its independent D1 receipt table, facebook_campaign_announcements, stores string post IDs; Telegram receipts remain unchanged. Uncertain outcomes stop automatic retries. A Facebook step can run even when the Telegram publishing step fails, provided regression tests passed; it verifies campaign and destination prerequisites independently. Existing Publisher trial promotions are suppressed on both channels only for the campaign's advertising dates.

Setup run: https://github.com/arabsnipertech-bet/arabsniper2/actions/runs/43088964634
The Facebook Page identity and published-post read access were verified in production without publishing an extra post. Four dedicated tests cover deduplication, uncertain sends, wrong-destination receipts and the campaign end boundary. Existing campaign and renewal tests also passed. The existing monitoring automation now checks both channels.

## Existing editorial behavior (verified in current source and recent production logs)

- Publisher V7.1 runs on a nominal 15-minute schedule and after successful Fast Scan.
- Facebook was enabled in the successful production run 43087076146.
- Recap is considered from 10:45 Rome, only when certified Auditor data and required cohorts exist.
- Academy/info rubric is considered between 16:45 and 20:00 Rome, with selection priority until 17:45.
- Prematch selections depend on fresh eligible candidates, generally 20–60 minutes before kickoff. Facebook receives at most the representative eligible daily BEST under the current selection policy.
- Live tracking updates Telegram's selected match post; Facebook receives the final live recap. Continuous Facebook live updates are not configured.
- In run 43087076146 there were zero new eligible BEST candidates. A successful cycle therefore does not imply that a selection was sent.
