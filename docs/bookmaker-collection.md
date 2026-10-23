# API-Football bookmaker collection

The general fixture query remains the primary source. Its complete pagination is required before any supplemental data is used. Thresholds and market definitions are unchanged.

## Deep recovery

`src/odds_collection.py` supplements each upcoming fixture with queries for sparsely covered exact markets (1X2, FT totals, HT totals, HT BTTS), followed by rotating bookmaker IDs missing from the general response. Reference catalogs refresh daily. Invalid entries are ignored and failed catalog refreshes preserve the previous valid copy.

Default limits: 1,200 additional HTTP attempts per scan, at most eight targeted queries per fixture, five minutes from collector initialization. Stop new supplemental queries below a reserve of 10% of the verified daily quota, minimum 7,500. Quota is rechecked at least every 60 seconds during recovery. Retries and pagination consume the budget; an in-flight paginated request can exceed the target by its remaining pages/retries, still subject to the original pagination reserve. These are recovery limits, not a promise to consume the full allowance. Base scans and other API consumers retain their existing budgets.

Successful targeted queries have a three-hour persistent cooldown. After 24 consecutive successful queries with no new usable markets, that route (market or bookmaker) pauses for six hours; an actual gain resets the streak. This avoids routinely spending the cap when filtered routes return the same coverage. Bookmaker rotation eventually covers the catalog across scans. API failures are not treated as empty successful replies. `ARAB_DEEP_ODDS_ENABLED=0` disables recovery.

Supplemental quotes must have a verified future kickoff and a provider update no more than six hours old, not in the future. Each market record is preserved whole. Never assemble complementary outcomes from different snapshots. An older snapshot cannot overwrite a newer one. A targeted reply cannot remove an already usable target market. Duplicate bookmaker IDs count once.

## Statistics recovery

Fast Scan defaults to 250 historical fixture-statistics requests instead of 20; Night Scan to 1,000 instead of 80. Existing repository variable overrides still take precedence. Complete cached observations remain reusable. The same verified reserve guards additional statistics requests when deep collection is enabled. No fabricated xG or shots, and no threshold or model weight changes.

## Evidence

`data/arab_quote_odds_collection.json` stores the bookmaker catalog, cooldowns, rotation and last-run per-fixture coverage. It is published with the existing `arab_quote_*.json` flow and packed losslessly. API-health and usage-history records contain:

- `odds_targeted_requests`: HTTP attempts for filtered odds queries, including retries/pages.
- `odds_targeted_books_recovered`: additional usable bookmaker IDs, summed per fixture; not globally unique operators.
- `odds_targeted_markets_recovered`: new complete target market/bookmaker pairs.
- Empty, error, no-gain, cooldown and budget-stop counters.

Original `odds_books_recovered` measures pagination only. It must not be confused with targeted recovery or historical common-window recovery.

The manual **Diagnose bookmaker coverage** workflow compares all catalog bookmaker IDs, relevant bet IDs and league/date queries on eight upcoming matches from distinct leagues, under a bounded call budget. It writes sanitized reports and does not publish match data. It can disprove the hypothesis that filtered routes contain additional bookmakers; zero gains must be reported as zero.

## Initial live probe, 23 October 2026

Run 39920733987 used 331 HTTP requests: eight fixtures across distinct leagues, all 33 catalog bookmaker IDs (one unnamed), six goal/result market filters and league/date queries. No new bookmaker or book/market pair was found on any filtered route. Six covered fixtures had 8–10 bookmakers; two had no odds. This is evidence for adaptive backoff, not a promise of hidden coverage.
