# Quote coverage acquisition

Exact Asian half lines (HT 1.5/2.5, FT 2.5/3.5) can supply a missing or unusable
standard Over/Under pair. IDs and market identities, period, line, bookmaker,
provider timestamp and both outcomes are checked. Whole/quarter lines never
substitute for a half line. Fresh complete native pairs take precedence.
Versioned source keys retain the real Asian origin and prevent a source change
from fabricating an opening-price movement. Normalization is idempotent.

Every returned bookmaker is admitted to parsing; the provider catalogue, not a
static whitelist, drives supplemental requests. These requests now include the
Asian identities and the second-half total, interleaved with missing operators
so that the per-fixture cap does not starve catalogue expansion. Complete pairs
remain mandatory for margin validation. An expanded catalogue cannot create
prices that the provider does not actually supply.

The Combo fallback multiplies HT Over 0.5 and SH Over 0.5 from the same operator
and provider snapshot. Each source pair must be fresh and have 0–9% overround.
The maximum constituent margin is displayed with both underlying margins; no
combined opposite outcome or combined overround is invented. The price and
fair probability are estimates under the disjoint-halves independence
assumption, labelled `synthetic_combo` / `quote_executable: false`, including in
cache and the UI. Actual direct prices take precedence. No synthetic OCI or
direct movement confirmation is generated. Gold, Platinum, Radar and Combo
statistical admission thresholds remain unchanged.

For fixtures starting within four hours, an existing saved first price and a
missing/stale current market can trigger one uncached fixture request per
cycle, with a persistent 30-minute cooldown. `NEAR_KICKOFF_MAX_HTTP_CALLS`
defaults to 40 (0 disables; ceiling 200). The hard limit includes status checks,
retries and pagination. Supplemental enrichment is disabled for that request.
Unknown quota or the larger of 7,500 calls / 10% of the daily allocation left
as reserve blocks recovery. Failed/incomplete requests preserve original data;
re-downloading an old snapshot never renews its freshness. Diagnostics are
stored under `near_kickoff_quotes.last_run` in the existing quote state.

`auditor/quote_coverage.py` compares the prior and new parser on identical live
responses, read-only, with a 400-HTTP-call total audit cap. The denominator is
five requested markets per audited upcoming fixture in today's published Gold
feed. A valid quote needs a complete fresh pair and 0–9% overround (the Gold
8% rule is still applied separately). Equivalent standard/Asian settlements
are counted once. Standard-row fill, real market coverage, synthetic coverage
and the extra effect of targeted refresh are reported separately. Cached
prices are excluded. API errors and budget-excluded fixtures are disclosed;
only aggregates and response hashes are exported, never private selections.
