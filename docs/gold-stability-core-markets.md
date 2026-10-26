# Gold: stable proposals and core market matrix

Production policy: `gold-goals-v3`. Stability: `gold-stability-v1`.
Computed scope: `gold-core-markets-v1`. The Poisson kernel remains V5.0.

## Entry and maintenance

A new proposal must pass every strict Gold rule. Only the previously published
best proposal may use tolerance. New scans transfer that choice by fixture ID
from any of the five saved day files, including after a day-file rollover.

Maintenance is limited to 90 minutes after the last strict admission. A held
proposal never refreshes that timestamp or its reference values. It requires:

| Change from the last strict admission | Maximum |
|---|---:|
| Same-book decimal odds | 3% relative |
| Statistical probability | 1 percentage point |
| Selected bookmaker overround | 0.75 percentage points |
| OCI | 20 index points |
| Central/prudent EV admission cushion | 1 percentage point |

Central EV must still be at least 0.5%; prudent EV cannot fall below −0.5%.
Higher overround tiers retain their stronger requirements minus that same
bounded cushion. Unknown overround, changed bookmaker, missing previously
available OCI, or a detected price reversal cannot use maintenance tolerance.

Only EV/sensitivity failures may be tolerated. Probability >=35%, real odds
1.40–3.50, supported history, coherent goal probabilities, the competition
whitelist and quote freshness remain hard gates. The current OCI classification
is recomputed; a former convergence label is not carried forward without support.
These are explicit product thresholds, not evidence of calibrated profitability.

Among strict candidates of equal priority, replacement requires a score gain
of at least 15% or 2 score points, whichever is larger. A better PT priority
still wins immediately. The final saved rank is authoritative for Gold and the
public preview. UI shows retained status and its maximum expiry time.

## Computation and display

Each admitted future fixture receives 55 base rows, instead of 297:

- FT 1X2; FT O/U 1.5, 2.5, 3.5; FT and HT Goal/No Goal; HT O/U 1.5.
- Asian FT totals 1.75, 2, 2.25, 2.75, 3, 3.25; HT 1, 1.25, 1.75, 2.
- FT draw-no-bet and handicap quarter steps from −1 to +1.

Up to eight extra half-line Asian rows are admitted only when actually offered
through the provider's Asian catalogue. Exact scores, extreme totals, combos,
second-half-only and team-total markets are excluded before probability
estimation, price partitioning, OCI and overround processing. Under outcomes
remain necessary for complete and correctly paired overround calculations.
Legacy settlement functions remain available for already frozen historical data.

The native dialog opens on the four goal targets; compact tabs expose FT, HT,
1X2 and bounded Asian references. It supports Escape, backdrop and close button,
focus containment/restoration, independent scrolling and Fast refresh while open.
The 1X2 three-column opening/current board remains on the match card.

## Release and audit

The compatible release refreshes future matrices from saved statistics and quote
observations with **zero API-Football calls**. It preserves source timestamps;
this is not a new quote collection. Scheduled Fast/Night collection is unchanged.
Started forecasts are immutable. Public assets use cache version 5.5.

The prospective audit retains the v2 hashes and choices, separates v2/v3 cohorts,
and freezes a strict `without_hysteresis` control alongside published v3 choices.
It never combines the two policies into one performance estimate.

Validation covers admission/exit boundaries, non-renewing anchors, cumulative
drift, expired quotes, persisted Fast/day rollover, unchanged core probabilities,
backend/browser scope parity, skipped dead-market computation, historical
immutability, mixed audit cohorts and desktop/390px/360px modal behavior.
