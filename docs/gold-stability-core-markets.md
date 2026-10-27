# Gold: stable proposals and core market matrix

Production policy: `gold-goals-v6`. Stability: `gold-stability-v1`.
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

Only EV/sensitivity failures may be tolerated. The per-market probability floor, the market's
real-odds range below, supported history, coherent goal probabilities, the competition
whitelist and quote freshness remain hard gates. The current OCI classification
is recomputed; a former convergence label is not carried forward without support.
These are explicit product thresholds, not evidence of calibrated profitability.

| Market | Minimum probability | Inclusive odds range |
|---|---:|---|
| Over 1.5 HT | 35% | 1.40–3.50 |
| Goal/BTTS HT Yes | 30% | 1.40–5.50 |
| Over 2.5 FT | 30% | 1.40–3.50 |
| Over 3.5 FT | 30% | 1.40–5.50 |

Equivalent Asian **half-lines** use the same limits: 1.5 HT and 2.5 FT at
3.50, 3.5 FT at 5.50. Integer/quarter lines have different push or split
settlements and remain matrix references, not additional proposal markets.
Overround, EV, whitelist, timing-priority and tolerance thresholds stay unchanged. Each row exports `goal_market`, `odds_min`, `odds_max`; the policy
exports `odds_max_by_market` and `min_probability_pct_by_market`. These admission rules never remove statistical rows.

Among strict candidates of equal priority, replacement requires a score gain
of at least 15% or 2 score points, whichever is larger. A better PT priority
still wins immediately. The final saved rank is authoritative for Gold and the
public preview. UI shows retained status and its maximum expiry time.

## Dedicated first-half admission

The user-authorized v6 floors are 35% for Over 1.5 HT and 30% for Goal HT
and both FT targets, reduced from v5's 45% / 40% / 35%. They are **unvalidated
admission choices**, not an empirical calibration or promised success rate. Four valid,
deduplicated first-half scores per team are required (the same four-match
window as the model); insufficient evidence blocks only the proposal, never
the fixture or matrix. Full-time history cannot satisfy this check. Missing,
inconsistent HT scores and duplicate fixtures cannot manufacture coverage.

`ht_evidence` records per-team observed goals, venue coverage, HT xG sample
counts/means and HT shots when present, with `prospective_pending` validation.
Missing HT xG stays null, not zero, and is displayed explicitly. Three actual
HT xG observations in each attack/defence profile are needed to label the xG
coverage supported, matching the existing model. This coverage label is not
an additional probability bonus or an automatic admission override.

The independent HT Poisson rates already use HT goals and HT xG when available;
this release never scales FT probability, uses FT xG as an HT proxy, or changes
pure probabilities. The current ordinary collector does not request split HT
xG; this release does not invent coverage or add collection calls. An empirical
xG-based calibration requires real split coverage and a larger prospective
sample. The audits preserve evidence and decisions for that later review.

The same probability/history hard gates apply to retained proposals and exact
Asian equivalents. The browser independently enforces the per-market floor;
a stale or malformed decision cannot bypass the applicable floor. Archived v5
rows retain their original 45% / 40% / 35% limits and four-HT-history check;
v2/v3/v4 rows retain their own historical limits. Lower floors alone cannot
override price, overround, EV, freshness, history or whitelist failures.

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
Started forecasts are immutable. Updated JavaScript assets use cache version 5.8.

The prospective audit retains the v2/v3/v4/v5 hashes and choices, separates v2/v3/v4/v5/v6
cohorts, and freezes a strict `without_hysteresis` control alongside published
choices. Asian aliases have separate audit market keys to avoid counting two
prices for the same event as independent observations. The study dates and
scheduled collection stay unchanged. The odds-cap, dedicated PT admission and subsequent reduction of probability
floors are the user's explicit exceptions to the freeze until 2 November; no other model adjustments are authorized.

Validation covers admission/exit boundaries, non-renewing anchors, cumulative
drift, expired quotes, persisted Fast/day rollover, unchanged core probabilities,
backend/browser scope parity, skipped dead-market computation, historical
immutability, mixed audit cohorts and desktop/390px/360px modal behavior.
