# Radar study admission and suppressed evidence

This release changes study publication only. Gold and Platinum policy, stakes,
settlement and official statistics are unchanged.

* Result ideas require Poisson >=32%, or >=35% for draws/prices above 3.60,
  and an executable observed price <=4.50.
* Fair market probability below 18%, or a model advantage above 18 percentage
  points without a drop of at least 3%, suppresses the candidate. Fair probabilities
  use fresh row probabilities or a complete normalized 1X2 price set. Missing
  fair data are not replaced by the reciprocal of a single price.
* Draws and prices above 4.00 require their own drop of at least 2.5%.
  Positive OCI alone never earns the text “valore e flusso”.
* Favorite direction comes from a home/away price <=1.75 or fair probability >=53%.
  Opposite result/over and double-chance/over combinations are suppressed.
* Combo blending retains the Poisson conditional distribution within each result
  cell and averages the result marginals 50/50 with normalized market probabilities.
  `poisson_pct` remains the original model estimate. `blended_pct` is a separate
  heuristic estimate, not a measured true probability. No bookmaker combo price
  is fabricated from products of marginal prices.
* An unpriced combo is visible only as the sole calculable alternative, with
  blended probability strictly above 48%. At most one such fallback is shown.
* In the strong-favorite/attacking-underdog context, consider GG at >=52%, HT over
  1.5 at price 1.65–3.20 with probability >=30% or combined HT lambda >=1.10,
  and higher goal lines at 1.70–2.60 when FT over 2.5 is below 1.65 and >=55%.
  Underdog support requires lambda >=0.95 or the actual `for_ft.xg_mean` >=1.15;
  favorite lambda must be >=1.50 and underdog price >3.80. Shots are not xG.

`quote_monitoring.suppressed_study_candidates` preserves the original candidate,
blocking reasons and separate suppression reasons. Freeze, journal, daily archive,
settlement and calibration retain them. Audit reports separate visible selections
from suppressed candidates. `contrarian_poisson_trap` diagnostics include counts,
Brier, recorded-price ROI and same-book CLV by suppression reason. Reasons overlap
and their counts must not be added. Missing quotes or closing observations remain
missing, and do not become fabricated profit or CLV.

Suppressed candidates cannot promote themselves or modify official model weights.
The existing 50-independent-fixture threshold remains in place for study promotion
diagnostics. This evidence enables evaluation of opponent-context distortion over
time; it does not establish from two examples that a filter improves profitability.

Regression scenarios use user-reported Santander/Real and Elfsborg/Orgryte prices
and probabilities with controlled rates and results. They are not historical feed
replays. Tests cover both directions, thresholds, missing prices, rebound flow,
full nightly archive/calibration traversal and compact browser rendering.

Follow-up integrity checks: a flat/rising own price never receives the flow label,
even when fair probability rises or an older feed claims `real_flow=true`. Missing
fair probabilities fail result admission; fallback normalization requires a complete
single-book 1X2, rather than unrelated best prices. Eligible goal-context studies
precede the cheap main goal line in the star ranking, retaining the 32% star floor.
