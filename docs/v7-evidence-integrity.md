# V7: price reference and probability evidence

This release repairs a reference mismatch and records diagnostic evidence. It
does not retune the engine, select markets on historical ROI, or change a role's
probability, price band, ranking, Gold/Platinum policy or recovery admission.

## Selected-price edge

`adaptive_pricing.reprice` previously recomputed `edge_pp` against the median
`market_fair_pct` even after V7 had selected an executable price and calibrated
the score distribution. V7 admission and recovery already used the selected
bookmaker fair. The row could consequently show an edge with the opposite sign
to its selected-price comparison.

V7 repricing now uses `selected_market_fair_pct`, through the existing
`outcome_fair` validation. `edge_fair_basis = selected_bookmaker` identifies the
reference. If that fair is unavailable, the edge is unavailable; a consensus
price does not silently substitute for the selected operator. The median fair
remains in `market_fair_pct`, and legacy repricing keeps its original behavior.

EV itself is unchanged: `win_weight * (odd - 1) - loss_weight`. This equals
`p * odd - 1` only for a binary contract without a push or partial settlement.
The margin is not subtracted from EV again.

## Stored diagnostic evidence

Each new V7 snapshot contains `goal_structure` computed from its existing
calibrated intensities: `lambda_pt`, `lambda_st`, their sum `lambda_ft`,
`pt_share`, and `pt_symmetry = 4 * (home_ht / lambda_pt) *
(away_ht / lambda_pt)`. No input rate changes. An unknown rate produces no
structure. At zero PT volume symmetry is undefined (`null`), not fabricated
as balance or imbalance. Normalized factors preserve symmetry for tiny
nonzero intensities without an arbitrary denominator floor.

Each usable priced market contains `pricing_evidence`:

- `basis`: binary or general settlement weights;
- `break_even_probability`: `1 / odd` for binary contracts, otherwise null;
- `edge_fair_pp`: percentage-point gap using the same operator fair. For
  non-binary contracts the comparison uses normalized settlement weights;
- `fair_identity_error_pp`: discrepancy between stored fair and
  `1 / (odd * (1 + selected_margin_pct / 100))`, in percentage points;
- `raw_ev` and `statistical_ev`: diagnostic returns of the corresponding
  score distributions at that exact same usable price, in fractional units.

The official `calibrated`, `ev`, `ev_low`, and quote remain unchanged. The raw
and statistical estimates share goal history: their EVs are related diagnostics,
not independent votes and never a maximum from which to select a preferred EV.
No new field is used by the router or the learning gate.

## Deliberately preserved behavior

EV_RECOVERY_AGGIO_V1 from PR #241 remains unchanged, including the positive-EV
priority, three roles, raw/statistical price support for Consigliata/Colpaccio,
original negative EV, BLU qualification, and frozen Studio commitments.

A 25% GG PT at 4.20 can already enter Colpaccio as BLU with positive prudent
EV, even though it does not pass the distinct Gold probability floor. No new
global symmetry, OCI, margin or ROI threshold is introduced.

`V7_SHADOW_ONLY` is not reintroduced. The unified production engine explicitly
ignores the obsolete switch and existing tests require that all price writers
remain on V7. Adding the variable to YAML alone would not be a working rollback.
Rollback of this release is a revert of this isolated commit.

## Validation and release scope

The historical reconstruction checks 1,286 archived proposals: 1,261 known
probabilities agree with the archived calibrated lambdas within 5e-9; 988 EVs
agree within the corresponding probability rounding; 964 same-book margin
identities agree within exported rounding. Missing probabilities stay missing.
This is not a routing replay of the unavailable universe of historical V7
alternatives, and no historical promotion count is inferred from it.

A separate paired full-engine regression uses 96 **synthetic** prematch
fixtures, including 14 frozen cases. Probabilities, prices, EVs, Gold/Platinum
decisions, signals and all three Triad choices are identical. The corrected
edge and additive diagnostic evidence are the intended differences. Synthetic
observations are not evidence of historical or future financial performance.

All work before release is local. No API-Football scans, historical
republishing, infrastructure/budget changes or social sends are required.
Use the existing `[night-before-fast]` release guard and wait for the ordinary
scheduled Fast. Do not change workflow schedules or disable policy CI.
