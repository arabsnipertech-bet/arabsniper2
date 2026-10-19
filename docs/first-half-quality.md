# Over 1.5 first-half quality comparison — v1.1.9

The probability/convergence view and the value/EV view answer different questions.
The existing value gate continues to require calibrated probability >=40%, data
reliability >=60%, a verified market, available odds and positive theoretical EV.
The new research view uses book fair probability >=45%, Poisson HT >=40%,
empirical HT >=40%, a verified market, reliability >=60% and recorded HT history
for both teams. It does not require positive EV and does not claim a new combined
success probability. Shared inputs make its three components dependent.

The 45/40/40 thresholds were explored using October 18, 2026 results. October 19
is a transition day. Only whole dates from October 20 are reported as prospective
validation. There is no automatic threshold fitting or promotion to a winning
filter; at least 100 prospective selected matches across at least three dates
are needed before the report marks a profile available for review.

## Data and traceability

- Missing or partial halftime scores remain null, rather than becoming 0-0.
- Team, venue, league and H2H HT statistics use only observed halftime scores.
- HT shrinkage and blending use HT sample counts, not FT sample counts.
- A missing historical HT rate contributes no empirical zero; available sources
  are reweighted, or an explicitly identified league fallback is used.
- `model.first_half_evidence` records HT sample sizes, venue context, missing
  counts, historical rows, rate estimates and lambdas.
- `display.first_half_assessment` and the Over HT candidate record convergence,
  its fixed thresholds, exclusion reasons and a separate value decision.
- `probability_rankings.over15_ht` contains future research candidates, including
  candidates rejected by the value gate. The website presents kickoff order.
- Immutable first-observed forecasts include the new fields on new captures.
  Existing frozen forecasts are never replaced or enriched after the event.

`filter_comparison.over15_ht_probability_comparison` compares the whole frozen
universe, separately reports selections from accepted/rejected candidates, and
keeps discovery, transition and prospective results apart. The quality-aware
profile excludes legacy records whose actual HT coverage was not archived;
the threshold-only profile remains available for the historical comparison.
Predictor bands and Brier scores are descriptive, not fitted calibration weights.
Model-version cohorts should be inspected because HT data handling changed.

Validation: `python -m unittest discover -s tests -v` and
`node tests/test_dashboard.js`. No new API requests are added by the signal.
