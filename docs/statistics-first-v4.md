# Statistics-first V4

Active entry point: `3appdays.py` → `statistics_integration.install` → `statistics_first`.
Model: `statistics-first-v4.0`. Prospective validation begins 26 October 2026.
The existing API catalogue, caches, quote ledger, private publication and immutable audit remain in use.
No external model API or paid dependency is introduced.

## Statistical estimate

Use up to eight completed official current-season matches strictly before the analysis timestamp.
First-half calculations use four observed halftime scores; absent scores are unknown.
Regulation-time results are used. Future fixtures, duplicates and friendlies are excluded.

For each attack/conceded rate, cap exceptional scores at median + 2.5 × max(1.4826 × MAD, sqrt(max(median, 0.5))).
Keep raw scores, last score, mean excluding last and every adjustment in the frozen inputs.
Weight recency with a four-match half-life, and home/away context 60/40 when both are observed.
Blend real xG with these robust goals up to weight 0.60 × xG coverage, requiring three observations.
Shrink toward the league rate with four FT or three HT pseudo-matches.

Expected goals for a team = sqrt(its attack rate × the opponent's conceded rate).
H2H contributes a small age-weighted adjustment capped at 8%, requiring at least two additional matches.
Reject H2H older than two years and those already included in the team histories.
Independent Poisson rates provide coherent Over 2.5 FT, Over 1.5 HT, Over 3.5 FT and BTTS HT estimates.
Fulltime xG cannot create a halftime opportunity bonus.

A drought alone adds zero. xG under-conversion can recover part of potential through the xG blend;
it is not counted again as a bonus. A goleada has limited influence, but does not make a future failure due.
Shots remain descriptive supporting evidence, never substitute xG or goal units.

## Selection and quote confirmation

Initial admission requires six FT/four HT matches per team, two in the relevant home/away context,
and twelve FT/eight HT league observations. Recalculate after deleting each historical match in turn
and without H2H. The lowest resulting estimate must exceed the league baseline by five percentage points.
BTTS HT additionally requires both team HT rates to reach their league reference.
This sensitivity range is **not** a statistical confidence interval.

Rank by that statistical floor, then coverage and deterministic fixture order. Publish up to five per market
per day. Started selections retain their slots; empty lists are valid. Preserve frozen forecasts when kickoff
occurs during a scan. Every selected/rejected market is saved before kickoff; never evaluate only the best
market per fixture. The public free preview projects at most three distinct upcoming selected fixtures with
an available price. Historical model versions remain separate.

Only after the statistical calculation, obtain prices. **Gold = published statistical selection.**
**Blue = same-market confirmation:** decline ≥3%, at least two same operators, strict majority, coherent
change in margin-adjusted probability and positive OCI. Price freshness: six hours, one hour within three
hours of kickoff. OCI and margin describe a single movement; they are not additional independent votes.
Prices, favorite direction and inferred book probability never change statistical rates or rank.

Track Over 2.5 declines from observed prices above 2.00. A falling 1.50–1.70 favorite against an opponent
with an xG-supported drought is an **indirect 1X2 context** for an already eligible BTTS HT estimate.
It is not a direct BTTS confirmation and adds zero probability. Low recent FT scoring alone cannot imply
that an Over will be completed in the first half. Display unavailable/stale prices explicitly.

## Validation

These coefficients are transparent engineering assumptions, not an optimized or calibrated winning system.
V4 automatically stores comparisons without xG, without H2H and using raw goals, plus selected/rejected,
under-conversion, peak reduction and quote-confirmation cohorts. Assess Brier/log loss, sample size, hit rate
and theoretical price return on the same frozen future fixtures. Do not tune thresholds from one weekend
and present that same weekend as validation. Outcomes from the old model cannot establish V4 performance.
Strength of each historical opponent is not normalized; the shrinkage reference is the league.

Methodological references: [probability calibration](https://scikit-learn.org/stable/modules/calibration.html),
[bias and noise in finishing/xG estimates](https://arxiv.org/abs/2401.09940).
