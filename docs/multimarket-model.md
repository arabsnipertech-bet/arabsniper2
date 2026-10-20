# Multimarket model 1.2.0

The new page assesses each fixture chronologically, across match result, first-half
goals/BTTS, full-time 2.5/3.5 totals, home/away 1.5 totals and second-half goals.
Old signal screens remain in a collapsed comparison view. Login still uses the
existing Gold check. Legacy dashboard URLs redirect to the canonical page.

## Model and fixed research protocol

`coherent-markets-v1` represents home/away goals in each half with four independent
Poisson increments. A single joint distribution supplies all displayed events.
The HT+SH conjunction uses that increments model; it has no quoted price or EV
unless a specific verified combination market becomes available. Independence of
halves is an assumption, not a validated observation.

The statistical prior uses the existing goal-rate lambdas before the experimental
drought bonus. Current bookmaker probabilities are normalized within each complete
operator market and then pooled by median, with renormalization. A deterministic
coordinate search minimizes mean squared market-probability residuals plus a
0.01 mean squared log-rate distance to the statistical prior. The independent
market projection uses penalty 0.0001. These are initial engineering choices,
not estimates fitted to yesterday's winners. Changing them requires a new model ID.

Compare book-only, statistical, projected-market and combined forecasts on the
same fixture/market/side cohort. Keep the model experimental. Development day is
18 October; 19 October is a transition/development day. Prospective validation
starts 20 October 2026. Never tune thresholds or weights against that holdout.
The report records Brier, log loss, calibration bins, candidate/rejected outcomes,
sample counts and Wilson intervals for observed hit rates. The range between
model variants is a sensitivity range, not a confidence interval.

## Quotes and signal interpretation

Capture the already requested `/odds` payload; no extra odds API calls.
Require exact catalogue ID, name, line and complete sides. Additional mapped IDs:
5 (3.5 match totals), 16/17 (home/away totals), 8 (FT BTTS), 26 (SH totals).
Never substitute team totals, exact-score markets or live bet IDs.

Keep operator ID/name, provider update, observed time, exact market/line and both
sides. The displayed representative quote is a real operator's lower-median
quote, with the operator named. It is an observed quote, not an executed wager.
Pair movements only within the same operator/market/line. Retain first observation
and up to 99 later points per operator; prune working ledger with fixture state.
This does not certify the bookmaker's original opening price.

Record OCI, goal drought, historical/recent goal means, lambda, overround changes
and the user's home-favourite steam hypothesis (first observed 1.70–1.85 down to
1.45–1.65). Compare their settled cohorts. Do not add independent probability
bonuses for drought, OCI or margin redistribution. Margin load/share is algebraic
in odds and overround; it cannot reveal bookmaker cash flows or profits.

A research candidate requires a pre-match timestamp, coherent input rates, data
reliability at least 60, at least two complete operator quotes, known provider
update no older than six hours, positive EV across all three model variants and
no variant spread above ten percentage points. HT events need at least six
observed halftime results per team. There is no universal 40% probability floor.
These conservative research gates do not certify profitable betting.

The manual comparison accepts at most one event per fixture and three different
fixtures. Products of probabilities explicitly assume inter-match independence.
It does not force selections to reach combined odds 8–10 or place bets.

## Immutable validation and reproducibility

`multimarket_evaluations` in the existing prematch ledger freezes the first
evaluation per model ID/fixture, including rejected and unpriced events, quote
provenance, features, rates and configuration. Only settlement is later attached.
The auditor archives it by day, uses one results request per pending date and
settles FT markets at regulation time even when match status is AET/PEN.

Run `python src/model_research.py` to reproduce the retrospective comparison from
already published, timestamped Git snapshots and the first frozen odds observations.
It reconstructs 171 inputs in memory and publishes aggregate metrics only; no
individual development dataset is added to the repository. One input
has no book benchmark, leaving 170 matched fixtures. Reconstructed evaluations
are labelled retrospective and never enter the live/prospective ledger.

On those same 170 fixtures (binary Brier; smaller is better):

| Event | Previous calibrated | New combined | Book benchmark |
| --- | ---: | ---: | ---: |
| Over 1.5 HT | .244568 | .231921 | .230836 |
| BTTS HT yes | .203140 | .194213 | .190764 |
| Over 2.5 FT | .242443 | .228116 | .227200 |

This is development evidence, not future validation. Newly added markets have
settled historical outcomes but no operator-specific historical prices, so there
is no retrospective price/ROI claim for them. Both scans run the same research
script; current live audit is published separately in
`data/arab_quote_multimarket_report.json`.

## Primary references

- Egidi, Pauli, Torelli, *Combining historical data and bookmakers' odds in
  modelling football scores* (2018):
  https://www.statmod.org/smij/Vol18/Iss5-6/Egidi/Abstract.html
  Supports investigating historical/market information jointly. This release
  does not implement their hierarchical Bayesian model or inherit its validation.
- API-Football official endpoint and odds-history guide:
  https://www.api-football.com/news/post/how-to-get-started-with-api-football-the-complete-beginners-guide
  Pre-match history is limited; archive observations when acquired. Pre-match and
  live market IDs use different namespaces.


## Fixed September signal protocol (observation only)

`signal_research.py` adds `september-signals-v1` under `signal_observations` in
 the existing prematch ledger. It does not change MODEL_ID, probabilities,
 candidate gates, UI, or the existing legacy drought calculation. The coherent
 model continues to use baseline lambdas without a drought bonus.

Every scan appends an immutable timestamped observation before kickoff, including
 unavailable/outside-band cases, current forecasts, raw paired quotes and source
 games. Existing observations and older frozen forecasts are not backfilled.
 The daily auditor archives and settles this collection through the existing
 one-request-per-pending-date path. Results appear in `signal_research` in the
 prematch report and in the auditor's final report, ordered by kickoff.

The favourite cohort uses the first snapshot with at least two valid complete
 1X2 operator pairs, regardless of band or direction; later scans never move
 that fixture to a more favourable cohort. Both mean home prices use exactly the
 same operators. First observed mean 1.50–1.70, drop at least 2%, rise at least
 2%, otherwise stable. Unknown/stale/future timestamps and duplicate operators
 are excluded. Quote freshness is at most six hours. Initial observations are
 not certified opening prices, and current observations are not closing prices.
 These paired means differ from Football-Data's market averages, so this is a
 prospective operational hypothesis, not an exact replication of September.

Team baselines use the FIRST snapshot: last eight distinct prior matches in the
 fixture's league, including previous seasons when already in the fetched data.
 Dates must be strictly before the observation date in Europe/Rome; same-day
 history is excluded conservatively. Verified regulation-time scores are required;
 legacy zero-filled values are not used. No extra history/odds calls are added.
 Mean scored/conceded >=1.5 and the latest two scores equal to zero define the
 drought groups. Unavailable first baselines remain unavailable for that fixture.

The report separates development before 20 October from prospective observations,
 reports all requested goal/combination outcomes (unpriced outcomes have no ROI),
 Wilson intervals and unavailable coverage. Team rows are team-fixture observations,
 not independent games. Repeated scans do not inflate hit-rate denominators.
 The statistical uncertainty and non-matched opponent strength remain limitations.
 No threshold tuning, automatic bonus, or claim of profitability is introduced.

## Exact market selection (dashboard 1.2.1)

The dashboard now exposes exact market/side filters and a selector on each match.
First-half, full-time and combination group views open the full comparison list.
New lines: Over/Under 2.5 HT and Over/Under 1.5 FT. New combinations:
Over 0.5 HT + Over 0.5 SH, and Over 1.5 HT + Over 1.5 SH, alongside the existing
Over 1.5 HT + Over 0.5 SH. Combination complements are retained in the ledger.
The three conjunctions use the same independent-half Poisson assumption;
none receives a synthetic bookmaker quote or EV.

The expanded catalogue can add bookmaker constraints to rate fitting, so the
model ID is bumped to `coherent-markets-v2`. Existing v1 snapshots are preserved
and reports separate the versions. Source-market IDs/names and exact lines must
still match. Additional lines reuse the existing odds response. New snapshots
are frozen only before kickoff. Old daily JSONs may lack new markets until a
successful scan publishes v2 data; the UI does not fabricate those probabilities.

## Single proposal (dashboard 1.3.0, primary-v1)

Replaces the three mandatory suggestions. UI ranks finite model rows by evidence
quality (fresh backend candidate; usable book and no statistical coverage flags;
statistical quality only; incomplete), then statistical-versus-book agreement
within 10 percentage points, conservative EV where book evidence is usable,
combined probability and stable market/side key. Two operators and a quote aged
0–6 hours are required for usable book evidence. Combination complements are
excluded. The day cohort and UI filters do not determine ranking.

The agreement threshold is a provisional display rule, not calibrated confidence.
Agreement does not boost probability. Divergence explanations say whether the
existing combined output is nearer to book or statistics, not that an optimal
source weight has been learned. Fallbacks are explicitly exploratory, including
missing/stale book data; no missing probabilities are generated. Unavailable
markets remain visible as missing in the four fixed comparisons: Over 1.5 HT,
BTTS HT yes, Over 2.5 FT, Over 3.5 FT. Every comparison includes its own reason.

This change does not alter backend probability formulas, candidate gates or
previous frozen forecasts. The proposal itself is presentation-time selection,
not an additional frozen recommendation ledger; do not report it as an audited
historical recommendation. Forecast archival remains unchanged.

## Current data and coefficient inventory

Coefficients below belong to sequential transformations, NOT an additive feature
importance breakdown or percentages of a final probability. Source: active
build_lambdas wrappers in src/3appdays.py and evaluate in multimarket_model.py.

| Data | Current role / coefficient |
| --- | --- |
| Goals scored/conceded FT and HT, home/away | Attack/defence base relative to league means; rates shrink to league with sample/8, capped at 1 (HT event frequencies use sample/6). |
| Home/away-specific history | Context weight 0, .20, .33, .46, .55, .62 for 0,1,2,3,4,5+ observations; remaining weight to overall history. |
| Recency | Date/order-weighted historical observations; recent five versus previous five tracked. |
| xG for/against | FT blend 0/.20/.35/.50/.60 for 0/1–3/4–6/7–10/11+ paired samples. HT adjustment limited to ±6%. |
| Shots on target for/against | FT blend .05/.10/.15 for 2–3/4–7/8+ paired samples; multiplied by .55 when xG is available; assumed .30 goals per shot on target. HT adjustment limited to ±5%. |
| Total shots and big chances | Collected where available; no separate direct coefficient in the coherent model. |
| Finishing/form | Regression factor ±8% with xG, ±6% goals-only; recent points gap contributes at most ±2.16% to FT rates. |
| H2H | At least 2 usable matches; FT and HT rate factors each limited to ±5%. |
| HT scored/conceded frequencies | Attack/defence event input .52/.48, shrunk toward league; blended into HT rates with .20/.27/.34 for context samples <3/3–4/5+. |
| Recent HT trend | Mean change in total HT goals times .025; rate factor limited to ±4%. |
| Empirical Over/GG frequencies | Stored, and used by the legacy probability layer (.18/.26/.32 blend); not an additional direct blend in coherent-markets-v2. |
| Book odds and margin | Complete outcome pairs normalized per operator, median probabilities renormalized. Each eligible market contributes equally to fitting loss (outcomes averaged within market). At least 2 operators for fit targets. |
| Combined statistical/book model | Four rates fitted to book targets with log-rate prior penalty .01; market projection penalty .0001. These are NOT 1%/99% source weights. |
| Drought / goal absence | Observed for research; direct bonus 0 in coherent model. Legacy drought layer is explicitly bypassed through baseline_lambdas. |
| Quote changes, breadth/consensus, margin changes | Recorded with same-operator comparisons; direct probability bonus 0 in coherent model. |
| OCI and favorite/trap patterns | Recorded diagnostics, no direct coherent probability coefficient. |
| Data reliability | Index: overall team samples 34%, home/away samples 36%, league sample 15%, xG coverage 15%. Candidate threshold 60; not event probability. |
| Quote freshness / sample coverage | Candidate gates: at least 2 books, quote at most 6h old, HT samples at least 6 per team; spread <=10pp and positive conservative EV. |
| Country/competition classification | Eligibility/context, not an automatic probability bonus. |

Prospective tests must measure the incremental benefit of each feature without
counting correlated inputs (e.g. xG and shots, odds and OCI) as independent evidence.
