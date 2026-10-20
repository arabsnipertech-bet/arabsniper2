# ArabSniper 1.4.0 — context-goals-v3

## Contract

Night scans today plus four days; Fast scans today and tomorrow, reusing cached
opportunities. Day files outside Fast's horizon retain the Night timestamp.
The old fields remain for comparison; the primary view uses `multimarket.model_id`.

The statistical model never reads odds. Each market exposes statistics and bookmaker
fair probability separately. Bookmaker probabilities normalize a complete market
per operator, then normalize the median across operators. Updates older than six
hours or without a provider timestamp cannot enter the current benchmark.

## Inputs and effective weights

- At most eight completed official current-season matches before observation time.
  A retrieval of 32 candidates replaces friendlies/excluded seasons; only eight
  enter the analysis. Historical domestic cup matches remain eligible; proposed
  domestic cup fixtures are excluded. Listed international cups remain eligible.
- Home and away contexts use only these eight records. FT uses the actual 90-minute
  fulltime score, including fixtures which went to extra time.
- PT uses the four most recent valid halves within the eight; missing is not zero.
  When there is no usable PT evidence, use FT × observed league PT/FT ratio and
  label it indirect. Without an observed league ratio, PT is unavailable; FT survives.
- Each attack/defence component mixes context and overall means with context
  weight `context_n / n`. Quality weight is `(n/window) * (.5 + .5*context_n/n)`.
  Opposing attack/defence weights normalize their quality. They need not be 50/50.
- xG, when present, replaces up to 25% of a FT component, scaled by `xg_n/8`.
  This coefficient is a provisional modelling choice, not a backtested optimum.
  Shots on target are an alternative qualitative corroboration, not another
  independent numerical bonus. Conversion and coverage are displayed.
- Trends: four recent versus four preceding FT; two versus two valid PT.
  Trend and sensitivity to the most prolific match affect evidence ranking;
  they do not add another goal bonus. No result is removed.
- Context correction is active and experimental: compare the latest context
  fixture to the preceding context mean (at least three prior observations).
  Apply 2% per goal gap, cap at ±5%. Compare the context mean to 60/40 context/
  opposite mean: distance ≤.2 retains 25%; ≤.5 retains 50%; otherwise 100%.
  Opportunities retain 100% when supportive, 50% when unknown, 0 when contrary.
  Low-conceding defences (<.75) receive no positive goal-due adjustment.
  Attack and defence corrections use their existing weights, never sum bonuses.
  No FT correction is automatically applied to PT. The uncorrected model is frozen
  alongside the active one. Drought is not evidence that a goal is mathematically due.
- Opponent strength before historical fixtures is unavailable and has no fabricated
  weight. Current 1X2 strength is shown separately as bookmaker context.

Four independent Poisson goal rates generate coherent outcomes and complements.
Independence between halves and teams is a model assumption, not validated fact.
GG PT cannot exceed Over 1.5 PT; both remain below Over .5 PT. FT missing halves
are computed from FT totals without publishing artificial halftime observations.

## Proposal and evidence

One primary hypothesis per usable fixture. Independent evidence blocks are goals,
form/continuity, opportunities and bookmaker comparison. Each block contributes
-1, 0 or +1; the total is an ordering score, **not a success percentage**. Goals are
compared with the same market's league reference (two-point provisional tolerance),
not one probability threshold for all markets. Ranking uses block score, coverage,
contradictions and probability as a final tie-breaker. No mandatory number of alternatives.

Book agreement within ten percentage points is a provisional descriptive criterion.
It does not raise statistical probability. Divergent sources remain visible. With
at least three observations, statistics supplies the proposed interpretation;
otherwise an available bookmaker benchmark is labelled as the source. All
alternative market rows remain selectable with up to three pros, three cons and
separate missing-data messages. This is not an audited optimal strategy.

A net favourite (fair 1X2 ≥60%, at least two operators), clean sheet, underdog scoring
drought, nondeclining opportunities and both relevant PT frequencies ≥2/4 create
an underdog PT context signal. It works both home and away. A confirmed favourite
drop is shown separately. The signal does not imply GG or Over 1.5, or add a bonus.

## Market movement

Only complete quotes from the same operator/market/side are paired. Comparable
start/end observations must be within one hour. Confirmation needs two operators
and strictly more than half of comparable operators moving the same way.
A single operator never triggers a confirmed movement. Reversals show ⇅ and
use the last leg for intensity: green <3%, yellow 3–<8%, red ≥8%.
The complete observed sequence and timestamps are available in the card.

OCI-AS is a descriptive signed 0–100 index: magnitude 40, consensus 25,
velocity 10, persistence 15, operator coverage 10. It uses paired operators and
last-leg fair-probability change. Margin is sum of inverse odds minus one; neither
margin nor OCI measures bookmaker money flows or actual earnings. They share
inputs with odds movement and do not count as independent votes. Initial observed
quote is not certified opening price.

## Archiving and audit

`multimarket_evaluations` retains the immutable first snapshot per model/fixture.
`analysis_updates` appends every prematch scan, all proposed and unselected markets,
weights and evidence, timestamp and change reason. Settlement only adds outcomes.
The auditor archives this collection by fixture date; first and latest prematch
snapshots are reported separately, never counted as independent fixtures.
Statistical and without-correction variants share the same outcome cohort.
Legacy versions remain separate. No retrospective forecast becomes prospective.

## Delivery

Engine and dashboard version 1.4.0. Dashboard source:
`web/gold-dashboard-live.html`. Backend is run through GitHub Actions, not by
Cloudflare. Cloudflare must serve this updated HTML (or build the linked commit).
No claim of live deployment should be made until the public version is checked.

## Audit storage recovery (20 October 2026)

The second Fast run hit GitHub's 100 MiB file limit: computation succeeded but
publication failed at 103.09 MiB. `audit_storage.py` transparently migrates large
JSON to an atomic manifest plus immutable gzip parts in `_audit_parts/`.
Each part contains at most 4 MiB of original UTF-8 bytes. SHA-256 validates each
part and the reassembled document. The existing JSON format is still readable.
Missing/corrupt parts stop processing; they never reset historical evidence.
No forecast or old part is deleted. The current auditor, research reader and both
scan publishers share this format. Parts and their manifests are committed together.

Compatibility: state schema 1.4 retains the existing 1.1/1.2 quote ledger; a version
change must not reset it. Scan failures retain generated public data in a recovery
artifact for 14 days. Archived daily forecasts are read through the same storage API.
