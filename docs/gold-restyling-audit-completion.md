# Gold Matchroom and prospective audit completion

The Gold dashboard now uses the approved Matchroom layout with same-operator
opening/current 1X2 quotes, percentage movements, a selectable full matrix and
match instruments. Goal expectations and observed scoring/conceding means have
separate PT/ST rows. ST means use paired regulation-time FT/HT scores from the
same historical games; absent paired scores remain unavailable. EV remains a
backend input and is not displayed in the new matrix. Existing authentication,
private data loading, historical categories and live-score overlay are retained.

## Prospective evidence

`statistics_first.statistical_analysis` stores `without_peak` in each observed
goal profile and freezes `anti_peak_most_prolific` in the assessment. Both periods
record sample size, original mean and mean with exactly one highest-scoring
game removed. Insufficient observations are explicitly unavailable. New shadow
captures include this evidence in their SHA256 identity; legacy frozen captures
are not rewritten. Audit summaries expose per-snapshot experiment availability.

`decision-shadow-v2.2` labels the market-fair comparison with
`fair_quote_probability_flow_v1`. Admission uses the selected operator's
margin-free fair quote, probability, quote-quality, structural and flow gates.
It excludes model-value/EV admission gates, which are degenerate at a fair quote.
Published Gold calls do not enable this option. Frozen decision rows retain the
actual gross operator quote for settlement and theoretical returns, and expose
the fair `eligibility_odd` and actual `gross_quote_ev_pct` separately. Audit
protocol counts distinguish this comparison from legacy snapshots.

## Daily pending counts

`pending_summary` is stored in the daily JSON, its receipt and encrypted repair
certificate. It counts distinct unresolved fixture IDs across all daily ledger
collections and separates fixtures with an observed, valid prematch rank-1 Gold
from non-Gold fixtures. Duplicate scans do not multiply fixture counts. Pending
records lacking a fixture ID have their own counter. This summary explains the
daily `pending` boolean independently of decision-path `pending_choices`.

Legacy seals without counts remain explicitly unavailable. A certificate rejects
disagreement between a new receipt and its sealed report's pending summary.
No historical prediction is rebuilt to populate missing experimental variants.

Telegram/Facebook publishers, schedules, templates and credentials are unchanged.
