# Quantitative chain finalization — 7 November 2026

Baseline: `285d3197a47968fbb3525c1ef631ca6e6631018d` (PR #146).
Historical forecast hashes remain unchanged. Results and derived reports can be regenerated; original probabilities, quotes, fixture IDs and capture times cannot.

| Block | Implementation / verification |
|---|---|
| Recovery | `auditor/recover_daily.py`: original daily archive wins; master streamed through `entries`; journal observations fill missing identities; all prior fingerprints verified before manifest publication. Explicit refresh regenerates complete results/calibration/daily JSON+Markdown/seal even when an earlier seal exists. Final official results are reused; unresolved fixtures require official 90-minute fulltime scores, including AET/PEN. |
| Identity | `daily_audit.same_frozen_forecast` ignores only root ingestion `update_reason` and mutable settlement for reconciliation. Existing `fingerprint` stays byte-for-byte compatible; descriptive differences never rewrite archived records. |
| Provenance | Daily report includes hashes of frozen archive, settled results and calibration evidence. Weights include sorted dates, per-file SHA-256 and sample counts, window 28 days / half-life 14. Missing evidence differs from an explicit empty day and blocks training publication. |
| Markets/history | Over 2.5 HT already mapped and monotonic constraints already enforced. Micro=50, macro=25, prior=50 preserved. Explicit FT history cap=8; HT uses 4 observed halves. HT xG remains observed-only; drought bonus remains zero. |
| Prices/flow | Same-book complete-partition margin and cache-origin freshness remain mandatory. Existing rise>2%, OCI<-20 guards retained. Divergence>15pp now requires genuinely confirmed positive flow plus negative same-book delta; a tiny positive OCI is insufficient. |
| Ranking | Published Gold remains `gold-goals-v6.1`. Actual baseline Over 2.5 FT floor is 1.70, not 1.85. No silent threshold/ranking promotion. |
| Platinum/Radar | Platinum eligibility unchanged; positive-central-and-prudent-EV cohort separated from flow-only negative/unknown EV. Existing Radar supported-quality/executable quote/nonnegative prudent and positive central EV gate preserved. Contract and distinct-fixture denominators added. |
| Decisions | `decision_shadow`, `decision_metrics`, `official_selections`: identical selector across fair/pure/anchored/calibrated inputs, first/latest cohorts, activation counts, changes vs pure Poisson, pending choices, hit rate, average odds, unit returns, ROI and deterministic fixture-clustered bootstrap interval. Frozen official first Gold, latest-snapshot Gold and all eligible fixture/markets explicitly separate from counterfactual decisions. |
| Storage/UI | Existing master materialization guard and AuditLedger streaming retained. SHA digest itself now streams. Production UI preserved; 360/390/1280 browser checks remain in CI. CI runs every Node test file, plus all three browser scripts. |

## Isolated research; no production promotion

Each decision experiment changes one hypothesis: removal of automatic Over upgrade priority; divergence caps 10 FT/8 HT; market-prior blend (30% FT/45% HT); Over 2.5 FT minimum 1.85; exclusion of the most prolific historical match; total-lambda expansion with half compensation and +10% cap. Lambda compares against anchored Poisson, other experiments against calibrated V5.1. Missing original-stage or historical anti-peak evidence is not fabricated.

Symmetric alpha is a separate training shadow: new paired divergent fixtures accumulate until 25, are consumed once, and permit cooling towards 0.15 when model Brier is no worse than fair. Retries cannot ratchet alpha. Published alpha behavior remains unchanged pending validation.

The pre-existing anti-peak gate removes the latest match. Removing the most prolific match changes selection, so it is measured in shadow before promotion. Historical Colab OCI returns are not used to relax any gate.

## Interpretation limits

Fixed-contract ROI is intentionally not a policy comparison. Decision ROI uses each path's actual selected quotes. Bootstrap intervals group by fixture and are absent for fewer than two independent fixtures; they do not certify profitability or correct model-selection bias. Gross unit-stake profit minus stake excludes fees, tax and slippage. First/latest populations overlap and must not be summed.

Historical frozen shadow-v1 decisions remain v1. New hypotheses are frozen prospectively in v2; an explicitly labelled replay is used only when the historical shadow snapshot is absent. Social picks require actual delivery receipts and remain explicitly unavailable in the audit when those receipts have not been loaded. Eligibility is never equated with a social publication.

Recovery workflow contains no Telegram/Facebook posting steps. Live numerical promotion and Gold restyling are separate from this update.

## Recovery and shadow follow-through

The date-aware AuditLedger reader verifies the complete original JSON/chunk stream but stops constructing nested record payloads once an observed kickoff belongs to another Rome date. Unknown dates still reach the existing strict identity/date validator. Recovery logs identify each original source and print the verified/published seal hashes and pending status; no prediction payload or credential is logged.

Shadow v2.1 chooses the same complete, fresh 1X2 partition as the published anchor. If the central expansion condition is inactive, it reuses the anchored decision path exactly; uncertainty scenarios cannot introduce an expansion on their own. Historical frozen shadow versions remain identifiable and unchanged.

The final calibration job verifies the requested dates against the live R2 manifest, sealed outputs and the exact calibration hashes read by training before publishing weights. It exports alpha, evidence provenance and aggregate first/latest decision/variant/cohort metrics as an authenticated encrypted artifact. Raw forecasts are excluded. Only the public recipient key is committed; the private key is kept outside the repository.
