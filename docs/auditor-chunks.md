# Bounded nightly Auditor recovery

Effective November 5, 2026. Selection thresholds, market settlement formulas and the V5.1 freeze are unchanged.

- Production admission is 00:30–01:30 and 04:00–05:10 Europe/Rome (end exclusive). All phases are forcibly bounded by 01:50 or 05:30, leaving the scheduled 02:00 Night Scan and 06:00 Fast Scan clear. An active or queued data writer prevents admission.
- Automatic schedules are 00:30 and 04:30. The existing external Auditor watchdog can resume partial work in the same permitted windows. Manual recovery uses the identical admission gate and cannot bypass it during the day.
- Preparation consumes up to eight immutable scan journals per invocation, before the frozen batch's midnight cutoff. Later scans remain pending for the next batch. The legacy combined forecast archive remains updated; only dates touched by the consumed journals are repartitioned.
- Preparation is checkpointed as `preparing` before any daily settlement. A successful partial run emits `completed=false` and `paused=true`, never the full-completion proof expected by the watchdog.
- Historical recovery processes at most two dates per invocation and publishes R2 after each date. A per-date receipt binds the input pointer and daily report, outcomes, Markdown and calibration outputs by SHA-256. Changed evidence triggers recalculation; missing or changed saved outputs stop certification.
- Historical resumes have a 45-minute budget including restore. Preparation has at most 75 minutes, additionally capped by the protected window boundary. A new history date or final assembly is not started with under 20 minutes remaining. A hard timeout may require repeating only work since the latest committed checkpoint.
- Completed terminal dates can be reused across batches. Pending outcomes retain their original status and the existing 14-day lookup limit. Aggregate reports are assembled after all required date receipts are available. Full certification is written only after aggregate/comparison/calibration and final publication succeed.

The previous daytime recovery run 42077586094 was explicitly cancelled to release the Fast queue. Already published checkpoints were retained; uncommitted computations were not certified. The first production night must confirm actual durations and throughput; passing tests alone does not certify completion of the historical backlog.
