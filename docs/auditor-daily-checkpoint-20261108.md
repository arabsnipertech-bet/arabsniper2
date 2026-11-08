# Auditor daily checkpoint repair — 8 November 2026

On 8 November, runs `42426327610` and `42441234737` built the local
7 November daily report and published calibration, then hit the protected
deadline inside `gold_storage.py publish`. This did not prove that the daily
report reached the durable private checkpoint. At revision
`dfdf7bd20b75bf53bae84f4b87c2a0c94b0ed8d3`, the public results dated
7 November still exposed `available: false` and no result rows.

## Change

- The priority daily save uploads only the dated frozen ledger, report,
  official results, learning evidence, receipt, calibration weights and
  priority status, plus their referenced immutable parts.
- It verifies the receipt, hashes, report date, weights version,
  validation cycle and `as_of` before advancing the private manifest.
  Unrelated scans, pending journals and historical progress retain their
  existing manifest entries. A failed upload leaves the old pointer usable.
- Historical checkpoints save private state without rebuilding Gold browser
  pages. Fast and Night retain the existing full Gold publication path.
- If the private save succeeded but public publication failed, a retry
  rebuilds public results without repeating daily settlement or calibration.
- Date-filtered journal reads preserve the original observations. Recovery
  includes policy journals as well as forecast journals, retaining documented
  publication markers and Study observations.

No eligibility thresholds, match categories, predictions or settlement rules
are changed. A daily receipt remains distinct from full historical Auditor
completion.

## Validation

The Python suite passed 651 tests. After the final provenance validation
changes, the 49 focused storage, priority, history and recovery tests also
passed. Changed Python modules compile and `git diff --check` passes.
New regression coverage includes interrupted uploads, preserved scan state,
immutable-part validation, stale or mismatched weights, repeated publication,
and the latest original policy observation during recovery. Test storage and
provider data are local fixtures; this is not a production-volume timing test.

## Activation and recovery

Production has not been modified by preparing this branch. The original
monitoring mandate explicitly excluded production writes. After approval:

1. Merge the reviewed fix after its checks pass.
2. Run the existing `Recover verified daily results` workflow on `main`
   with the explicit date `2026-11-07`. Its preparation stays outside the
   data-writer mutex; publication acquires the existing mutex and rechecks
   the immutable source set before changing the private manifest.
3. Verify the `published_receipt`, full dated learning chain and public
   `available` flag. Record losses and pending outcomes exactly as returned.
   Do not mark the open anomaly resolved merely because a workflow is green.
4. Preserve the receipt hashes and feed commit in the existing weekly
   register. The next nightly run can checkpoint 8 November independently of
   any unfinished historical batch, while retaining the protected scan hours.

The recovery command, for an authenticated authorized operator, is:

```bash
gh workflow run recover-daily-results.yml --ref main -f date=2026-11-07
```

The 7 November anomaly remains open until those production results are
verified. No unavailable private values are filled with estimates.
