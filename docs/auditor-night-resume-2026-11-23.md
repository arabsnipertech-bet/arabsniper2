# Overnight Auditor recovery — 23 November 2026

## Observed failures

- Auditor run `45108427896` saved the 22 November priority freeze, then exhausted
  its 12 minute budget while writing outcomes. A nested timeout became exit 1.
  This did not certify results or learning for that day.
- Recovery run `45084467620` prepared for 2h42m, including replay of the global
  forecast master and old journals. Publication correctly rejected changed
  source hashes. Its daily report alone took about 17 minutes to build.
- Fast Backup run `45109720175` stopped at the Auditor retry limit before its
  independent Fast check. Auditor Backup `45105764661` hit the same guard.
- Night Scan `45094037109` succeeded. Repeated backup failures are not evidence
  that the Night Scan or all feeds failed.

## Correction

Recovery can reuse an incomplete but verified priority freeze. The dated file
and every eligible journal must match its saved provenance. Publication remains
bound to those day-specific sources and any existing outcomes/receipt, so an
unrelated change to today's global master does not invalidate yesterday's work.
New journals require the full verification path; changed processed journals and
concurrent changes to the frozen day still reject publication. Settlement reuses
the checked disk index instead of decoding the same frozen ledger twice.

The Auditor work budget is 40 minutes, with a 45 minute job limit, retaining the
existing 01:50/05:30 absolute cutoffs and shared writer mutex. A budget pause
saves partial progress and propagates a distinct exit status; it never emits a
completion marker. Other failures still fail. Fast Backup continues its Fast
check after an Auditor recovery error. The separate Auditor guard retains its
failure limit and visibility.

Frozen forecast bytes, model weights, provider result limits, schedules and
social publication rules are unchanged. Tests use synthetic storage and results.
The fix does not retroactively change a recovery already running older code;
production certification must be checked on a subsequent run of this revision.

## Regression coverage

Tests cover no global replay for a covered freeze, one load of the frozen day,
publication after an unrelated master update, rejection of changed or newly
arrived journal evidence, rejection of concurrent frozen-day updates, partial
progress without certification, explicit partial-exit handling, and protected
window cutoffs. Full Python, Node and browser suites run for the pull request.
