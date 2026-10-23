# Gold private data rollout

Status: preparation only. Public login and scan workflows are unchanged.
OTP frontend/backend changes remain in draft PR #39. Do not merge that PR before
all production prerequisites below are met.

## Credentials and first cloud verification

1. Keep `arabsniper-gold-test` and create private production bucket `arabsniper-gold`.
   Use default jurisdiction (the SDK endpoint here is the default account endpoint).
   Leave r2.dev and custom public domains disabled.
2. Create an R2 S3 credential with Object Read & Write, scoped only to these buckets.
   In repository Settings > Secrets and variables > Actions, save Access Key ID as
   `R2_ACCESS_KEY_ID` and Secret Access Key as `R2_SECRET_ACCESS_KEY`. Never put them
   in a commit, issue, workflow input, screenshot or chat.
3. Run `Verify private Gold storage` with the test bucket. This is a manual bootstrap,
   NOT the production scanner publisher. It uploads current data and verifies a full
   download with checksums plus all seven dashboard feeds. It never modifies git data
   or activates production auth. It replaces the old test fixture with real data.
4. Recheck the staging login, data display, logout, anonymous denial and expired access.

The checkpoint pointer is written last; failed upload does not replace the previous
checkpoint. Dashboard feeds currently use individual object names; they are not an
atomic release and must not be represented as one. Immutable checkpoint objects are
reused between runs. Old objects are retained for recovery; add retention after rollout.

## Required before production activation

- Migrate every data writer (Fast, Night, freeze, auditor and manual tools) to restore
  private state before running and publish it before public projections. Share the
  existing writer concurrency group. No fallback to public/stale data on R2 failure.
- In private mode, stop all public Gold commits and recovery artifacts. Only homepage
  preview, settled results and explicitly reviewed operational public data may remain.
  Review Pages output, previews and old entry points. Historical public copies cannot
  be made secret retroactively.
- Configure production GOLD_DATA binding, auth tables in the existing ACCESS_DB without
  changing passes, Resend/OTP secrets, production origin, precise auth/data routes and
  cleanup schedule. Keep Stripe live configuration and existing payment routes.
- Bootstrap and verify production before setting GOLD_PRIVATE=true. The bootstrap
  workflow then deliberately refuses to overwrite authoritative private state from git.
- Deploy production Worker code (not the test-only bundle), enable auth only with the
  coordinated dashboard release, and verify login, bad/expired code, reuse, session,
  logout, nonpaying access, existing passes and monthly access. Test webhook duplicate,
  delayed payment and refund handling without charging a real customer unnecessarily.
- Confirm real scan completion, latest update time and monitoring for data/email/payment
  failures before advertising. If release fails, preserve private data and repair the
  protected path; do not restore anonymous access to Gold feeds.
