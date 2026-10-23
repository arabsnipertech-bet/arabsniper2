# Production cutover

Keep AUTH_ENABLED=false until the production Worker and OTP dashboard are ready.

1. Set GitHub Actions variable GOLD_STORAGE_ENABLED=true. Keep GOLD_PRIVATE unset/false.
2. Run Fast Scan. In this mirror phase the existing public site keeps working, and every current scanner/auditor/freezer writes its final state to production R2. Confirm the private save step succeeds.
3. Deploy the production auth Worker (never otp-test-bundle.mjs), verify bindings, routes and secrets, and prepare the matching dashboard release.
4. Enable GOLD_PRIVATE=true only during the coordinated cutover. Every writer restores R2 before processing and aborts on failure. It publishes R2 before committing only the explicit public projections. The first successful private publication removes tracked managed Gold files and audit chunks from main.
5. Release the OTP dashboard, enable AUTH_ENABLED=true, and verify login, entitlement, logout, denied anonymous API access, and fresh data after a scan.
6. Verify Pages no longer serves the former public Gold URLs. Past Git history and prior deployments are not erased by this change; retire exposed deployment URLs separately.

All data writers share the same concurrency group. Legacy Manual Auditor and public storage cleanup are disabled in private mode; use Run Auditor for current audits. Private failure artifacts are disabled. Missing storage is fatal: never fall back to public data.

R2 checkpoint transfer uses eight workers, deduplicates objects, and prints progress every 25 transfers. A failed object transfer cannot replace the checkpoint pointer. The seven root Gold projections are still individual writes, not an atomic seven-file release.

The public Git publisher preserves only homepage previews, settled public results, live fixture identifiers and the market catalog. Other staged additions/changes are rejected.
