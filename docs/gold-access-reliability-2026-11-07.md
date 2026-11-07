# Gold access and loading reliability — 7 November 2026

The login now separates email and code steps, success from errors, resend from email changes, and supports OTP paste/autofill. A pending challenge (never the OTP itself) survives a reload in session storage when available. Storage-denied browsers still perform authentication. Authentication has a 20-second timeout and no automatic retries of email sends. Server entitlement, rate limits, cookies and OTP rules are unchanged.

Authenticated data reads retry transient GET failures, bound request duration, cancel replaced calendar requests and recognize 401/403 even on data pages. Failed manual refreshes retain an explicitly dated last successful view. SHA-256 and byte/count validation remain mandatory. Older raw JSON and audit-parts formats still load.

The private metadata endpoint `arab_quote_model_research.json` retains its research fields and gains `_gold_browser_days` and `_gold_audit_summary`. Each day index points to immutable, gzip-compressed complete JSON arrays under the already-authenticated `_audit_parts` path. All fields, order, probabilities and source timestamps are retained; the browser parses at most two approximately 8 MiB pages concurrently instead of a whole 135 MiB JSON document. A single fixture exceeding the target is kept together. Yesterday/current archive indexes are fetched only when needed. This reduces transient parsing memory; the complete selected day's parsed objects still reside in memory.

Original daily root feeds used by Telegram/Facebook and raw state checkpoints keep their existing format. The history workflow refreshes the catalog under the shared writer lock, checking source ETags before replacement. Normal private publication also regenerates it. No auth Worker or Stripe deployment is needed. No user email has been sent by these tests.

The verified historical day contained 295 matches: 135,048,668 bytes, 17 pages (largest 8,375,265 bytes), 13,946,347 compressed bytes and a 135,567-byte index. These are measured transfer/parser sizes, not a claim of device-specific load time.

Validation covers blocked browser storage, OTP errors/success, resend cooldown, transient 503, mid-page expired session, checksum/count failure, lazy archive lookup, source changes during publication, and preservation of social feeds. The browser suite exercises the code-entry UI using a local synthetic server, with a mobile screenshot. Real email delivery and individual reported device errors require production evidence and are not inferred from synthetic tests.

Each page still requires the existing Worker authentication and entitlement check. Page sizing bounds parsing memory while avoiding excessive per-page authorization requests; no authorization cache or bypass is introduced.
