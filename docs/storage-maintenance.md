# Storage maintenance

Current forecasts, results and historical audit JSON are retained, including legacy
March–July reports. The cleanup only removes SHA-256-named compressed parts that
are not referenced by any manifest under data, auditordata or auditarchive.
Every manifest is fully read and checksummed before any deletion; malformed JSON,
unknown storage formats and missing/corrupt chunks abort the operation. Unknown
files are retained. No forecast probability or settlement is recalculated.

`python src/storage_cleanup.py` reports candidates without mutation.
`python src/storage_cleanup.py --apply` removes them and verifies all manifests again.
The workflow runs weekly or manually, sharing the scan/auditor writer lock. A
concurrent remote update rejects the push; it never force-pushes or overwrites it.

Old source copies, duplicate legacy scan workflows, tests targeting the absent
3appdays_v25.py, the superseded temporary-audit deletion workflow, and the completed
one-off night recovery workflow are removed from the current tree. They remain
recoverable through Git history. The existing manual historical auditor remains.

This reduces current checkouts and deployments; it does not purge historical Git
objects, remove old Actions runs/artifacts, or delete Cloudflare deployments.
Historical commit URLs used by model research remain valid. For rollback, deploy
an entire earlier commit rather than copying an old manifest into current main.

The prematch freezer loads each daily archive once per execution. Do not put
`read(...)` inside `dict.setdefault(...)`: Python evaluates that argument for
every record, repeatedly decompressing and checking the same archive until the
workflow times out. Existing frozen records are retained unchanged; only missing
keys with valid prematch timestamps are appended. The daily auditor uses the
same archive function before settling results.
