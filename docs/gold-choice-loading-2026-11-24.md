# Choice explanations and current-view loading

The small Gold/Explorer box contains at most three numerical references: native
period lambda, saved V7 probability, healthy margin, confirmed opening drop or
documented positive EV. No role narrative or explanatory paragraph is displayed.
Unavailable references are omitted. Failed qualification gates and original EV
remain in collapsed Dati e limiti. Status, prices, probabilities and tiers stay
unchanged. Explorer receives only saved facts matched to the exact prematch SHA
and original probability/price/EV values; no newer snapshot is substituted.

A read-only production profile found 36,354 current Explorer rows in 146 pages,
123,088,594 raw JSON bytes. Each sampled page mixed all five dates and roughly
190 fixtures. The default Today view therefore waited for five days of objects.
Gold's current-day feed contained 50,536,863 decoded bytes in seven gzip pages;
its catalog was also fetched twice concurrently by the audit and day loaders.
These are payload/R2 measurements, not mobile-network timing measurements.

The mutable Explorer current view is grouped by date, kickoff and fixture. Its
page index gives exact date/fixture membership and row counts; the browser loads
all pages needed for the chosen view before ranking. Other days load on demand.
Filters on already-loaded pages reuse immutable objects. Old publications keep
their original full-load fallback; malformed indexes fail visibly. Historical
snapshots, certifications, favorites, ranking rules and auth routes are preserved.
View metrics are explicitly labelled as applying to the selected view.

The one-off release repack verifies every row after reading back the new objects,
preserves generation time, switches the current pointer last, and touches neither
Gold nor historical references. An additional one-off check waits for any older
scan, whose already-checked-out publisher could otherwise restore the old layout.
Future scans produce the indexed layout directly. No new scans/provider calls.

Gold's two concurrent catalog consumers share only the in-flight authenticated
read. A later day opening authenticates again; logout/denial aborts and clears the
shared read. Verified compressed page caching and integrity checks stay intact.
