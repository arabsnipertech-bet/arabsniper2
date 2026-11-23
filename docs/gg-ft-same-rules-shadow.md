# GG FT in Consigliata: prospective comparison

The live role list and probability model remain unchanged. The new private arm
`nov23_gg_ft_same_rules` measures admitting GG FT to the recommended role. It
does not enable a production Over-to-GG switch or lower the role's price floor.

The arm and `nov22_control` receive the same executable, statistically supported
candidate pool, observed flags, existing recovery certificates and qualified
alternatives. Both use the same routing, ranking and limits. Only the recommended
role's admitted market set changes. A GG candidate is neither forced nor
automatically qualified. Routing still enforces compatibility across the triad;
any downstream slot effects remain visible in its independently frozen picks.

The legacy `gg_ft_recommended` arm retains its meaning. It is not rewritten or
used as the clean control: its Study/unpriced settings differ from the new pair.
The new configuration fingerprint separates future observations from the older
November 22 configurations. Existing snapshots are not reconstructed or changed.

For future conditional analysis, the ledger also copies the native statistical
lambda vector already used by V7, with its original statistics timestamp. Missing
or future statistics remain explicitly unavailable. These rates are observations,
not new probability inputs or an independent vote for the selected market.
Over PT and GG FT retain the same-snapshot 1X2 context with an explicit operator
match flag. This does not change the older cross-market experimental arm.

Metrics distinguish a variant absent from a historical freeze from an available
variant selecting no market. The new arm and its control also have separate
recommended-role summaries, so results are not diluted by the other two slots.
Brier/log-loss deltas still require the same target event. ROI is monitoring only.

This is prospective measurement only: no automatic promotion, new provider call,
social send, frozen-data backfill, schedule change or production selector change.
The next ordinary scan can freeze the new evidence. Results must accumulate and
be evaluated before proposing a conditional switch in production.
