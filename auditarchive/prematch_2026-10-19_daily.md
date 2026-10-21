# Audit 2026-10-19

Experimental. First/latest are separate cohorts, never additive. Paper ROI is not executed profit. No forecast is recalculated.

Generated: 2026-10-21T14:52:09.656379+02:00 | Revision: 5cef6ef3f86de0cdb802538ca5ca0d45c6e2206b

Invalid records excluded: 0

| Snapshot | Model | Period | Fixtures | With primary | Primary settled | Primary pending |
|---|---|---|---:|---:|---:|---:|
| first | coherent-markets-v1 | development | 23 | 5 | 5 | 0 |
| first | coherent-markets-v2 | development | 23 | 2 | 2 | 0 |
| latest | coherent-markets-v1 | development | 23 | 5 | 5 | 0 |
| latest | coherent-markets-v2 | development | 23 | 2 | 2 | 0 |

| Snapshot | Model | Fixture | Captured | Proposal | Quote | Statistics % | Book % | Result | Profit (paper) |
|---|---|---|---|---|---:|---:|---:|---|---:|
| first | coherent-markets-v1 | 1498899 | 2026-10-19T11:26:12.104627+02:00 | 1x2_ft::1 | 3.2 | 34.592 | 27.917 | lost | -1 |
| first | coherent-markets-v1 | 1550154 | 2026-10-19T11:26:12.104627+02:00 | 1x2_ft::2 | 2.45 | 41.438 | 38.685 | won | 1.45 |
| first | coherent-markets-v1 | 1575458 | 2026-10-19T11:26:12.104627+02:00 | 1x2_ft::2 | 5.85 | 23.222 | 15.873 | won | 4.85 |
| first | coherent-markets-v1 | 1612259 | 2026-10-19T11:26:12.104627+02:00 | 1x2_ft::1 | 3.64 | 28.945 | 24.514 | lost | -1 |
| first | coherent-markets-v1 | 1612260 | 2026-10-19T11:26:12.104627+02:00 | 1x2_ft::2 | 7.0 | 24.484 | 13.022 | lost | -1 |
| first | coherent-markets-v2 | 1498899 | 2026-10-19T14:15:40.133400+02:00 | 1x2_ft::1 | 3.2 | 34.592 | 27.917 | lost | -1 |
| first | coherent-markets-v2 | 1575458 | 2026-10-19T14:15:40.133400+02:00 | 1x2_ft::2 | 6.0 | 23.222 | 15.679 | won | 5.0 |
| latest | coherent-markets-v1 | 1498899 | 2026-10-19T11:26:12.104627+02:00 | 1x2_ft::1 | 3.2 | 34.592 | 27.917 | lost | -1 |
| latest | coherent-markets-v1 | 1550154 | 2026-10-19T11:26:12.104627+02:00 | 1x2_ft::2 | 2.45 | 41.438 | 38.685 | won | 1.45 |
| latest | coherent-markets-v1 | 1575458 | 2026-10-19T11:26:12.104627+02:00 | 1x2_ft::2 | 5.85 | 23.222 | 15.873 | won | 4.85 |
| latest | coherent-markets-v1 | 1612259 | 2026-10-19T11:26:12.104627+02:00 | 1x2_ft::1 | 3.64 | 28.945 | 24.514 | lost | -1 |
| latest | coherent-markets-v1 | 1612260 | 2026-10-19T11:26:12.104627+02:00 | 1x2_ft::2 | 7.0 | 24.484 | 13.022 | lost | -1 |
| latest | coherent-markets-v2 | 1498899 | 2026-10-19T14:15:40.133400+02:00 | 1x2_ft::1 | 3.2 | 34.592 | 27.917 | lost | -1 |
| latest | coherent-markets-v2 | 1575458 | 2026-10-19T14:15:40.133400+02:00 | 1x2_ft::2 | 6.0 | 23.222 | 15.679 | won | 5.0 |

See the adjacent JSON for every frozen source key, SHA-256, quote, probability, missing field and result.
A missing result is pending, never a loss. A missing quote is excluded from paper ROI.
