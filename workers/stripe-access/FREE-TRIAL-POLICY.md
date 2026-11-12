# Permanent free promotion eligibility

Owner approved 12 November 2026. A granted Telegram campaign trial permanently disqualifies the matching normalized email OR Telegram ID from later free trials and campaigns. The current campaign retains its retry/idempotency behavior; historical pre-campaign trials can still claim this final campaign once.

`telegram_campaign_claims` is the permanent eligibility ledger: retain granted rows after expiry and after campaign closure. Both the email stage and verified OTP grant stage check it. Never reset/delete this history to launch another campaign; all future free-promotion entry points must enforce the same eligibility check. Reserved/unfulfilled claims do not count as consumed promotions. This does not identify a person who changes both email and Telegram account.

Existing access expiry, paid purchases, and eligible renewal discounts are unchanged. Deployment patches only the exact prior Telegram module, privately backs up the Worker, preserves settings and verifies deployed content plus renewal regression tests. No customer messages or promotional posts are sent by deployment.
