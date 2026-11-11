-- Additive: original telegram_trial_claims and all existing entitlements remain intact.
CREATE TABLE IF NOT EXISTS telegram_campaign_claims (
 campaign TEXT NOT NULL,
 email TEXT NOT NULL,
 telegram_id TEXT NOT NULL,
 requested_at INTEGER NOT NULL,
 base_expires_at INTEGER NOT NULL,
 expires_at INTEGER NOT NULL,
 status TEXT NOT NULL CHECK(status IN ('reserved','granted')),
 action TEXT CHECK(action IN ('create','extend')),
 customer_id TEXT,
 subscription_id TEXT,
 granted_at INTEGER,
 PRIMARY KEY(campaign,email),
 UNIQUE(campaign,telegram_id),
 CHECK(expires_at=base_expires_at+604800)
);
