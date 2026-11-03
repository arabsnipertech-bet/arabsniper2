-- Run on the SAME D1 database bound as ACCESS_DB before enabling Telegram trials.
-- One claim per verified email AND Telegram account; no paid passes are created here.
CREATE TABLE IF NOT EXISTS telegram_trial_codes (
 telegram_id TEXT PRIMARY KEY, email TEXT NOT NULL, digest TEXT NOT NULL,
 expires_at INTEGER NOT NULL, sent_at INTEGER NOT NULL,
 attempts INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS telegram_trial_codes_expiry ON telegram_trial_codes(expires_at);
CREATE TABLE IF NOT EXISTS telegram_trial_claims (
 email TEXT PRIMARY KEY,
 telegram_id TEXT NOT NULL UNIQUE,
 started_at INTEGER NOT NULL,
 expires_at INTEGER NOT NULL,
 status TEXT NOT NULL CHECK(status IN ('reserved','granted')),
 subscription_id TEXT UNIQUE
);
CREATE INDEX IF NOT EXISTS telegram_trial_claims_status ON telegram_trial_claims(status);
CREATE TABLE IF NOT EXISTS telegram_trial_limits (
 key TEXT PRIMARY KEY, hits INTEGER NOT NULL, expires_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS telegram_trial_limits_expiry ON telegram_trial_limits(expires_at);
