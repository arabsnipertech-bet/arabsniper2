CREATE TABLE IF NOT EXISTS passes (
 checkout_id TEXT PRIMARY KEY,
 email TEXT NOT NULL,
 plan TEXT NOT NULL CHECK(plan IN ('daily','weekly')),
 payment_intent TEXT NOT NULL,
 starts_at INTEGER NOT NULL,
 expires_at INTEGER NOT NULL,
 revoked INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS passes_email_expiry ON passes(email, expires_at);
CREATE INDEX IF NOT EXISTS passes_payment ON passes(payment_intent);
