-- Execute as one D1 migration before deploying the updated Worker.
CREATE TABLE passes_next (
 checkout_id TEXT PRIMARY KEY,
 email TEXT NOT NULL,
 plan TEXT NOT NULL CHECK(plan IN ('daily','weekly','pass3d','pass14d','pass1m')),
 payment_intent TEXT NOT NULL,
 starts_at INTEGER NOT NULL,
 expires_at INTEGER NOT NULL,
 revoked INTEGER NOT NULL DEFAULT 0
);
INSERT INTO passes_next SELECT checkout_id,email,plan,payment_intent,starts_at,expires_at,revoked FROM passes;
DROP TABLE passes;
ALTER TABLE passes_next RENAME TO passes;
CREATE INDEX IF NOT EXISTS passes_email_expiry ON passes(email, expires_at);
CREATE INDEX IF NOT EXISTS passes_payment ON passes(payment_intent);
