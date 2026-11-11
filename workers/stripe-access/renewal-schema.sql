-- Additive migration: preserves passes, OTP sessions and administrative grants.
CREATE TABLE IF NOT EXISTS gold_access_cache (
 email TEXT PRIMARY KEY, payload TEXT NOT NULL, valid_until INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS gold_renewals (
 id TEXT PRIMARY KEY, email TEXT NOT NULL, plan TEXT NOT NULL,
 cycle_expires_at INTEGER NOT NULL, promo INTEGER NOT NULL CHECK(promo IN (0,1)),
 amount INTEGER NOT NULL, created_at INTEGER NOT NULL, expires_at INTEGER NOT NULL,
 stripe_params TEXT NOT NULL, checkout_id TEXT UNIQUE, checkout_url TEXT,
 status TEXT NOT NULL CHECK(status IN ('creating','open','fulfilled','expired')),
 payment_intent TEXT, fulfilled_at INTEGER
);
CREATE UNIQUE INDEX IF NOT EXISTS gold_renewal_pending
 ON gold_renewals(email) WHERE status IN ('creating','open');
CREATE UNIQUE INDEX IF NOT EXISTS gold_renewal_promo_cycle
 ON gold_renewals(email,cycle_expires_at) WHERE promo=1 AND status!='expired';
CREATE INDEX IF NOT EXISTS gold_renewals_email ON gold_renewals(email,created_at);
