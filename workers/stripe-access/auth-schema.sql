CREATE TABLE IF NOT EXISTS gold_codes (
 email TEXT PRIMARY KEY, challenge TEXT NOT NULL, digest TEXT NOT NULL,
 expires_at INTEGER NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
 sent_at INTEGER NOT NULL, consumed INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS gold_sessions (
 digest TEXT PRIMARY KEY, email TEXT NOT NULL, expires_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS gold_sessions_expiry ON gold_sessions(expires_at);
CREATE TABLE IF NOT EXISTS gold_limits (
 key TEXT PRIMARY KEY, hits INTEGER NOT NULL, expires_at INTEGER NOT NULL
);
