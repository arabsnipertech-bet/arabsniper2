CREATE TABLE IF NOT EXISTS explorer_searches (
 email TEXT NOT NULL, id TEXT NOT NULL, name TEXT NOT NULL, filters TEXT NOT NULL,
 updated_at INTEGER NOT NULL, PRIMARY KEY(email,id)
);
CREATE TABLE IF NOT EXISTS explorer_favorites (
 email TEXT NOT NULL, id TEXT NOT NULL, snapshot TEXT NOT NULL, saved_at INTEGER NOT NULL,
 PRIMARY KEY(email,id)
);
CREATE INDEX IF NOT EXISTS explorer_favorites_order ON explorer_favorites(email,saved_at DESC,id);
