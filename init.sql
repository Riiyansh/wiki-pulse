-- Wikipedia Pulse — PostgreSQL schema

CREATE TABLE IF NOT EXISTS edits (
    id              BIGSERIAL PRIMARY KEY,
    event_time      TIMESTAMPTZ NOT NULL,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    title           TEXT NOT NULL,
    wiki            TEXT NOT NULL,          -- e.g. "enwiki", "frwiki"
    language        TEXT NOT NULL,          -- e.g. "en", "fr"
    user_name       TEXT,
    is_bot          BOOLEAN NOT NULL DEFAULT FALSE,
    is_new_page     BOOLEAN NOT NULL DEFAULT FALSE,
    delta_bytes     INTEGER,               -- size change (+/-)
    comment         TEXT,
    server_url      TEXT,
    namespace       INTEGER DEFAULT 0      -- 0 = article, 4 = project, etc.
);

CREATE INDEX IF NOT EXISTS idx_edits_time     ON edits (event_time DESC);
CREATE INDEX IF NOT EXISTS idx_edits_title    ON edits (title, event_time DESC);
CREATE INDEX IF NOT EXISTS idx_edits_wiki     ON edits (wiki, event_time DESC);
CREATE INDEX IF NOT EXISTS idx_edits_bot      ON edits (is_bot, event_time DESC);

-- Pre-aggregated stats refreshed every minute by Spark
CREATE TABLE IF NOT EXISTS edit_stats_1min (
    window_start    TIMESTAMPTZ PRIMARY KEY,
    total_edits     INTEGER NOT NULL DEFAULT 0,
    bot_edits       INTEGER NOT NULL DEFAULT 0,
    human_edits     INTEGER NOT NULL DEFAULT 0,
    new_pages       INTEGER NOT NULL DEFAULT 0,
    unique_editors  INTEGER NOT NULL DEFAULT 0,
    top_language    TEXT
);

-- Top articles per time window
CREATE TABLE IF NOT EXISTS top_articles (
    id              BIGSERIAL PRIMARY KEY,
    window_start    TIMESTAMPTZ NOT NULL,
    window_minutes  INTEGER NOT NULL DEFAULT 5,
    title           TEXT NOT NULL,
    wiki            TEXT NOT NULL,
    edit_count      INTEGER NOT NULL,
    is_spike        BOOLEAN NOT NULL DEFAULT FALSE  -- breaking news flag
);

CREATE INDEX IF NOT EXISTS idx_top_articles_window ON top_articles (window_start DESC, window_minutes);

-- Breaking news / anomaly spikes
CREATE TABLE IF NOT EXISTS spikes (
    id              BIGSERIAL PRIMARY KEY,
    detected_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    title           TEXT NOT NULL,
    wiki            TEXT NOT NULL,
    edits_in_window INTEGER NOT NULL,
    baseline_avg    FLOAT,
    spike_ratio     FLOAT,                 -- edits / baseline
    is_active       BOOLEAN NOT NULL DEFAULT TRUE
);
