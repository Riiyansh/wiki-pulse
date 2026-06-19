"""
WikiPulse FastAPI Backend
Serves aggregated Wikipedia edit data to the dashboard.
When LITE_MODE=true, also ingests Wikipedia SSE directly (no Kafka/Spark needed).
"""
import os
import json
import threading
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

DB_URL = os.environ.get("DATABASE_URL", "postgresql://wiki:wiki123@localhost:5432/wikipulse")
LITE_MODE = os.environ.get("LITE_MODE", "false").lower() == "true"
_conn = None


def get_conn():
    global _conn
    if _conn and not _conn.closed:
        return _conn
    _conn = psycopg2.connect(DB_URL)
    _conn.autocommit = True
    return _conn


def query(sql: str, params=None) -> list[dict]:
    with get_conn().cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params)
        return [dict(r) for r in cur.fetchall()]


def _lite_ingestor():
    """Directly reads Wikipedia SSE and writes to Postgres. Runs in a background thread."""
    import requests, sseclient, time
    url = "https://stream.wikimedia.org/v2/stream/recentchange"
    article_counts: dict[tuple, int] = {}
    print("[lite] Starting Wikipedia SSE ingestor")

    # Own connection for this thread
    conn = psycopg2.connect(DB_URL)
    conn.autocommit = True

    count = 0
    while True:
        try:
            print("[lite] Connecting to Wikipedia SSE...")
            with requests.get(url, stream=True, timeout=60) as r:
                print(f"[lite] SSE connected, status={r.status_code}")
                client = sseclient.SSEClient(r)
                for event in client.events():
                    if not event.data:
                        continue
                    try:
                        d = json.loads(event.data)
                    except Exception:
                        continue
                    count += 1
                    if count % 100 == 0:
                        print(f"[lite] Processed {count} events total")
                    if d.get("namespace") != 0 or d.get("type") not in ("edit", "new"):
                        continue
                    title = d.get("title", "")
                    wiki = d.get("wiki", "")
                    lang = wiki[:-4] if wiki.endswith("wiki") else wiki
                    user = d.get("user", "")
                    is_bot = bool(d.get("bot", False))
                    is_new = d.get("type") == "new"
                    length = d.get("length") or {}
                    delta = (length.get("new") or 0) - (length.get("old") or 0)
                    comment = (d.get("comment") or "")[:500]
                    now = datetime.now(timezone.utc)
                    window = now.replace(second=0, microsecond=0)

                    try:
                        with conn.cursor() as cur:
                            cur.execute("""
                                INSERT INTO edits (event_time, title, wiki, language, user_name,
                                                   is_bot, is_new_page, delta_bytes, comment)
                                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                            """, (now, title, wiki, lang, user, is_bot, is_new, delta, comment))

                            cur.execute("""
                                INSERT INTO edit_stats_1min
                                    (window_start, total_edits, bot_edits, human_edits, new_pages, unique_editors)
                                VALUES (%s,1,%s,%s,%s,1)
                                ON CONFLICT (window_start) DO UPDATE SET
                                    total_edits    = edit_stats_1min.total_edits + 1,
                                    bot_edits      = edit_stats_1min.bot_edits + EXCLUDED.bot_edits,
                                    human_edits    = edit_stats_1min.human_edits + EXCLUDED.human_edits,
                                    new_pages      = edit_stats_1min.new_pages + EXCLUDED.new_pages,
                                    unique_editors = edit_stats_1min.unique_editors + 1
                            """, (window, 1 if is_bot else 0, 0 if is_bot else 1, 1 if is_new else 0))

                            key = (title, wiki, window)
                            article_counts[key] = article_counts.get(key, 0) + 1
                            cnt = article_counts[key]
                            is_spike = cnt >= 5
                            cur.execute("""
                                INSERT INTO top_articles (window_start, title, wiki, edit_count, is_spike)
                                VALUES (%s,%s,%s,%s,%s)
                                ON CONFLICT (window_start, title, wiki) DO UPDATE SET
                                    edit_count = EXCLUDED.edit_count,
                                    is_spike   = EXCLUDED.is_spike
                            """, (window, title, wiki, cnt, is_spike))

                            if is_spike:
                                cur.execute("""
                                    INSERT INTO spikes (detected_at, title, wiki, edits_in_window, spike_ratio, is_active)
                                    VALUES (%s,%s,%s,%s,1.0,TRUE)
                                    ON CONFLICT (detected_at, title, wiki) DO NOTHING
                                """, (now, title, wiki, cnt))
                    except Exception as db_err:
                        print(f"[lite] DB error: {db_err}")
                        try:
                            conn = psycopg2.connect(DB_URL)
                            conn.autocommit = True
                        except Exception:
                            pass

        except Exception as e:
            print(f"[lite] SSE error: {e}, reconnecting in 5s")
            time.sleep(5)


def init_db():
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS edits (
                id          BIGSERIAL PRIMARY KEY,
                event_time  TIMESTAMPTZ NOT NULL,
                title       TEXT NOT NULL,
                wiki        TEXT NOT NULL,
                language    TEXT NOT NULL,
                user_name   TEXT,
                is_bot      BOOLEAN NOT NULL DEFAULT FALSE,
                is_new_page BOOLEAN NOT NULL DEFAULT FALSE,
                delta_bytes INTEGER,
                comment     TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_edits_time ON edits (event_time DESC);

            CREATE TABLE IF NOT EXISTS edit_stats_1min (
                window_start  TIMESTAMPTZ PRIMARY KEY,
                total_edits   INTEGER NOT NULL DEFAULT 0,
                bot_edits     INTEGER NOT NULL DEFAULT 0,
                human_edits   INTEGER NOT NULL DEFAULT 0,
                new_pages     INTEGER NOT NULL DEFAULT 0,
                unique_editors INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS top_articles (
                window_start TIMESTAMPTZ NOT NULL,
                title        TEXT NOT NULL,
                wiki         TEXT NOT NULL,
                edit_count   INTEGER NOT NULL DEFAULT 0,
                is_spike     BOOLEAN NOT NULL DEFAULT FALSE,
                PRIMARY KEY (window_start, title, wiki)
            );
            CREATE INDEX IF NOT EXISTS idx_top_articles_window ON top_articles (window_start DESC);

            CREATE TABLE IF NOT EXISTS spikes (
                id              BIGSERIAL PRIMARY KEY,
                detected_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                title           TEXT NOT NULL,
                wiki            TEXT NOT NULL,
                edits_in_window INTEGER NOT NULL,
                spike_ratio     FLOAT,
                is_active       BOOLEAN NOT NULL DEFAULT TRUE,
                UNIQUE (detected_at, title, wiki)
            );
        """)
    print("[backend] Schema ready")


@asynccontextmanager
async def lifespan(app: FastAPI):
    get_conn()
    init_db()
    print("[backend] DB connected")
    if LITE_MODE:
        t = threading.Thread(target=_lite_ingestor, daemon=True)
        t.start()
        print("[backend] Lite ingestor started")
    yield
    if _conn and not _conn.closed:
        _conn.close()


app = FastAPI(title="WikiPulse API", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


@app.get("/api/health")
def health():
    rows = query("SELECT COUNT(*) as cnt FROM edits")
    return {"status": "ok", "total_edits_stored": rows[0]["cnt"]}


@app.get("/api/live-feed")
def live_feed(limit: int = 50):
    """Latest edits as they come in."""
    rows = query("""
        SELECT event_time, title, wiki, language, user_name,
               is_bot, is_new_page, delta_bytes, comment
        FROM edits
        ORDER BY event_time DESC
        LIMIT %s
    """, (limit,))
    for r in rows:
        if r["event_time"]:
            r["event_time"] = r["event_time"].isoformat()
    return {"edits": rows}


@app.get("/api/stats")
def stats(minutes: int = 30):
    """Edits per minute for the last N minutes — for the time-series chart."""
    rows = query("""
        SELECT window_start, total_edits, bot_edits, human_edits, new_pages, unique_editors
        FROM edit_stats_1min
        WHERE window_start >= NOW() - (%s || ' minutes')::interval
        ORDER BY window_start ASC
    """, (str(minutes),))
    for r in rows:
        r["window_start"] = r["window_start"].isoformat()
    return {"stats": rows}


@app.get("/api/top-articles")
def top_articles(minutes: int = 15, limit: int = 20):
    """Most edited articles in the last N minutes."""
    rows = query("""
        SELECT title, wiki, SUM(edit_count) as total_edits,
               BOOL_OR(is_spike) as is_spike
        FROM top_articles
        WHERE window_start >= NOW() - (%s || ' minutes')::interval
        GROUP BY title, wiki
        ORDER BY total_edits DESC
        LIMIT %s
    """, (str(minutes), limit))
    return {"articles": rows, "window_minutes": minutes}


@app.get("/api/bot-vs-human")
def bot_vs_human(minutes: int = 30):
    """Bot vs human edit breakdown."""
    rows = query("""
        SELECT
            SUM(bot_edits)   AS bot,
            SUM(human_edits) AS human,
            SUM(new_pages)   AS new_pages,
            SUM(total_edits) AS total
        FROM edit_stats_1min
        WHERE window_start >= NOW() - (%s || ' minutes')::interval
    """, (str(minutes),))
    return rows[0] if rows else {"bot": 0, "human": 0, "new_pages": 0, "total": 0}


@app.get("/api/spikes")
def spikes(limit: int = 10):
    """Breaking news / anomaly spike alerts."""
    rows = query("""
        SELECT detected_at, title, wiki, edits_in_window, spike_ratio, is_active
        FROM spikes
        WHERE is_active = TRUE
        ORDER BY detected_at DESC
        LIMIT %s
    """, (limit,))
    for r in rows:
        r["detected_at"] = r["detected_at"].isoformat()
    return {"spikes": rows}


@app.get("/api/languages")
def languages(minutes: int = 30):
    """Edit breakdown by language/wiki."""
    rows = query("""
        SELECT language, COUNT(*) as edit_count
        FROM edits
        WHERE event_time >= NOW() - (%s || ' minutes')::interval
        GROUP BY language
        ORDER BY edit_count DESC
        LIMIT 15
    """, (str(minutes),))
    return {"languages": rows}
