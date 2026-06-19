"""
WikiPulse FastAPI Backend
Serves aggregated Wikipedia edit data to the dashboard.
"""
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta

import psycopg2
import psycopg2.extras
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

DB_URL = os.environ.get("DATABASE_URL", "postgresql://wiki:wiki123@localhost:5432/wikipulse")
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


@asynccontextmanager
async def lifespan(app: FastAPI):
    get_conn()
    print("[backend] DB connected")
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
