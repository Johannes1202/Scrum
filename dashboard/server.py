"""
server.py — Scrum: rugby prediction league dashboard.
"""

import asyncio
import base64
import hashlib
import hmac
import json
import logging
import os
import re
import secrets
import time
from datetime import datetime, timedelta, timezone

import aiosqlite
import httpx
import redis.asyncio as aioredis
import uvicorn
from fastapi import FastAPI, File, Form, Request, Response, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from passlib.context import CryptContext

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(name)s: %(message)s")
logger = logging.getLogger("dashboard")

# ── Config ────────────────────────────────────────────────────────────────────
REDIS_URL       = os.environ.get("REDIS_URL", "redis://redis:6379")
SERVER_PORT     = int(os.environ.get("SERVER_PORT", "8888"))
SESSION_SECRET  = os.environ.get("SESSION_SECRET", secrets.token_hex(32))
SESSION_COOKIE   = "sps_session"
SESSION_MAX_AGE  = 60 * 60 * 24 * 30
MAGIC_EXTRA_AGE  = 60 * 60 * 24 * 365 * 5 - SESSION_MAX_AGE  # token ts offset for ~5yr sessions
DB_PATH          = os.environ.get("DB_PATH", "/data/scrum.db")
ADMIN_USERNAME  = os.environ.get("ADMIN_USERNAME", "admin")
ADMIN_PASSWORD  = os.environ.get("ADMIN_PASSWORD", os.environ.get("STREAM_PASSWORD", "changeme"))

pwd_ctx = CryptContext(schemes=["bcrypt"], deprecated="auto")

AVATAR_DIR = "/data/avatars"
os.makedirs(AVATAR_DIR, exist_ok=True)

def _avatar_url(username: str) -> str | None:
    path = os.path.join(AVATAR_DIR, f"{username}.jpg")
    return f"/avatar/{username}" if os.path.exists(path) else None

# Single source of truth for all supported leagues: slug → (espn_id, display_name)
ALL_ESPN_LEAGUES = {
    "nations-championship": (17567, "Nations Championship"),
    "six-nations":        (180659, "Six Nations"),
    "rugby-championship": (244293, "Rugby Championship"),
    "super-rugby":        (242041, "Super Rugby Pacific"),
    "urc":                (270557, "URC"),
    "world-cup":          (164205, "Rugby World Cup"),
    "international":      (289234, "International"),
    "premiership":        (267979, "Gallagher Premiership"),
    "top-14":             (270559, "French Top 14"),
    "champions-cup":      (271937, "European Champions Cup"),
    "challenge-cup":      (272073, "European Challenge Cup"),
}

TOURNAMENTS  = {k: v[1] for k, v in ALL_ESPN_LEAGUES.items()}
ESPN_LEAGUES = {k: v[0] for k, v in ALL_ESPN_LEAGUES.items()}

# Standings page tabs
STANDINGS_TABS = [
    ("nations-championship", "Nations Championship", 17567),
    ("world-cup",          "World Cup",          164205),
    ("six-nations",        "Six Nations",         180659),
    ("rugby-championship", "Rugby Championship",  244293),
    ("super-rugby",        "Super Rugby Pacific", 242041),
    ("urc",                "URC",                 270557),
    ("premiership",        "Premiership",         267979),
    ("top-14",             "Top 14",              270559),
    ("champions-cup",      "Champions Cup",       271937),
    ("challenge-cup",      "Challenge Cup",       272073),
    ("world-rankings",     "World Rankings",      None),
]

# Team name aliases (lowercase) for fuzzy matching
_TEAM_ALIASES = {
    "all blacks": "new zealand",
    "springboks": "south africa",
    "wallabies":  "australia",
    "pumas":      "argentina",
    "los pumas":  "argentina",
}

_last_auto_fetch: dict = {"ts": 0, "applied": 0, "checked": 0}


# ── App ───────────────────────────────────────────────────────────────────────
app = FastAPI(title="Scrum")
app.mount("/static", StaticFiles(directory="static"), name="static")

_redis: aioredis.Redis = None
_http_client: httpx.AsyncClient = None
_db: aiosqlite.Connection = None

# ── Database ──────────────────────────────────────────────────────────────────
_SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    username      TEXT    UNIQUE NOT NULL,
    password_hash TEXT    NOT NULL,
    is_admin      INTEGER NOT NULL DEFAULT 0,
    created_at    REAL    NOT NULL,
    chat_color    TEXT
);
CREATE TABLE IF NOT EXISTS invite_tokens (
    token      TEXT PRIMARY KEY,
    username   TEXT NOT NULL,
    created_at REAL NOT NULL,
    used_at    REAL
);
CREATE TABLE IF NOT EXISTS predictions (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    match_id       TEXT NOT NULL,
    match_title    TEXT NOT NULL,
    team_home      TEXT NOT NULL,
    team_away      TEXT NOT NULL,
    kickoff_ts     REAL NOT NULL DEFAULT 0,
    tournament     TEXT,
    username       TEXT NOT NULL,
    score_home     INTEGER NOT NULL,
    score_away     INTEGER NOT NULL,
    pred_winner    TEXT,
    pred_margin    TEXT,
    pred_try_any   TEXT,
    pred_try_first TEXT,
    pred_btts      INTEGER,
    pred_motm      TEXT,
    created_at     REAL NOT NULL,
    UNIQUE(match_id, username)
);
CREATE TABLE IF NOT EXISTS match_results (
    match_id     TEXT PRIMARY KEY,
    match_title  TEXT NOT NULL,
    team_home    TEXT NOT NULL,
    team_away    TEXT NOT NULL,
    tournament   TEXT NOT NULL,
    kickoff_ts   REAL NOT NULL DEFAULT 0,
    final_home   INTEGER NOT NULL,
    final_away   INTEGER NOT NULL,
    entered_by   TEXT NOT NULL,
    entered_at   REAL NOT NULL,
    res_winner        TEXT,
    res_margin        TEXT,
    res_btts          INTEGER,
    res_motm          TEXT,
    res_try_scorers   TEXT,
    res_first_try     TEXT,
    motm_pending      INTEGER NOT NULL DEFAULT 0,
    motm_scrape_debug TEXT
);
CREATE TABLE IF NOT EXISTS leaderboard (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    group_id     INTEGER NOT NULL DEFAULT 1,
    match_id     TEXT NOT NULL,
    tournament   TEXT NOT NULL,
    username     TEXT NOT NULL,
    diff         INTEGER NOT NULL,
    exact_score  INTEGER NOT NULL DEFAULT 0,
    points       INTEGER NOT NULL,
    pts_score    INTEGER NOT NULL DEFAULT 0,
    pts_winner   INTEGER NOT NULL DEFAULT 0,
    pts_margin   INTEGER NOT NULL DEFAULT 0,
    pts_btts     INTEGER NOT NULL DEFAULT 0,
    pts_try_any  INTEGER NOT NULL DEFAULT 0,
    pts_try_first INTEGER NOT NULL DEFAULT 0,
    pts_motm     INTEGER NOT NULL DEFAULT 0,
    pts_banker   INTEGER NOT NULL DEFAULT 0,
    created_at   REAL NOT NULL,
    UNIQUE(group_id, match_id, username)
);
CREATE TABLE IF NOT EXISTS groups (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT NOT NULL,
    slug        TEXT UNIQUE NOT NULL,
    created_by  TEXT NOT NULL,
    created_at  REAL NOT NULL,
    description TEXT
);
CREATE TABLE IF NOT EXISTS group_members (
    group_id  INTEGER NOT NULL REFERENCES groups(id),
    username  TEXT NOT NULL,
    role      TEXT NOT NULL DEFAULT 'member',
    joined_at REAL NOT NULL,
    PRIMARY KEY (group_id, username)
);
CREATE TABLE IF NOT EXISTS group_leagues (
    group_id    INTEGER NOT NULL REFERENCES groups(id),
    league_slug TEXT NOT NULL,
    PRIMARY KEY (group_id, league_slug)
);
CREATE TABLE IF NOT EXISTS group_prediction_types (
    group_id        INTEGER NOT NULL REFERENCES groups(id),
    prediction_type TEXT NOT NULL,
    PRIMARY KEY (group_id, prediction_type)
);
CREATE TABLE IF NOT EXISTS group_invites (
    token           TEXT PRIMARY KEY,
    group_id        INTEGER NOT NULL REFERENCES groups(id),
    created_by      TEXT NOT NULL,
    created_at      REAL NOT NULL,
    invited_username TEXT,
    used_by         TEXT,
    used_at         REAL,
    max_uses        INTEGER NOT NULL DEFAULT 1,
    use_count       INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS players (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    name            TEXT NOT NULL,
    team_name       TEXT NOT NULL,
    jersey          INTEGER,
    position        TEXT,
    last_seen_ts    REAL NOT NULL DEFAULT 0,
    last_seen_match TEXT,
    UNIQUE(name, team_name)
);
CREATE TABLE IF NOT EXISTS friends (
    username        TEXT NOT NULL,
    friend_username TEXT NOT NULL,
    created_at      REAL NOT NULL,
    PRIMARY KEY (username, friend_username)
);
CREATE TABLE IF NOT EXISTS seasons (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    group_id    INTEGER NOT NULL REFERENCES groups(id),
    name        TEXT NOT NULL,
    archived_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS season_leaderboard (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    season_id   INTEGER NOT NULL REFERENCES seasons(id),
    username    TEXT NOT NULL,
    total_points INTEGER NOT NULL DEFAULT 0,
    matches     INTEGER NOT NULL DEFAULT 0,
    exact_scores INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS custom_competitions (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    group_id   INTEGER NOT NULL REFERENCES groups(id),
    name       TEXT NOT NULL,
    slug       TEXT NOT NULL,
    created_at REAL NOT NULL,
    UNIQUE(group_id, slug)
);
CREATE TABLE IF NOT EXISTS custom_competition_matches (
    comp_id    INTEGER NOT NULL REFERENCES custom_competitions(id),
    match_id   TEXT NOT NULL,
    espn_id    TEXT,
    league_id  INTEGER,
    team_home  TEXT NOT NULL,
    team_away  TEXT NOT NULL,
    kickoff_ts REAL NOT NULL,
    tournament TEXT NOT NULL,
    PRIMARY KEY (comp_id, match_id)
);
"""


async def _init_db():
    global _db
    data_dir = os.path.dirname(DB_PATH)
    if data_dir:
        os.makedirs(data_dir, exist_ok=True)
    _db = await aiosqlite.connect(DB_PATH)
    _db.row_factory = aiosqlite.Row
    await _db.executescript(_SCHEMA)
    await _db.commit()
    admin = ADMIN_USERNAME.lower()
    new_hash = pwd_ctx.hash(ADMIN_PASSWORD)
    async with _db.execute("SELECT id FROM users WHERE username=?", (admin,)) as cur:
        row = await cur.fetchone()
    if row:
        # Admin account exists under expected name — sync password
        await _db.execute("UPDATE users SET password_hash=? WHERE username=?", (new_hash, admin))
    else:
        # Check if any admin user exists (e.g. admin renamed themselves)
        async with _db.execute("SELECT id FROM users WHERE is_admin=1 LIMIT 1") as cur:
            any_admin = await cur.fetchone()
        if not any_admin:
            # First run — create the admin account
            await _db.execute(
                "INSERT INTO users (username,password_hash,is_admin,created_at) VALUES(?,?,1,?)",
                (admin, new_hash, time.time()),
            )
        else:
            logger.info("Admin user renamed — skipping creation of '%s'", admin)
    await _db.commit()

    # Migrations — safe to run on every startup
    for col in ("tournament TEXT", "pred_winner TEXT", "pred_margin TEXT", "pred_try_any TEXT",
                "pred_try_first TEXT", "pred_btts INTEGER", "pred_motm TEXT", "is_banker INTEGER DEFAULT 0"):
        try:
            await _db.execute(f"ALTER TABLE predictions ADD COLUMN {col}")
            await _db.commit()
        except Exception:
            pass
    for col in ("res_winner TEXT", "res_margin TEXT", "res_btts INTEGER",
                "res_motm TEXT", "res_try_scorers TEXT", "res_first_try TEXT",
                "motm_pending INTEGER NOT NULL DEFAULT 0",
                "motm_scrape_debug TEXT"):
        try:
            await _db.execute(f"ALTER TABLE match_results ADD COLUMN {col}")
            await _db.commit()
        except Exception:
            pass
    try:
        await _db.execute("ALTER TABLE leaderboard ADD COLUMN group_id INTEGER NOT NULL DEFAULT 1")
        await _db.commit()
    except Exception:
        pass
    for col in ("pts_score INTEGER NOT NULL DEFAULT 0", "pts_winner INTEGER NOT NULL DEFAULT 0",
                "pts_margin INTEGER NOT NULL DEFAULT 0", "pts_btts INTEGER NOT NULL DEFAULT 0",
                "pts_try_any INTEGER NOT NULL DEFAULT 0", "pts_try_first INTEGER NOT NULL DEFAULT 0",
                "pts_motm INTEGER NOT NULL DEFAULT 0", "pts_banker INTEGER NOT NULL DEFAULT 0"):
        try:
            await _db.execute(f"ALTER TABLE leaderboard ADD COLUMN {col}")
            await _db.commit()
        except Exception:
            pass
    for col in ("invited_username TEXT", "max_uses INTEGER NOT NULL DEFAULT 1",
                "use_count INTEGER NOT NULL DEFAULT 0", "expires_at REAL"):
        try:
            await _db.execute(f"ALTER TABLE group_invites ADD COLUMN {col}")
            await _db.commit()
        except Exception:
            pass

    for ddl in [
        "CREATE TABLE IF NOT EXISTS custom_competitions (id INTEGER PRIMARY KEY AUTOINCREMENT, group_id INTEGER NOT NULL REFERENCES groups(id), name TEXT NOT NULL, slug TEXT NOT NULL, created_at REAL NOT NULL, UNIQUE(group_id, slug))",
        "CREATE TABLE IF NOT EXISTS custom_competition_matches (comp_id INTEGER NOT NULL REFERENCES custom_competitions(id), match_id TEXT NOT NULL, espn_id TEXT, league_id INTEGER, team_home TEXT NOT NULL, team_away TEXT NOT NULL, kickoff_ts REAL NOT NULL, tournament TEXT NOT NULL, PRIMARY KEY (comp_id, match_id))",
    ]:
        try:
            await _db.execute(ddl)
            await _db.commit()
        except Exception:
            pass

    # Rebuild leaderboard if UNIQUE constraint is missing group_id (old schema)
    async with _db.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='leaderboard'"
    ) as cur:
        lb_schema_row = await cur.fetchone()
    if lb_schema_row and "group_id, match_id, username" not in (lb_schema_row[0] or ""):
        logger.info("Rebuilding leaderboard table — fixing UNIQUE constraint to include group_id")
        await _db.execute("""CREATE TABLE leaderboard_rebuild (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            group_id INTEGER NOT NULL DEFAULT 1,
            match_id TEXT NOT NULL,
            tournament TEXT NOT NULL,
            username TEXT NOT NULL,
            diff INTEGER NOT NULL,
            exact_score INTEGER NOT NULL DEFAULT 0,
            points INTEGER NOT NULL,
            pts_score INTEGER NOT NULL DEFAULT 0,
            pts_winner INTEGER NOT NULL DEFAULT 0,
            pts_margin INTEGER NOT NULL DEFAULT 0,
            pts_btts INTEGER NOT NULL DEFAULT 0,
            pts_try_any INTEGER NOT NULL DEFAULT 0,
            pts_try_first INTEGER NOT NULL DEFAULT 0,
            pts_motm INTEGER NOT NULL DEFAULT 0,
            pts_banker INTEGER NOT NULL DEFAULT 0,
            created_at REAL NOT NULL,
            UNIQUE(group_id, match_id, username)
        )""")
        await _db.execute("DROP TABLE leaderboard")
        await _db.execute("ALTER TABLE leaderboard_rebuild RENAME TO leaderboard")
        await _db.commit()

    # Ensure the Global group (id=1) exists
    async with _db.execute("SELECT id FROM groups WHERE id=1") as cur:
        if not await cur.fetchone():
            await _db.execute(
                "INSERT INTO groups (id,name,slug,created_by,created_at,description) VALUES(1,?,?,?,?,?)",
                ("Global", "global", admin, time.time(), "All users — the original leaderboard"),
            )
            await _db.commit()

    # Add all existing users to Global group if not already members
    async with _db.execute("SELECT username FROM users") as cur:
        all_users = [r["username"] for r in await cur.fetchall()]
    for u in all_users:
        try:
            role = "admin" if u == admin else "member"
            await _db.execute(
                "INSERT OR IGNORE INTO group_members (group_id,username,role,joined_at) VALUES(1,?,?,?)",
                (u, role, time.time()),
            )
        except Exception:
            pass
    await _db.commit()

    # Global group: follow all leagues by default
    for slug in ESPN_LEAGUES:
        try:
            await _db.execute(
                "INSERT OR IGNORE INTO group_leagues (group_id,league_slug) VALUES(1,?)", (slug,)
            )
        except Exception:
            pass
    # Global group: enable all prediction types by default
    for pt in ("score", "winner", "margin", "try_anytime", "try_first", "btts"):
        try:
            await _db.execute(
                "INSERT OR IGNORE INTO group_prediction_types (group_id,prediction_type) VALUES(1,?)", (pt,)
            )
        except Exception:
            pass
    await _db.commit()

    logger.info("Admin credentials synced: %s", admin)



# ── Startup / Shutdown ────────────────────────────────────────────────────────
@app.on_event("startup")
async def startup():
    global _redis, _http_client
    _redis = aioredis.from_url(REDIS_URL, decode_responses=True)
    _http_client = httpx.AsyncClient(
        timeout=15,
        limits=httpx.Limits(max_connections=100, max_keepalive_connections=40),
        follow_redirects=True,
    )
    await _init_db()
    asyncio.create_task(_auto_fetch_loop())
    logger.info("Dashboard started.")


@app.on_event("shutdown")
async def shutdown():
    if _redis:        await _redis.aclose()
    if _http_client: await _http_client.aclose()
    if _db:          await _db.close()


# ── ESPN result fetching ──────────────────────────────────────────────────────

def _norm_name(name: str) -> str:
    n = name.lower().strip()
    return _TEAM_ALIASES.get(n, n)


def _names_match(a: str, b: str) -> bool:
    a, b = _norm_name(a), _norm_name(b)
    if a == b:
        return True
    if a in b or b in a:
        return True
    wa = set(a.split()) - {"the", "of", "and", "&", "rugby", "union"}
    wb = set(b.split()) - {"the", "of", "and", "&", "rugby", "union"}
    return bool(wa and wb and wa & wb)


async def _fetch_espn_results(days_back: int = 45) -> list[dict]:
    """Fetch completed results from ESPN for all known leagues."""
    results = []
    end = datetime.now(timezone.utc) + timedelta(days=1)  # +1: ESPN end date is exclusive
    start = end - timedelta(days=days_back + 1)
    date_range = f"{start.strftime('%Y%m%d')}-{end.strftime('%Y%m%d')}"
    for slug, (league_id, _) in ALL_ESPN_LEAGUES.items():
        url = (f"https://site.api.espn.com/apis/site/v2/sports/rugby"
               f"/{league_id}/scoreboard?limit=100&dates={date_range}")
        try:
            resp = await _http_client.get(url, timeout=12)
            if resp.status_code != 200:
                logger.warning("ESPN %s returned %s", slug, resp.status_code)
                continue
            for event in resp.json().get("events", []):
                if not event.get("status", {}).get("type", {}).get("completed"):
                    continue
                comp = event.get("competitions", [{}])[0]
                teams = comp.get("competitors", [])
                if len(teams) != 2:
                    continue
                home = next((t for t in teams if t.get("homeAway") == "home"), teams[0])
                away = next((t for t in teams if t.get("homeAway") == "away"), teams[1])
                try:
                    score_home = int(home.get("score", 0))
                    score_away = int(away.get("score", 0))
                except (ValueError, TypeError):
                    continue
                results.append({
                    "tournament":  slug,
                    "league_id":   league_id,
                    "espn_id":     event.get("id", ""),
                    "team_home":   home["team"]["displayName"],
                    "team_away":   away["team"]["displayName"],
                    "score_home":  score_home,
                    "score_away":  score_away,
                    "event_date":  event.get("date", "")[:10],
                    "event_ts":    _iso_to_ts(event.get("date", "")),
                    "title":       event.get("name", ""),
                })
        except Exception as exc:
            logger.warning("ESPN fetch error for %s: %s", slug, exc)
    return results


def _iso_to_ts(iso: str) -> float:
    try:
        return datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp()
    except Exception:
        return 0.0


def _calc_margin_band(diff: int) -> str:
    if diff <= 7:   return "1-7"
    if diff <= 14:  return "8-14"
    if diff <= 21:  return "15-21"
    return "22+"


async def _score_group(group_id: int, match_id: str, tournament: str,
                       fh: int, fa: int, res_winner: str, res_margin: str, res_btts: int,
                       res_try_scorers: list | None = None,
                       res_first_try: str | None = None) -> None:
    """Calculate and upsert leaderboard points for all group members who predicted this match."""
    async with _db.execute(
        "SELECT league_slug FROM group_leagues WHERE group_id=?", (group_id,)
    ) as cur:
        group_leagues = {r["league_slug"] for r in await cur.fetchall()}
    if tournament not in group_leagues:
        return

    async with _db.execute(
        "SELECT prediction_type FROM group_prediction_types WHERE group_id=?", (group_id,)
    ) as cur:
        pred_types = {r["prediction_type"] for r in await cur.fetchall()}

    async with _db.execute(
        """SELECT gm.username, p.score_home, p.score_away,
                  p.pred_winner, p.pred_margin, p.pred_btts,
                  p.pred_try_any, p.pred_try_first,
                  p.is_banker
           FROM group_members gm
           JOIN predictions p ON p.username=gm.username AND p.match_id=?
           WHERE gm.group_id=?""",
        (match_id, group_id),
    ) as cur:
        preds = [dict(r) for r in await cur.fetchall()]

    if not preds:
        return

    try_set = set(_scorer_names(res_try_scorers or []))

    for p in preds:
        p["diff"] = abs(p["score_home"] - fh) + abs(p["score_away"] - fa)

    min_diff = min(p["diff"] for p in preds)

    for p in preds:
        ps = pw = pm = pb = pta = ptf = pmo = 0
        if "score" in pred_types:
            ps = 5 if p["diff"] == 0 else (3 if p["diff"] == min_diff else 1)
        if "winner" in pred_types and p["pred_winner"] and p["pred_winner"] == res_winner:
            pw = 2
        if "margin" in pred_types and p["pred_margin"] and p["pred_margin"] == res_margin:
            pm = 2
        if "btts" in pred_types and p["pred_btts"] is not None:
            try:
                if int(p["pred_btts"]) == res_btts:
                    pb = 1
            except (ValueError, TypeError):
                pass
        if "try_anytime" in pred_types and p["pred_try_any"] and try_set:
            if p["pred_try_any"] in try_set:
                pta = 3
        if "try_first" in pred_types and p["pred_try_first"] and res_first_try:
            if p["pred_try_first"] == res_first_try:
                ptf = 4

        pts = ps + pw + pm + pb + pta + ptf + pmo
        banker_bonus = 0
        if int(p.get("is_banker") or 0) == 1 and pts > 0:
            banker_bonus = pts
            pts *= 2

        exact = 1 if p["diff"] == 0 else 0
        await _db.execute(
            """INSERT OR REPLACE INTO leaderboard
               (group_id,match_id,tournament,username,diff,exact_score,points,
                pts_score,pts_winner,pts_margin,pts_btts,pts_try_any,pts_try_first,pts_motm,pts_banker,
                created_at)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (group_id, match_id, tournament, p["username"],
             p["diff"], exact, pts,
             ps, pw, pm, pb, pta, ptf, pmo, banker_bonus,
             time.time()),
        )
    await _db.commit()


def _scorer_names(scorers: list) -> list[str]:
    """Extract flat name list from either old format (list[str]) or new format (list[dict])."""
    return [s["name"] if isinstance(s, dict) else s for s in (scorers or [])]


async def _fetch_try_scorers(espn_id: str, league_id: int) -> tuple[list[dict], str | None]:
    """Fetch ESPN match summary and extract try scorers.
    Returns (list of {name, team, clock}, first_scorer_name)."""
    url = (f"https://site.api.espn.com/apis/site/v2/sports/rugby"
           f"/{league_id}/summary?event={espn_id}")
    try:
        resp = await _http_client.get(url, timeout=15)
        logger.info("Try scorer fetch %s: HTTP %s", espn_id, resp.status_code)
        if resp.status_code != 200:
            logger.warning("Try scorer fetch failed for %s: HTTP %s", espn_id, resp.status_code)
            return [], None
        data = resp.json()
        details = (data.get("header", {})
                       .get("competitions", [{}])[0]
                       .get("details", []))
        logger.info("Try scorer details for %s: %d events total", espn_id, len(details))
        all_types = list({e.get("type", {}).get("text", "") for e in details})
        logger.info("Try scorer event types for %s: %s", espn_id, all_types)
        scorers = []
        first = None
        # Sort by clock value so first try is chronologically first
        try_events = sorted(
            [e for e in details
             if "try" in e.get("type", {}).get("text", "").lower()
             and "conversion" not in e.get("type", {}).get("text", "").lower()
             and "penalty" not in e.get("type", {}).get("text", "").lower()],
            key=lambda e: e.get("clock", {}).get("value", 9999)
        )
        for event in try_events:
            name = None
            for field in ("participants", "athletes", "athletesInvolved"):
                people = event.get(field, [])
                if people:
                    name = (people[0].get("athlete", {}).get("displayName")
                            or people[0].get("displayName", ""))
                    if name:
                        break
            if name:
                team = event.get("team", {}).get("displayName", "")
                clock = event.get("clock", {}).get("displayValue", "")
                scorers.append({"name": name, "team": team, "clock": clock})
                if first is None:
                    first = name
        logger.info("Try scorers for %s: %s (first: %s)", espn_id, scorers, first)
        if not scorers:
            logger.warning("No try scorers found for %s — details had %d events, types: %s", espn_id, len(details), all_types)
        return scorers, first
    except Exception as exc:
        logger.warning("Try scorer fetch error for %s: %s", espn_id, exc)
        return [], None


async def _auto_apply_results() -> tuple[int, int]:
    """Match ESPN results to pending predictions and apply. Returns (checked, applied)."""
    espn = await _fetch_espn_results()
    if not espn:
        return 0, 0

    # Pending matches: have predictions, no result, kickoff started >90 min ago
    cutoff = time.time() - 5400
    async with _db.execute(
        """SELECT p.match_id, p.match_title, p.team_home, p.team_away, p.kickoff_ts, p.tournament
           FROM predictions p
           LEFT JOIN match_results r ON p.match_id = r.match_id
           WHERE r.match_id IS NULL
             AND (p.kickoff_ts = 0 OR p.kickoff_ts < ?)
           GROUP BY p.match_id""",
        (cutoff,),
    ) as cur:
        pending = [dict(r) for r in await cur.fetchall()]

    applied = 0
    for pred in pending:
        for espn_match in espn:
            if not (_names_match(pred["team_home"], espn_match["team_home"]) and
                    _names_match(pred["team_away"], espn_match["team_away"])):
                if not (_names_match(pred["team_home"], espn_match["team_away"]) and
                        _names_match(pred["team_away"], espn_match["team_home"])):
                    continue
                fh, fa = espn_match["score_away"], espn_match["score_home"]
            else:
                fh, fa = espn_match["score_home"], espn_match["score_away"]

            if pred["kickoff_ts"] and espn_match["event_ts"]:
                if abs(pred["kickoff_ts"] - espn_match["event_ts"]) > 129600:
                    continue

            try:
                winner = "home" if fh > fa else ("away" if fa > fh else "draw")
                margin = _calc_margin_band(abs(fh - fa))
                btts = 1 if fh > 0 and fa > 0 else 0
                tourn = espn_match["tournament"]

                await _db.execute(
                    """INSERT OR IGNORE INTO match_results
                       (match_id,match_title,team_home,team_away,tournament,kickoff_ts,
                        final_home,final_away,entered_by,entered_at,
                        res_winner,res_margin,res_btts)
                       VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (pred["match_id"], pred["match_title"],
                     pred["team_home"], pred["team_away"],
                     tourn, pred["kickoff_ts"],
                     fh, fa, "auto-fetch", time.time(),
                     winner, margin, btts),
                )
                await _db.commit()

                # Score per group
                async with _db.execute("SELECT id FROM groups") as cur2:
                    group_ids = [r["id"] for r in await cur2.fetchall()]
                for gid in group_ids:
                    try:
                        await _score_group(gid, pred["match_id"], tourn, fh, fa, winner, margin, btts)
                    except Exception as exc:
                        logger.warning("Score group %d error for %s: %s", gid, pred["match_id"], exc)

                applied += 1
                logger.info("Auto-applied: %s %d-%d %s", espn_match["team_home"], fh, fa, espn_match["team_away"])
                if espn_match.get("espn_id") and espn_match.get("league_id"):
                    asyncio.create_task(_harvest_players(espn_match["espn_id"], espn_match["league_id"]))
                    asyncio.create_task(_resolve_and_rescore(
                        pred["match_id"], tourn,
                        espn_match["espn_id"], espn_match["league_id"],
                        pred["team_home"], pred["team_away"],
                        espn_match["event_ts"],
                        fh, fa, winner, margin, btts,
                    ))
            except Exception as exc:
                logger.warning("Auto-apply error for %s: %s", pred["match_id"], exc)
            break

    # Store ALL recent ESPN results as match records (not just prediction-linked ones)
    # This powers the companion Results page even for leagues nobody predicted on
    for espn_match in espn:
        if not espn_match.get("espn_id"):
            continue
        slug = espn_match["espn_id"]
        async with _db.execute(
            "SELECT match_id FROM match_results WHERE match_id=?", (slug,)
        ) as cur:
            if await cur.fetchone():
                continue  # already have it
        fh, fa = espn_match["score_home"], espn_match["score_away"]
        winner = "home" if fh > fa else ("away" if fa > fh else "draw")
        margin = _calc_margin_band(abs(fh - fa))
        btts = 1 if fh > 0 and fa > 0 else 0
        try:
            await _db.execute(
                """INSERT OR IGNORE INTO match_results
                   (match_id,match_title,team_home,team_away,tournament,kickoff_ts,
                    final_home,final_away,entered_by,entered_at,res_winner,res_margin,res_btts)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (slug,
                 f"{espn_match['team_home']} vs {espn_match['team_away']}",
                 espn_match["team_home"], espn_match["team_away"],
                 espn_match["tournament"], espn_match.get("event_ts", 0),
                 fh, fa, "espn-companion", time.time(),
                 winner, margin, btts),
            )
            await _db.commit()
            # Queue try scorer fetch in background
            asyncio.create_task(_resolve_and_rescore(
                slug, espn_match["tournament"],
                espn_match["espn_id"], espn_match["league_id"],
                espn_match["team_home"], espn_match["team_away"],
                espn_match.get("event_ts", 0),
                fh, fa, winner, margin, btts,
            ))
            logger.info("Companion result stored: %s %d-%d %s",
                        espn_match["team_home"], fh, fa, espn_match["team_away"])
        except Exception as exc:
            logger.warning("Companion store error for %s: %s", slug, exc)

    return len(pending), applied


async def _resolve_and_rescore(match_id: str, tournament: str,
                               espn_id: str, league_id: int,
                               team_home: str, team_away: str, event_ts: float,
                               fh: int, fa: int, winner: str, margin: str, btts: int) -> None:
    """Background task: fetch try scorers, update match_results, re-score all groups."""
    # ESPN rugby try scorer data can take 5-20 minutes to populate post-match.
    # Retry up to 3 times before giving up.
    try_scorers: list[dict] = []
    first_try: str | None = None
    for attempt, delay in enumerate([300, 600, 900]):  # 5min, 10min, 15min between attempts
        await asyncio.sleep(delay if attempt > 0 else 30)
        try_scorers, first_try = await _fetch_try_scorers(espn_id, league_id)
        if try_scorers:
            logger.info("Try scorers for %s fetched on attempt %d: %s", match_id, attempt + 1, try_scorers)
            break
        logger.info("Try scorers for %s: empty on attempt %d, %s",
                    match_id, attempt + 1,
                    "retrying" if attempt < 2 else "giving up")

    try_scorers_json = json.dumps(try_scorers) if try_scorers else None

    await _db.execute(
        """UPDATE match_results SET
             res_try_scorers=?, res_first_try=?
           WHERE match_id=?""",
        (try_scorers_json, first_try, match_id),
    )
    await _db.commit()

    async with _db.execute("SELECT id FROM groups") as cur:
        group_ids = [r["id"] for r in await cur.fetchall()]
    for gid in group_ids:
        try:
            await _score_group(gid, match_id, tournament, fh, fa,
                               winner, margin, btts,
                               try_scorers, first_try)
        except Exception as exc:
            logger.warning("Re-score group %d error: %s", gid, exc)

    logger.info("Resolved %s: tries=%s first=%s",
                match_id, try_scorers, first_try)


async def _harvest_players(match_id: str, league_id: int) -> None:
    """Fetch ESPN match summary and store player rosters in the players table."""
    url = (f"https://site.api.espn.com/apis/site/v2/sports/rugby"
           f"/{league_id}/summary?event={match_id}")
    try:
        resp = await _http_client.get(url, timeout=12)
        if resp.status_code != 200:
            return
        data = resp.json()
        rosters = data.get("rosters", [])
        for team_entry in rosters:
            team_name = team_entry.get("team", {}).get("displayName", "")
            if not team_name:
                continue
            for athlete in team_entry.get("roster", []):
                name = athlete.get("athlete", {}).get("displayName", "")
                jersey = athlete.get("jersey")
                position = athlete.get("position", {}).get("abbreviation", "")
                if not name:
                    continue
                try:
                    jersey_int = int(jersey) if jersey is not None else None
                except (ValueError, TypeError):
                    jersey_int = None
                await _db.execute(
                    """INSERT INTO players (name,team_name,jersey,position,last_seen_ts,last_seen_match)
                       VALUES(?,?,?,?,?,?)
                       ON CONFLICT(name,team_name) DO UPDATE SET
                         jersey=excluded.jersey, position=excluded.position,
                         last_seen_ts=excluded.last_seen_ts, last_seen_match=excluded.last_seen_match""",
                    (name, team_name, jersey_int, position, time.time(), match_id),
                )
        await _db.commit()
        logger.info("Harvested players for match %s", match_id)
    except Exception as exc:
        logger.warning("Player harvest error for match %s: %s", match_id, exc)


_pre_fetched_squads: set[str] = set()

async def _pre_fetch_squads() -> None:
    """Pre-fetch ESPN squad rosters for matches kicking off within 72h."""
    now = time.time()
    try:
        upcoming = await _fetch_espn_upcoming()
    except Exception as exc:
        logger.warning("Pre-fetch squads: upcoming fetch error: %s", exc)
        return
    for m in upcoming:
        kts = m.get("kickoff_ts") or 0
        espn_id = m.get("espn_id", "")
        league_id = m.get("league_id")
        if not espn_id or not league_id:
            continue
        if not kts or kts <= now or kts > now + 72 * 3600:
            continue
        if espn_id in _pre_fetched_squads:
            async with _db.execute(
                "SELECT COUNT(*) FROM players WHERE last_seen_match=? LIMIT 1", (espn_id,)
            ) as cur:
                row = await cur.fetchone()
            if row and row[0] > 0:
                continue
            _pre_fetched_squads.discard(espn_id)
        await _harvest_players(espn_id, league_id)
        _pre_fetched_squads.add(espn_id)
        logger.info("Pre-fetched squad for ESPN event %s (%s vs %s)", espn_id, m["team_home"], m["team_away"])


async def _auto_fetch_loop():
    """Background task: auto-fetch results every 60 minutes."""
    await asyncio.sleep(120)  # 2-min grace period on startup
    while True:
        try:
            checked, applied = await _auto_apply_results()
            _last_auto_fetch.update({"ts": time.time(), "checked": checked, "applied": applied})
            if applied:
                logger.info("Auto-fetch: applied %d/%d results", applied, checked)
            await _pre_fetch_squads()
        except Exception as exc:
            logger.warning("Auto-fetch loop error: %s", exc)
        await asyncio.sleep(3600)  # every hour


async def _fetch_espn_upcoming() -> list[dict]:
    """Fetch upcoming matches from ESPN for all known leagues. Cached 2h in Redis."""
    cached = await _redis.get("espn:upcoming")
    if cached:
        try:
            return json.loads(cached)
        except Exception:
            pass
    matches = []
    start = datetime.now(timezone.utc)
    end = start + timedelta(days=120)
    date_range = f"{start.strftime('%Y%m%d')}-{end.strftime('%Y%m%d')}"
    for slug, (league_id, league_name) in ALL_ESPN_LEAGUES.items():
        url = (f"https://site.api.espn.com/apis/site/v2/sports/rugby"
               f"/{league_id}/scoreboard?limit=100&dates={date_range}")
        try:
            resp = await _http_client.get(url, timeout=12)
            if resp.status_code != 200:
                continue
            for event in resp.json().get("events", []):
                status_type = event.get("status", {}).get("type", {})
                if status_type.get("completed"):
                    continue
                comp = event.get("competitions", [{}])[0]
                teams = comp.get("competitors", [])
                if len(teams) != 2:
                    continue
                home = next((t for t in teams if t.get("homeAway") == "home"), teams[0])
                away = next((t for t in teams if t.get("homeAway") == "away"), teams[1])
                kickoff_ts = _iso_to_ts(event.get("date", ""))
                matches.append({
                    "tournament": slug,
                    "tournament_name": league_name,
                    "espn_id": event.get("id", ""),
                    "slug": event.get("id", ""),
                    "league_id": league_id,
                    "team_home": home["team"]["displayName"],
                    "team_away": away["team"]["displayName"],
                    "kickoff_ts": kickoff_ts,
                    "in_progress": status_type.get("state") == "in",
                })
        except Exception as exc:
            logger.warning("ESPN upcoming fetch error for %s: %s", slug, exc)
    matches.sort(key=lambda x: x["kickoff_ts"])
    await _redis.setex("espn:upcoming", 7200, json.dumps(matches))
    return matches


async def _fetch_espn_live() -> list[dict]:
    """Fetch in-progress rugby matches for live score ticker. Cached 60s."""
    cached = await _redis.get("espn:live")
    if cached:
        try:
            return json.loads(cached)
        except Exception:
            pass
    live = []
    for slug, (league_id, league_name) in ALL_ESPN_LEAGUES.items():
        url = f"https://site.api.espn.com/apis/site/v2/sports/rugby/{league_id}/scoreboard"
        try:
            resp = await _http_client.get(url, timeout=8)
            if resp.status_code != 200:
                continue
            for event in resp.json().get("events", []):
                st = event.get("status", {}).get("type", {})
                if st.get("state") != "in":
                    continue
                comp = event.get("competitions", [{}])[0]
                teams = comp.get("competitors", [])
                if len(teams) != 2:
                    continue
                home = next((t for t in teams if t.get("homeAway") == "home"), teams[0])
                away = next((t for t in teams if t.get("homeAway") == "away"), teams[1])
                live.append({
                    "tournament":  league_name,
                    "team_home":   home["team"]["displayName"],
                    "team_away":   away["team"]["displayName"],
                    "score_home":  home.get("score", ""),
                    "score_away":  away.get("score", ""),
                    "clock":       event.get("status", {}).get("displayClock", ""),
                })
        except Exception as exc:
            logger.warning("ESPN live error %s: %s", slug, exc)
    await _redis.setex("espn:live", 60, json.dumps(live))
    return live


def _time_until(ts: float) -> str:
    delta = int(ts - time.time())
    if delta <= 0:
        return "now"
    if delta < 3600:
        return f"{delta // 60}m"
    if delta < 86400:
        h, m = divmod(delta, 3600)
        return f"{h}h {m // 60}m" if m >= 60 else f"{h}h"
    d, rem = divmod(delta, 86400)
    h = rem // 3600
    return f"{d}d {h}h" if h else f"{d}d"


# ── Auth helpers ──────────────────────────────────────────────────────────────
def _make_token(username: str, permanent: bool = False) -> str:
    """permanent=True offsets ts so the token is valid for ~5 years under SESSION_MAX_AGE check."""
    ts = str(int(time.time()) + (MAGIC_EXTRA_AGE if permanent else 0))
    sig = hmac.new(SESSION_SECRET.encode(), f"{ts}:{username}".encode(), hashlib.sha256).hexdigest()
    return base64.urlsafe_b64encode(f"{ts}:{username}:{sig}".encode()).decode()


def _token_to_user(token: str) -> str | None:
    try:
        raw = base64.urlsafe_b64decode(token + "==").decode()
        ts_str, username, sig = raw.split(":", 2)
        if time.time() - int(ts_str) > SESSION_MAX_AGE:
            return None
        expected = hmac.new(SESSION_SECRET.encode(), f"{ts_str}:{username}".encode(), hashlib.sha256).hexdigest()
        return username if hmac.compare_digest(sig, expected) else None
    except Exception:
        return None


def _get_session_user(request: Request) -> str | None:
    return _token_to_user(request.cookies.get(SESSION_COOKIE, ""))



async def _get_user(username: str) -> dict | None:
    if not username:
        return None
    async with _db.execute(
        "SELECT id, username, is_admin, created_at FROM users WHERE username=?", (username,)
    ) as cur:
        row = await cur.fetchone()
    return dict(row) if row else None


def _esc(s) -> str:
    return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")

def _ucfirst(s: str) -> str:
    return s[0].upper() + s[1:] if s else s

_WREATHS = ["wreath-gold", "wreath-silver", "wreath-bronze"]
def _avatar_el(username: str, size: int = 40, cls: str = "") -> str:
    """Return an img tag if avatar exists, else a letter-circle div."""
    url = _avatar_url(username)
    style = f"width:{size}px;height:{size}px;border-radius:50%;object-fit:cover;flex-shrink:0"
    if url:
        return f'<img src="{url}" alt="{_esc(username)}" style="{style}" class="{cls}">'
    letter = username[0].upper() if username else "?"
    bg = "background:var(--header)"
    return f'<div style="{style};{bg};display:flex;align-items:center;justify-content:center;font-family:\'Barlow Condensed\',sans-serif;font-size:{max(10,size//2.5):.0f}px;font-weight:700;color:#fff;flex-shrink:0" class="{cls}">{_esc(letter)}</div>'


def _medal(rank: int, size: int = 28) -> str:
    """Return wreath image for top 3, plain number otherwise."""
    if rank <= 3:
        src = _WREATHS[rank - 1]
        return f'<img src="/static/{src}.png" alt="#{rank}" style="width:{size}px;height:{size}px;vertical-align:middle">'
    return f'<span style="color:var(--muted);font-size:.85rem;font-weight:600">#{rank}</span>'


def _base_url(request: Request) -> str:
    host = request.headers.get("host", "")
    if any(host.startswith(p) for p in ["192.168.", "10.", "172."]):
        return f"http://{host}"
    scheme = request.headers.get("x-forwarded-proto", "https")
    return f"{scheme}://{host}"





def _parse_teams(title: str) -> tuple[str, str]:
    for sep in [" vs ", " VS ", " Vs ", " versus "]:
        if sep in title:
            parts = title.split(sep, 1)
            home = parts[0].strip().split("|")[-1].split(":")[-1].strip()
            away = parts[1].split("|")[0].split(":")[0].strip()
            return home, away
    m = re.search(r"(.+?)\s+v\s+(.+?)(?:\s*\||$)", title, re.IGNORECASE)
    if m:
        return m.group(1).strip(), m.group(2).strip()
    return title, ""



# ── Middleware ────────────────────────────────────────────────────────────────
_OPEN_PATHS = {"/login", "/health", "/manifest.json", "/sw.js", "/icon.svg", "/favicon.ico"}

@app.middleware("http")
async def auth_mw(request: Request, call_next):
    path = request.url.path
    if path in _OPEN_PATHS or path.startswith("/join/") or path.startswith("/static/") or path.startswith("/groups/join/") or path.startswith("/avatar/"):
        return await call_next(request)
    if path.startswith("/ws/"):
        return await call_next(request)
    if not _get_session_user(request):
        return RedirectResponse(url="/login", status_code=303)
    return await call_next(request)


# ── Shared HTML helpers ───────────────────────────────────────────────────────
_FONTS = ('<link rel="preconnect" href="https://fonts.googleapis.com">'
          '<link href="https://fonts.googleapis.com/css2?family=Barlow+Condensed:wght@400;600;700&family=Barlow:wght@400;500&display=swap" rel="stylesheet">'
          '<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Material+Symbols+Outlined:opsz,wght,FILL,GRAD@24,400,0,0">')
_PWA_META = ('<link rel="manifest" href="/manifest.json">'
             '<link rel="icon" type="image/png" href="/static/favicon.png">'
             '<meta name="theme-color" content="#1f6e3a">'
             '<meta name="apple-mobile-web-app-capable" content="yes">'
             '<meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">'
             '<meta name="apple-mobile-web-app-title" content="Scrum">'
             '<link rel="apple-touch-icon" href="/static/apple-touch-icon.png">'
             '<script>(function(){'
             'if(window.matchMedia("(display-mode:standalone)").matches)return;'
             'if(localStorage.getItem("pwa-dismissed"))return;'
             'var dp=null;'
             'var iOS=/iphone|ipad|ipod/i.test(navigator.userAgent)&&!window.MSStream;'
             'var iOSSafari=iOS&&/safari/i.test(navigator.userAgent)&&!/chrome|crios|fxios/i.test(navigator.userAgent);'
             'function showBanner(){'
             'if(document.getElementById("pwa-banner"))return;'
             'var b=document.createElement("div");'
             'b.id="pwa-banner";'
             'b.style="position:fixed;bottom:62px;left:0;right:0;background:#1f6e3a;color:#fff;padding:.7rem 1rem;display:flex;align-items:center;gap:.75rem;z-index:9999;box-shadow:0 -2px 12px rgba(0,0,0,.25)";'
             'b.innerHTML="<img src=\'/static/apple-touch-icon.png\' style=\'width:38px;height:38px;border-radius:8px;flex-shrink:0\'>"'
             '+"<div style=\'flex:1\'><div style=\'font-weight:700;font-size:.88rem\'>Install Scrum</div>"'
             '+(iOS?"<div style=\'font-size:.74rem;opacity:.8\'>Tap Share → Add to Home Screen</div>":"<div style=\'font-size:.74rem;opacity:.8\'>Add to your home screen for the best experience</div>")'
             '+"</div>"'
             '+(iOS?"":"<button id=\'pwa-install\' style=\'background:#fff;color:#1f6e3a;border:none;padding:.4rem .85rem;border-radius:6px;font-weight:700;font-size:.82rem;cursor:pointer;flex-shrink:0\'>Install</button>")'
             '+"<button id=\'pwa-x\' style=\'background:none;border:none;color:rgba(255,255,255,.75);font-size:1.25rem;cursor:pointer;padding:.2rem .4rem;flex-shrink:0;line-height:1\'>×</button>";'
             'document.body.appendChild(b);'
             'document.getElementById("pwa-x").onclick=function(){localStorage.setItem("pwa-dismissed","1");b.remove();};'
             'var ib=document.getElementById("pwa-install");'
             'if(ib)ib.onclick=function(){if(dp){dp.prompt();dp.userChoice.then(function(c){if(c.outcome==="accepted")localStorage.setItem("pwa-dismissed","1");b.remove();dp=null;});}};'
             '}'
             'window.addEventListener("beforeinstallprompt",function(e){e.preventDefault();dp=e;showBanner();});'
             'window.addEventListener("appinstalled",function(){localStorage.setItem("pwa-dismissed","1");var b=document.getElementById("pwa-banner");if(b)b.remove();});'
             'if(iOSSafari)document.addEventListener("DOMContentLoaded",showBanner);'
             '})()</script>')

_BASE_CSS = """:root{--bg:#f2f7f3;--surface:#ffffff;--surface2:#e8f2ea;--accent:#22a84a;--accent2:#22a84a;--accent3:#f5a623;--text:#1a2e1e;--muted:#5a7a62;--live:#22a84a;--border:#c8dece;--danger:#e05a2b;--header:#1f6e3a;--header-text:#ffffff}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--text);font-family:'Barlow',sans-serif;min-height:100vh}
a{color:inherit;text-decoration:none}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.4}}
.nav-wordmark{height:28px;width:auto;display:block}
header{display:grid;grid-template-columns:1fr auto 1fr;align-items:center;padding:0 2rem;height:64px;border-bottom:2px solid rgba(0,0,0,.12);background:var(--header);position:sticky;top:0;z-index:10}
.nav-left{display:flex;align-items:center}
.nav-center{display:flex;align-items:center;gap:2.5rem}
.nav-right{display:flex;align-items:center;justify-content:flex-end;gap:1.25rem}
.nav-link{font-family:'Barlow Condensed',sans-serif;font-size:.9rem;font-weight:700;letter-spacing:.08em;text-transform:uppercase;color:rgba(255,255,255,.65);padding:.18rem 0;border-bottom:2px solid transparent;transition:color .2s,border-color .2s}
.nav-link:hover{color:#fff;border-color:rgba(255,255,255,.5)}
.nav-link.active{color:var(--accent3);border-color:var(--accent3)}
.admin-link{font-size:.75rem;opacity:.5;letter-spacing:.05em;border-bottom:none}
.admin-link:hover{opacity:.9}
.ubadge{font-size:.75rem;color:rgba(255,255,255,.6);font-family:'Barlow Condensed',sans-serif;letter-spacing:.05em}
.logout-link{font-size:.78rem;border-bottom:none;color:rgba(255,255,255,.55)}.logout-link:hover{color:#fff}
input[type=text],input[type=password],input[type=number],select{background:#fff;border:1.5px solid var(--border);border-radius:8px;color:var(--text);font-family:'Barlow',sans-serif;font-size:.9rem;padding:.55rem .85rem;outline:none;transition:border-color .2s}
input:focus,select:focus{border-color:var(--header)}
.btn{display:inline-block;background:var(--accent3);color:#fff;border:none;border-radius:8px;font-family:'Barlow Condensed',sans-serif;font-size:.9rem;font-weight:700;letter-spacing:.06em;text-transform:uppercase;padding:.55rem 1.2rem;cursor:pointer;transition:background .2s}
.btn:hover{background:#d4891e}
.btn-sm{padding:.32rem .8rem;font-size:.78rem;border-radius:6px}
.btn-ghost{background:transparent;color:var(--muted);border:1.5px solid var(--border)}.btn-ghost:hover{color:var(--text);border-color:var(--muted)}
.btn-danger{background:rgba(239,68,68,.08);color:#d63030;border:1px solid rgba(239,68,68,.2)}.btn-danger:hover{background:rgba(239,68,68,.15)}
.card{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:1rem 1.2rem}
.tag{font-family:'Barlow Condensed',sans-serif;font-size:.68rem;font-weight:700;letter-spacing:.08em;text-transform:uppercase;padding:.15rem .45rem;border-radius:4px}
.material-symbols-outlined{font-size:1.1rem;vertical-align:middle;font-variation-settings:'FILL' 0,'wght' 400,'GRAD' 0,'opsz' 24}
.bnav{display:none;position:fixed;bottom:0;left:0;right:0;height:58px;background:#fff;border-top:1.5px solid var(--border);z-index:20;justify-content:space-around;align-items:center;box-shadow:0 -2px 12px rgba(0,0,0,.06)}
.bnav-item{display:flex;flex-direction:column;align-items:center;gap:3px;flex:1;color:var(--muted);font-family:'Barlow Condensed',sans-serif;font-size:.62rem;font-weight:700;letter-spacing:.05em;text-transform:uppercase;padding:.5rem 0;transition:color .2s;text-decoration:none}
.bnav-item:hover,.bnav-item.active{color:var(--header)}
.bnav-item.active span:not(.bnav-badge){color:var(--accent3)}
.bnav-icon{width:24px;height:24px;object-fit:contain;filter:brightness(0) saturate(100%) invert(42%) sepia(15%) saturate(600%) hue-rotate(95deg) brightness(85%);transition:filter .2s}
.bnav-item.active .bnav-icon{filter:brightness(0) saturate(100%) invert(62%) sepia(70%) saturate(900%) hue-rotate(3deg) brightness(100%)}
.more-sheet{display:none;position:fixed;inset:0;background:rgba(0,0,0,.45);z-index:30;align-items:flex-end}
.more-sheet.open{display:flex}
.more-body{background:#fff;width:100%;border-radius:16px 16px 0 0;padding:.75rem 0 2rem;display:flex;flex-direction:column}
.more-handle{width:36px;height:4px;background:var(--border);border-radius:99px;margin:.25rem auto .75rem}
.more-row{display:flex;align-items:center;gap:1rem;padding:.9rem 1.5rem;font-size:1rem;font-weight:500;color:var(--text);text-decoration:none;transition:background .15s}
.more-row:active{background:var(--surface2)}
.more-active{color:var(--header);font-weight:700}
.more-icon{font-size:1.35rem!important;color:var(--muted)}
@media(max-width:768px){.bnav{display:flex}.page-body{padding-bottom:72px}.nav-center{display:none}}
.bnav-badge{display:inline-block;background:var(--accent3);color:#fff;font-size:.55rem;font-weight:700;border-radius:99px;padding:.05rem .35rem;margin-left:.2rem;vertical-align:middle;line-height:1.4}
.banker-toggle{display:flex;align-items:center;gap:.75rem;background:rgba(245,166,35,.08);border:1.5px solid rgba(245,166,35,.3);border-radius:10px;padding:.7rem .85rem;cursor:pointer;margin-top:.75rem}
.banker-toggle input[type=checkbox]{width:18px;height:18px;accent-color:var(--accent3);flex-shrink:0}
.banker-label{display:flex;align-items:center;gap:.6rem}
.banker-icon{font-size:1.2rem}
.pred-extra-field{margin:.75rem 0}
.pef-label{font-size:.78rem;font-weight:700;letter-spacing:.07em;text-transform:uppercase;color:var(--muted);margin-bottom:.4rem}
.pef-opts{display:flex;gap:1rem;flex-wrap:wrap}
.radio-opt{display:flex;align-items:center;gap:.4rem;font-size:.9rem;cursor:pointer}
.radio-opt input[type=radio]{accent-color:var(--accent3)}
.pred-summary-row{display:flex;gap:.5rem;align-items:center;margin:.25rem 0;font-size:.85rem}
.psr-label{color:var(--muted);min-width:120px}
.psr-val{font-weight:500}
.mr-result-block{border-top:2px solid var(--border);margin-top:.25rem;padding:.6rem .75rem .5rem}
.mr-result-hdr{font-family:'Barlow Condensed',sans-serif;font-size:.68rem;font-weight:700;letter-spacing:.1em;text-transform:uppercase;color:var(--muted);margin-bottom:.45rem}
.mr-result-row{display:flex;align-items:center;justify-content:space-between;padding:.2rem 0;font-size:.82rem;border-bottom:1px solid var(--border)}
.mr-result-row:last-child{border-bottom:none}
.mr-result-label{color:var(--muted);font-size:.75rem}
.mr-result-val{font-family:'Barlow Condensed',sans-serif;font-weight:700;color:var(--text);text-align:right;max-width:60%;word-break:break-word}
"""


def _nav(username: str, is_admin: bool, active: str = "") -> str:
    def _ac(name):
        return " active" if active == name else ""
    adm = f'<a href="/admin" class="nav-link admin-link{_ac("admin")}">Admin</a>' if is_admin else ""
    return f"""<header>
  <div class="nav-left"><a href="/"><img src="/static/wordmark.png" alt="Scrum" class="nav-wordmark"></a></div>
  <div class="nav-center">
    <a href="/me" class="nav-link{_ac('me')}">Me</a>
    <a href="/groups" class="nav-link{_ac('groups')}">Groups</a>
    <a href="/leaderboard" class="nav-link{_ac('leaderboard')}">Predictions</a>
    <a href="/standings" class="nav-link{_ac('standings')}">Standings</a>
    <a href="/history" class="nav-link{_ac('history')}">Results</a>
    <a href="/news" class="nav-link{_ac('news')}">News</a>
    <a href="/how-to-play" class="nav-link{_ac('how-to-play')}">How to Play</a>
  </div>
  <div class="nav-right">
    {adm}
    <a href="/logout" class="nav-link logout-link">Logout</a>
  </div>
</header>"""


def _bnav(active: str = "", invite_count: int = 0, is_admin: bool = False) -> str:
    def _ac(name):
        return " active" if active == name else ""
    inv_badge = f'<span class="bnav-badge">{invite_count}</span>' if invite_count else ""
    def _icon(src, label, badge=""):
        return f'<img src="/static/{src}" class="bnav-icon" alt="{label}"><span>{label}{badge}</span>'
    more_active = " active" if active in ("news", "how-to-play", "admin", "standings") else ""
    admin_row = f'<a href="/admin" class="more-row">  <span class="material-symbols-outlined more-icon">admin_panel_settings</span><span>Admin</span></a>' if is_admin else ""
    return f"""<nav class="bnav">
  <a href="/me" class="bnav-item{_ac('me')}">{_icon("nav-person.png","Me",inv_badge)}</a>
  <a href="/groups" class="bnav-item{_ac('groups')}">{_icon("nav-shields.png","Groups")}</a>
  <a href="/leaderboard" class="bnav-item{_ac('leaderboard')}">{_icon("nav-ball.png","Predict")}</a>
  <a href="/history" class="bnav-item{_ac('history')}">
    <span class="material-symbols-outlined" style="font-size:1.35rem">scoreboard</span>
    <span>Results</span>
  </a>
  <button class="bnav-item{more_active}" onclick="document.getElementById('more-sheet').classList.toggle('open')" style="background:none;border:none;cursor:pointer">
    <span class="material-symbols-outlined" style="font-size:1.35rem;color:inherit">more_horiz</span>
    <span>More</span>
  </button>
</nav>
<div id="more-sheet" class="more-sheet" onclick="this.classList.remove('open')">
  <div class="more-body" onclick="event.stopPropagation()">
    <div class="more-handle"></div>
    <a href="/standings" class="more-row{' more-active' if active=='standings' else ''}">
      <span class="material-symbols-outlined more-icon">bar_chart</span><span>Standings</span>
    </a>
    <a href="/news" class="more-row{' more-active' if active=='news' else ''}">
      <span class="material-symbols-outlined more-icon">newspaper</span><span>News</span>
    </a>
    <a href="/how-to-play" class="more-row{' more-active' if active=='how-to-play' else ''}">
      <span class="material-symbols-outlined more-icon">help_outline</span><span>How to Play</span>
    </a>
    {admin_row}
    <a href="/logout" class="more-row" style="color:var(--danger)">
      <span class="material-symbols-outlined more-icon" style="color:var(--danger)">logout</span><span>Log Out</span>
    </a>
  </div>
</div>"""


# ── PWA endpoints ─────────────────────────────────────────────────────────────
@app.get("/manifest.json")
async def pwa_manifest():
    data = {
        "name": "Scrum",
        "short_name": "Scrum",
        "description": "Self-hosted rugby prediction league",
        "start_url": "/",
        "display": "standalone",
        "background_color": "#1f6e3a",
        "theme_color": "#1f6e3a",
        "orientation": "portrait",
        "icons": [
            {"src": "/static/icon-512.png",     "sizes": "512x512", "type": "image/png", "purpose": "any"},
            {"src": "/static/icon-maskable.png", "sizes": "512x512", "type": "image/png", "purpose": "maskable"},
        ],
    }
    return Response(content=json.dumps(data), media_type="application/manifest+json")


@app.get("/sw.js")
async def service_worker():
    js = """/* v3 */
self.addEventListener('install',e=>self.skipWaiting());
self.addEventListener('activate',e=>e.waitUntil(
  clients.claim().then(()=>clients.matchAll({type:'window'}).then(cs=>cs.forEach(c=>c.postMessage({type:'sw-updated'}))))
));
self.addEventListener('fetch',()=>{});"""
    return Response(content=js, media_type="application/javascript",
                    headers={"Service-Worker-Allowed": "/", "Cache-Control": "no-store"})


@app.get("/icon.svg")
async def pwa_icon():
    svg = '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 100 100"><rect width="100" height="100" rx="20" fill="#1f2838"/><text x="50" y="68" font-family="Arial Black,sans-serif" font-size="52" font-weight="900" text-anchor="middle" fill="#00b0ff">S</text></svg>'
    return Response(content=svg, media_type="image/svg+xml")


# ── Magic invite link ─────────────────────────────────────────────────────────
def _join_page(token: str, username: str) -> str:
    return f"""<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Join · Scrum</title>{_FONTS}<style>
{_BASE_CSS}
.join-wrap{{display:flex;align-items:center;justify-content:center;min-height:100vh}}
.join-box{{background:var(--surface);border:1px solid var(--border);border-radius:12px;
  padding:2.5rem 2rem;text-align:center;max-width:360px;width:100%;display:flex;flex-direction:column;gap:1.2rem}}
.join-logo{{font-family:'Barlow Condensed',sans-serif;font-size:1.8rem;font-weight:800;letter-spacing:.05em}}
.join-logo span{{color:var(--accent3)}}
.join-sub{{color:var(--muted);font-size:.9rem}}
</style></head><body>
<div class="join-wrap">
  <div class="join-box">
    <div class="join-logo">Scrum</div>
    <div>You've been invited to join as <strong>{_esc(username)}</strong>.</div>
    <div class="join-sub">Click below to activate your account and go to the site.</div>
    <form method="post" action="/join/{_esc(token)}">
      <button type="submit" class="btn" style="width:100%;font-size:1rem;padding:.75rem">Join Now</button>
    </form>
  </div>
</div></body></html>"""

@app.get("/join/{token}", response_class=HTMLResponse)
async def join_invite_get(token: str, request: Request):
    async with _db.execute(
        "SELECT username, used_at FROM invite_tokens WHERE token=?", (token,)
    ) as cur:
        row = await cur.fetchone()
    if not row:
        return HTMLResponse("""<!DOCTYPE html><html><head><meta charset="utf-8">
<title>Invalid Link</title></head><body style="background:var(--bg);color:var(--text);display:flex;
align-items:center;justify-content:center;min-height:100vh;font-family:sans-serif">
<div style="text-align:center"><h2>Link Not Found</h2>
<p style="color:#8d9fbc;margin-top:.5rem">This invite link is invalid.</p>
</div></body></html>""", status_code=404)
    if row["used_at"]:
        current = _get_session_user(request)
        if current == row["username"]:
            return RedirectResponse(url="/", status_code=303)
        return HTMLResponse("""<!DOCTYPE html><html><head><meta charset="utf-8">
<title>Link Used</title></head><body style="background:var(--bg);color:var(--text);display:flex;
align-items:center;justify-content:center;min-height:100vh;font-family:sans-serif">
<div style="text-align:center"><h2>Link Already Used</h2>
<p style="color:#8d9fbc;margin-top:.5rem">This invite link has already been used.<br>
Ask the admin for a new one.</p></div></body></html>""", status_code=410)
    return HTMLResponse(_join_page(token, row["username"]))

@app.post("/join/{token}", response_class=HTMLResponse)
async def join_invite_post(token: str, request: Request):
    async with _db.execute(
        "SELECT username, used_at FROM invite_tokens WHERE token=?", (token,)
    ) as cur:
        row = await cur.fetchone()
    if not row or row["used_at"]:
        return HTMLResponse("""<!DOCTYPE html><html><head><meta charset="utf-8">
<title>Link Used</title></head><body style="background:var(--bg);color:var(--text);display:flex;
align-items:center;justify-content:center;min-height:100vh;font-family:sans-serif">
<div style="text-align:center"><h2>Link Already Used</h2>
<p style="color:#8d9fbc;margin-top:.5rem">This invite link has already been used.<br>
Ask the admin for a new one.</p></div></body></html>""", status_code=410)
    await _db.execute("UPDATE invite_tokens SET used_at=? WHERE token=?", (time.time(), token))
    await _db.commit()
    username = row["username"]
    resp = RedirectResponse(url="/", status_code=303)
    resp.set_cookie(SESSION_COOKIE, _make_token(username, permanent=True),
                    max_age=60*60*24*365*5, httponly=True, samesite="lax")
    logger.info("Magic link used: %s", username)
    return resp


# ── Login ─────────────────────────────────────────────────────────────────────
@app.get("/login", response_class=HTMLResponse)
def login_page(error: str = ""):
    err_html = f'<div style="color:var(--live);font-size:.85rem;text-align:center">{_esc(error)}</div>' if error else ""
    return HTMLResponse(f"""<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>Scrum</title>
{_FONTS}{_PWA_META}<style>{_BASE_CSS}
body{{display:flex;align-items:center;justify-content:center;background-image:radial-gradient(ellipse at 50% 0%,rgba(0,176,255,.06) 0%,transparent 60%)}}
.box{{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:2.5rem;width:100%;max-width:360px;display:flex;flex-direction:column;gap:1.4rem}}
.box-title{{font-family:'Barlow Condensed',sans-serif;font-size:1.8rem;font-weight:700;text-align:center;letter-spacing:.05em;text-transform:uppercase}}
.box-title span{{color:var(--accent)}}form{{display:flex;flex-direction:column;gap:.9rem}}input{{width:100%}}
</style></head><body>
<div class="box">
  <div class="box-title">Scrum</div>
  <form method="post" action="/login">
    <input type="text" name="username" placeholder="Username" autofocus autocomplete="username">
    <input type="password" name="password" placeholder="Password" autocomplete="current-password">
    <button type="submit" class="btn" style="width:100%">Enter</button>
    {err_html}
  </form>
</div>
<script>if('serviceWorker' in navigator)navigator.serviceWorker.register('/sw.js').catch(()=>{{}});</script>
</body></html>""")


@app.post("/login")
async def login(request: Request, username: str = Form(...), password: str = Form(...)):
    username = username.lower().strip()
    async with _db.execute("SELECT password_hash FROM users WHERE username=?", (username,)) as cur:
        row = await cur.fetchone()
    if row and pwd_ctx.verify(password, row["password_hash"]):
        resp = RedirectResponse(url="/", status_code=303)
        resp.set_cookie(SESSION_COOKIE, _make_token(username),
                        max_age=SESSION_MAX_AGE, httponly=True, samesite="lax")
        return resp
    return login_page("Invalid username or password")


@app.get("/logout")
def logout():
    resp = RedirectResponse(url="/login", status_code=303)
    resp.delete_cookie(SESSION_COOKIE, httponly=True, samesite="lax")
    return resp


# ── Match predictions breakdown API ───────────────────────────────────────────
@app.get("/api/match-predictions/{match_id}")
async def api_match_predictions(request: Request, match_id: str):
    username = _get_session_user(request)
    if not username:
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    async with _db.execute(
        """SELECT p.username, p.score_home, p.score_away,
                  l.diff, l.exact_score, l.points
           FROM predictions p
           LEFT JOIN leaderboard l ON l.group_id=1 AND l.match_id = p.match_id AND l.username = p.username
           WHERE p.match_id = ?
           ORDER BY l.diff ASC NULLS LAST, p.created_at ASC""",
        (match_id,),
    ) as cur:
        rows = [dict(r) for r in await cur.fetchall()]
    # Include full result data for the match breakdown panel
    async with _db.execute(
        """SELECT final_home, final_away, res_winner, res_margin, res_btts,
                  res_try_scorers, res_first_try
           FROM match_results WHERE match_id=?""",
        (match_id,),
    ) as cur:
        result_row = await cur.fetchone()
    result = {}
    if result_row:
        result["final_home"]  = result_row["final_home"]
        result["final_away"]  = result_row["final_away"]
        result["winner"]      = result_row["res_winner"]
        result["margin"]      = result_row["res_margin"]
        result["btts"]        = result_row["res_btts"]
        result["first_try"]   = result_row["res_first_try"]
        try:
            raw = json.loads(result_row["res_try_scorers"] or "[]")
            # Support both old format (list of strings) and new format (list of dicts)
            if raw and isinstance(raw[0], str):
                result["try_scorers"] = raw  # old flat format
                result["try_details"] = []
            else:
                result["try_scorers"] = [s["name"] for s in raw]
                result["try_details"] = raw  # [{name, team, clock}, ...]
        except Exception:
            result["try_scorers"] = []
            result["try_details"] = []
    return JSONResponse({"me": username, "preds": rows, "result": result})


# ── Live scores API ───────────────────────────────────────────────────────────
@app.get("/api/live-scores")
async def api_live_scores(request: Request):
    if not _get_session_user(request):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    try:
        return JSONResponse({"matches": await _fetch_espn_live()})
    except Exception as exc:
        return JSONResponse({"matches": [], "error": str(exc)})


# ── Root ─────────────────────────────────────────────────────────────────────
@app.get("/", response_class=HTMLResponse)
async def root_redirect():
    return RedirectResponse(url="/me", status_code=302)


# ── Middleware: allow group invite paths ──────────────────────────────────────
# (group invites use /groups/join/{token} — already passes auth_mw as logged-in)

# ── Prediction types metadata ─────────────────────────────────────────────────
PREDICTION_TYPES = [
    ("score",      "Score Prediction",      "Predict the exact final score.",                             "auto"),
    ("winner",     "Winner",                "Pick the winning team (or draw).",                           "auto"),
    ("margin",     "Winning Margin Band",   "Predict the margin range (1–7, 8–14, 15–21, 22+).",         "auto"),
    ("try_anytime","Anytime Try Scorer",    "Pick a player who scores a try at any point.",               "auto"),
    ("try_first",  "First Try Scorer",      "Pick the player who scores the very first try.",             "auto"),
    ("btts",       "Both Teams to Score",   "Will both teams score at least one try?",                   "auto"),
]
# resolve_type: "auto" = ESPN resolves it; "manual" = admin may need to enter it
PRED_RESOLVE = {k: r for k, _, _, r in PREDICTION_TYPES}
PRED_LABEL   = {k: l for k, l, _, _ in PREDICTION_TYPES}


async def _get_friends(username: str) -> list[str]:
    async with _db.execute(
        "SELECT friend_username FROM friends WHERE username=? ORDER BY created_at",
        (username,),
    ) as cur:
        return [r["friend_username"] for r in await cur.fetchall()]


async def _pending_invite_count(username: str) -> int:
    async with _db.execute(
        "SELECT COUNT(*) as n FROM group_invites WHERE invited_username=? AND used_at IS NULL",
        (username,),
    ) as cur:
        row = await cur.fetchone()
    return row["n"] if row else 0


async def _get_user_groups(username: str, include_global: bool = False) -> list[dict]:
    async with _db.execute(
        """SELECT g.id, g.name, g.slug, g.description, gm.role
           FROM groups g JOIN group_members gm ON g.id=gm.group_id
           WHERE gm.username=? AND (? OR g.id != 1) ORDER BY g.name""",
        (username, 1 if include_global else 0),
    ) as cur:
        return [dict(r) for r in await cur.fetchall()]


async def _get_group(slug: str) -> dict | None:
    async with _db.execute("SELECT * FROM groups WHERE slug=?", (slug,)) as cur:
        row = await cur.fetchone()
    return dict(row) if row else None


async def _group_member_role(group_id: int, username: str) -> str | None:
    async with _db.execute(
        "SELECT role FROM group_members WHERE group_id=? AND username=?", (group_id, username)
    ) as cur:
        row = await cur.fetchone()
    return row["role"] if row else None


async def _group_leagues(group_id: int) -> list[str]:
    async with _db.execute(
        "SELECT league_slug FROM group_leagues WHERE group_id=?", (group_id,)
    ) as cur:
        return [r["league_slug"] for r in await cur.fetchall()]


async def _group_pred_types(group_id: int) -> list[str]:
    async with _db.execute(
        "SELECT prediction_type FROM group_prediction_types WHERE group_id=?", (group_id,)
    ) as cur:
        return [r["prediction_type"] for r in await cur.fetchall()]


async def _group_custom_comps(group_id: int) -> list[dict]:
    async with _db.execute(
        "SELECT * FROM custom_competitions WHERE group_id=? ORDER BY created_at",
        (group_id,),
    ) as cur:
        return [dict(r) for r in await cur.fetchall()]


async def _custom_comp_matches(comp_id: int) -> list[dict]:
    async with _db.execute(
        "SELECT * FROM custom_competition_matches WHERE comp_id=? ORDER BY kickoff_ts",
        (comp_id,),
    ) as cur:
        return [dict(r) for r in await cur.fetchall()]


async def _all_group_custom_matches(group_id: int) -> dict[str, int]:
    """Returns {match_id: comp_id} for all matches across all custom comps in a group."""
    async with _db.execute(
        """SELECT ccm.match_id, ccm.comp_id FROM custom_competition_matches ccm
           JOIN custom_competitions cc ON cc.id=ccm.comp_id
           WHERE cc.group_id=?""",
        (group_id,),
    ) as cur:
        return {r["match_id"]: r["comp_id"] for r in await cur.fetchall()}


# ── Me page ───────────────────────────────────────────────────────────────────
@app.get("/me", response_class=HTMLResponse)
async def me_page(request: Request):
    username = _get_session_user(request)
    user = await _get_user(username)
    is_admin = user and user["is_admin"]

    pending_invites = await _pending_invite_count(username)

    # Overall stats — group_id=1 (Global) gives exactly one row per match per user
    async with _db.execute(
        """SELECT COALESCE(SUM(points),0) as total_pts,
                  COUNT(*) as matches,
                  COALESCE(SUM(exact_score),0) as exact_count
           FROM leaderboard WHERE username=? AND group_id=1""",
        (username,),
    ) as cur:
        overall = dict(await cur.fetchone())

    total_pts   = overall["total_pts"]
    exact_count = overall["exact_count"]

    # Streak — consecutive scored matches, deduped via group_id=1
    async with _db.execute(
        """SELECT points FROM leaderboard WHERE username=? AND group_id=1
           ORDER BY created_at DESC LIMIT 50""",
        (username,),
    ) as cur:
        streak_rows = [r["points"] for r in await cur.fetchall()]
    streak = 0
    for pts in streak_rows:
        if pts > 0:
            streak += 1
        else:
            break

    # Per-group breakdown
    async with _db.execute(
        """SELECT g.name, g.slug, gm.role,
                  COALESCE(SUM(l.points),0) as pts,
                  COUNT(l.id) as scored,
                  COALESCE(SUM(l.exact_score),0) as exact_sc
           FROM group_members gm
           JOIN groups g ON g.id=gm.group_id
           LEFT JOIN leaderboard l ON l.group_id=gm.group_id AND l.username=gm.username
           WHERE gm.username=? AND g.id != 1
           GROUP BY g.id ORDER BY pts DESC""",
        (username,),
    ) as cur:
        group_stats = [dict(r) for r in await cur.fetchall()]

    # Per-group rank
    for gs in group_stats:
        async with _db.execute(
            """SELECT COUNT(*)+1 as rank FROM (
                   SELECT username, SUM(points) as tp FROM leaderboard
                   WHERE group_id=(SELECT id FROM groups WHERE slug=?)
                   GROUP BY username HAVING tp > ?
               )""",
            (gs["slug"], gs["pts"]),
        ) as cur:
            row = await cur.fetchone()
            gs["rank"] = row["rank"] if row else 1
        async with _db.execute(
            "SELECT COUNT(*) as n FROM group_members WHERE group_id=(SELECT id FROM groups WHERE slug=?)",
            (gs["slug"],),
        ) as cur:
            row = await cur.fetchone()
            gs["total_members"] = row["n"] if row else 1

    # Recent predictions (last 10)
    async with _db.execute(
        """SELECT p.match_title, p.team_home, p.team_away, p.score_home, p.score_away,
                  p.kickoff_ts, p.tournament,
                  r.final_home, r.final_away,
                  l.points, l.exact_score, l.diff
           FROM predictions p
           LEFT JOIN match_results r ON r.match_id=p.match_id
           LEFT JOIN leaderboard l ON l.match_id=p.match_id AND l.username=p.username AND l.group_id=1
           WHERE p.username=?
           ORDER BY p.created_at DESC LIMIT 10""",
        (username,),
    ) as cur:
        recent = [dict(r) for r in await cur.fetchall()]

    # Next upcoming fixture + missed predictions
    open_matches = []   # window open, user hasn't predicted
    next_upcoming = None  # soonest future match (window not yet open)
    missed_count = 0
    try:
        upcoming = await _fetch_espn_upcoming()
        now_ts = time.time()
        # Merge custom competition matches not yet in ESPN feed
        user_groups_for_custom = await _get_user_groups(username, include_global=False)
        custom_by_mid: dict[str, dict] = {}
        for ug in user_groups_for_custom:
            custom_by_mid.update(await _all_group_custom_matches(ug["id"]))
        # Load full match data for custom matches
        all_custom_match_data: dict[str, dict] = {}
        for ug in user_groups_for_custom:
            for cc in await _group_custom_comps(ug["id"]):
                for cm in await _custom_comp_matches(cc["id"]):
                    all_custom_match_data[cm["match_id"]] = cm
        espn_slugs_seen = {
            (m.get("slug") or re.sub(r"[^a-z0-9]+", "-", f"{m['team_home']}-vs-{m['team_away']}".lower()).strip("-"))
            for m in upcoming
        }
        full_upcoming = list(upcoming)
        for mid, cm in all_custom_match_data.items():
            if mid not in espn_slugs_seen and cm["kickoff_ts"] > now_ts:
                full_upcoming.append({
                    "team_home": cm["team_home"], "team_away": cm["team_away"],
                    "kickoff_ts": cm["kickoff_ts"], "tournament": cm["tournament"],
                    "slug": cm["match_id"],
                })
        full_upcoming.sort(key=lambda m: m.get("kickoff_ts") or 0)
        for m in full_upcoming:
            kts = m.get("kickoff_ts") or 0
            if not kts or kts <= now_ts:
                continue
            m_slug = m.get("slug") or re.sub(r"[^a-z0-9]+", "-", f"{m['team_home']}-vs-{m['team_away']}".lower()).strip("-")
            async with _db.execute(
                "SELECT id FROM predictions WHERE username=? AND match_id=?", (username, m_slug)
            ) as cur:
                already = await cur.fetchone()
            if already:
                continue
            window_open = kts - 172800 <= now_ts < kts
            if window_open:
                m["slug"] = m_slug
                open_matches.append(m)
            elif not next_upcoming:
                m["slug"] = m_slug
                next_upcoming = m
        # Missed = matches where window opened + closed and user didn't predict
        # Only count missed predictions from after the user joined their first group
        # (prevents legacy-imported predictions from counting as missed)
        async with _db.execute(
            "SELECT MIN(joined_at) FROM group_members WHERE username=? AND group_id != 1",
            (username,),
        ) as cur:
            row = await cur.fetchone()
            joined_since = row[0] if (row and row[0]) else now_ts
        async with _db.execute(
            """SELECT COUNT(DISTINCT p2.match_id) as n FROM predictions p2
               JOIN group_members gm ON gm.username=p2.username AND gm.group_id IN (
                   SELECT group_id FROM group_members WHERE username=? AND group_id != 1
               )
               LEFT JOIN predictions my ON my.match_id=p2.match_id AND my.username=?
               WHERE my.match_id IS NULL AND p2.kickoff_ts > ? AND p2.kickoff_ts < ?""",
            (username, username, joined_since, now_ts - 172800),
        ) as cur:
            row = await cur.fetchone()
            missed_count = row["n"] if row else 0
    except Exception:
        pass

    # Pending invites
    async with _db.execute(
        """SELECT gi.token, gi.created_at, g.name, g.slug, gi.created_by
           FROM group_invites gi JOIN groups g ON gi.group_id=g.id
           WHERE gi.invited_username=? AND gi.used_at IS NULL
           ORDER BY gi.created_at DESC""",
        (username,),
    ) as cur:
        invites = [dict(r) for r in await cur.fetchall()]

    # Friends
    my_friends = await _get_friends(username)
    # All users not already friends and not self, for the add-friend dropdown
    async with _db.execute(
        "SELECT username FROM users WHERE username != ? ORDER BY username",
        (username,),
    ) as cur:
        all_users = [r["username"] for r in await cur.fetchall()]
    not_friends = [u for u in all_users if u not in my_friends]

    # Prediction type accuracy — only resolved matches
    async with _db.execute(
        """SELECT p.score_home, p.score_away,
                  p.pred_winner, p.pred_margin, p.pred_btts,
                  p.pred_try_any, p.pred_try_first,
                  r.final_home, r.final_away,
                  r.res_winner, r.res_margin, r.res_btts,
                  r.res_try_scorers, r.res_first_try
           FROM predictions p
           JOIN match_results r ON r.match_id = p.match_id
           WHERE p.username = ?""",
        (username,),
    ) as cur:
        resolved_preds = [dict(r) for r in await cur.fetchall()]

    def _acc(predicted, correct):
        if not predicted:
            return None
        pct = round(correct / predicted * 100)
        return {"predicted": predicted, "correct": correct, "pct": pct}

    w_pred = w_hit = 0
    m_pred = m_hit = 0
    b_pred = b_hit = 0
    ta_pred = ta_hit = 0
    tf_pred = tf_hit = 0

    for rp in resolved_preds:
        if rp["pred_winner"] and rp["res_winner"]:
            w_pred += 1
            if rp["pred_winner"] == rp["res_winner"]: w_hit += 1
        if rp["pred_margin"] and rp["res_margin"]:
            m_pred += 1
            if rp["pred_margin"] == rp["res_margin"]: m_hit += 1
        if rp["pred_btts"] is not None and rp["res_btts"] is not None:
            b_pred += 1
            try:
                if int(rp["pred_btts"]) == rp["res_btts"]: b_hit += 1
            except (ValueError, TypeError):
                pass
        if rp["pred_try_any"] and rp["res_try_scorers"]:
            ta_pred += 1
            try:
                raw = json.loads(rp["res_try_scorers"])
                if rp["pred_try_any"] in _scorer_names(raw): ta_hit += 1
            except Exception:
                pass
        if rp["pred_try_first"] and rp["res_first_try"]:
            tf_pred += 1
            if rp["pred_try_first"] == rp["res_first_try"]: tf_hit += 1

    # Score accuracy — use resolved predictions count (not leaderboard rows)
    s_pred = len(resolved_preds)
    s_hit  = sum(1 for rp in resolved_preds
                 if rp["final_home"] is not None
                 and rp["score_home"] == rp["final_home"]
                 and rp["score_away"] == rp["final_away"])

    type_stats = [
        ("Score",           s_pred,  s_hit,       "exact"),
        ("Winner",          w_pred,  w_hit,       "correct"),
        ("Margin Band",     m_pred,      m_hit,        "correct"),
        ("Both Teams Scored", b_pred,    b_hit,        "correct"),
        ("Anytime Try",     ta_pred,     ta_hit,       "correct"),
        ("First Try",       tf_pred,     tf_hit,       "correct"),
    ]

    type_rows_html = ""
    for label, pred, hit, hit_label in type_stats:
        if not pred:
            continue
        pct = round(hit / pred * 100)
        bar_w = max(4, pct)
        type_rows_html += f"""<div class="acc-row">
  <div class="acc-label">{label}</div>
  <div class="acc-bar-wrap">
    <div class="acc-bar" style="width:{bar_w}%"></div>
  </div>
  <div class="acc-nums">{hit}/{pred} <span class="acc-pct">{pct}%</span></div>
</div>"""

    accuracy_section = f"""<div class="section-block">
  <div class="section-title">Prediction Accuracy</div>
  <div style="background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:.75rem 1rem">
    {type_rows_html if type_rows_html else '<div style="color:var(--muted);font-size:.85rem;padding:.5rem 0">No resolved predictions yet.</div>'}
  </div>
</div>""" if type_rows_html else ""

    # Friends section HTML
    friend_chips = ""
    for f in my_friends:
        friend_chips += f"""<div class="friend-chip">
  <a href="/h2h/{_esc(f)}" title="Head to Head" style="text-decoration:none;display:flex;align-items:center">{_avatar_el(f, 26)}</a>
  <a href="/h2h/{_esc(f)}" class="fc-name" style="text-decoration:none;color:inherit">{_esc(_ucfirst(f))}</a>
  <form method="post" action="/friends/remove" style="display:inline">
    <input type="hidden" name="target" value="{_esc(f)}">
    <button type="submit" class="fc-remove" title="Remove friend">×</button>
  </form>
</div>"""

    users_json = json.dumps([{"v": u, "l": _ucfirst(u)} for u in not_friends])
    add_form = f"""<form method="post" action="/friends/add" class="friend-add-form" id="friend-add-form">
  <input type="hidden" name="target" id="friend-target">
  <div class="fac-wrap" id="fac-wrap">
    <input type="text" id="fac-input" placeholder="Tap to see all players…" autocomplete="off" style="width:100%;padding-right:2rem">
    <span class="fac-arrow">▾</span>
    <div class="fac-dropdown" id="fac-dropdown"></div>
  </div>
  <button type="submit" class="btn btn-sm" id="friend-add-btn" disabled style="opacity:.4">Add</button>
</form>
<script>
(function(){{
  const users={users_json};
  const input=document.getElementById('fac-input');
  const drop=document.getElementById('fac-dropdown');
  const target=document.getElementById('friend-target');
  const btn=document.getElementById('friend-add-btn');
  function select(v,l){{
    input.value=l; target.value=v;
    drop.innerHTML=''; drop.style.display='none';
    btn.disabled=false; btn.style.opacity='1';
  }}
  function clear(){{ target.value=''; btn.disabled=true; btn.style.opacity='.4'; }}
  function showDrop(hits){{
    const r=input.getBoundingClientRect();
    drop.style.top=r.bottom+'px';
    drop.style.left=r.left+'px';
    drop.style.width=r.width+'px';
    drop.innerHTML=hits.map(u=>`<div class="fac-item" data-v="${{u.v}}" data-l="${{u.l}}">${{u.l}}</div>`).join('');
    drop.style.display='block';
    drop.querySelectorAll('.fac-item').forEach(el=>{{
      el.addEventListener('mousedown',function(e){{
        e.preventDefault(); select(this.dataset.v,this.dataset.l);
      }});
    }});
  }}
  input.addEventListener('input',function(){{
    const q=this.value.toLowerCase().trim();
    clear();
    if(!q){{ drop.style.display='none'; return; }}
    const hits=users.filter(u=>u.l.toLowerCase().includes(q)).slice(0,8);
    if(!hits.length){{ drop.style.display='none'; return; }}
    showDrop(hits);
  }});
  input.addEventListener('blur',function(){{ setTimeout(()=>drop.style.display='none',150); }});
  input.addEventListener('focus',function(){{
    const q=this.value.toLowerCase().trim();
    if(!q) showDrop(users);
    else this.dispatchEvent(new Event('input'));
  }});
}})();
</script>""" if not_friends else ""

    friends_section = f"""<div class="section-block">
  <div class="section-title">Friends</div>
  <div class="friend-chips">{friend_chips if friend_chips else '<span style="color:var(--muted);font-size:.85rem">No friends added yet.</span>'}</div>
  {add_form}
</div>"""

    # Build stat cards
    def _stat(label, value, sub=""):
        sub_html = f'<div class="sc-sub">{sub}</div>' if sub else ""
        return f'<div class="stat-card"><div class="sc-val">{value}</div><div class="sc-label">{label}</div>{sub_html}</div>'

    score_acc = f"{round(s_hit / s_pred * 100)}%" if s_pred else "—"
    winner_acc = f"{round(w_hit/w_pred*100)}%" if w_pred else "—"
    missed_stat = f'<span style="color:var(--danger)">{missed_count}</span>' if missed_count else "0"
    stats_html = (
        _stat("Points", total_pts, "all groups combined") +
        _stat("Exact Scores", exact_count, f"{score_acc} of predictions") +
        _stat("Winner Picks", f"{w_hit}/{w_pred}" if w_pred else "—", f"{winner_acc} correct") +
        _stat("Hot Streak", f"{streak}🔥" if streak >= 2 else (streak if streak else "—"), "scored matches in a row")
    )

    # Next match card
    def _match_countdown(kts):
        secs = max(0, int(kts - time.time()))
        if secs < 3600: return f"{secs // 60}m"
        if secs < 86400: return f"{secs // 3600}h {(secs % 3600) // 60}m"
        return f"{secs // 86400}d {(secs % 86400) // 3600}h"

    def _pred_url(m):
        kts = m["kickoff_ts"]
        return f"/predict/{_esc(m['slug'])}?th={_esc(m['team_home'])}&ta={_esc(m['team_away'])}&kts={int(kts)}&title={_esc(m['team_home']+' vs '+m['team_away'])}&tournament={_esc(m.get('tournament',''))}"

    if open_matches:
        m = open_matches[0]
        kts = m["kickoff_ts"]
        league_name = ALL_ESPN_LEAGUES.get(m.get("tournament",""), (None, ""))[1]
        more_html = ""
        if len(open_matches) > 1:
            more_html = f'<a href="/leaderboard" style="display:block;text-align:center;font-size:.82rem;color:var(--muted);margin-top:.5rem;padding:.4rem;background:var(--surface2);border-radius:8px">+ {len(open_matches)-1} more match{"es" if len(open_matches)-1!=1 else ""} to predict →</a>'
        next_match_html = f"""<div class="section-block">
  <div class="section-title">Predict Now</div>
  <a href="{_pred_url(m)}" class="next-match-card">
    <div class="nm-league">{_esc(league_name)}</div>
    <div class="nm-teams">{_esc(m['team_home'])} <span class="nm-vs">vs</span> {_esc(m['team_away'])}</div>
    <div class="nm-foot">
      <span class="nm-countdown">⏱ {_match_countdown(kts)} to kickoff</span>
      <span class="nm-cta">Predict →</span>
    </div>
  </a>
  {more_html}
</div>"""
    elif next_upcoming:
        kts = next_upcoming["kickoff_ts"]
        league_name = ALL_ESPN_LEAGUES.get(next_upcoming.get("tournament",""), (None, ""))[1]
        opens_in = _match_countdown(kts - 172800)
        next_match_html = f"""<div class="section-block">
  <div class="section-title">Coming Up</div>
  <div class="next-match-card" style="opacity:.75;cursor:default">
    <div class="nm-league">{_esc(league_name)}</div>
    <div class="nm-teams">{_esc(next_upcoming['team_home'])} <span class="nm-vs">vs</span> {_esc(next_upcoming['team_away'])}</div>
    <div class="nm-foot">
      <span class="nm-countdown">Window opens in {opens_in}</span>
      <span class="nm-cta" style="opacity:.5">Not yet</span>
    </div>
  </div>
</div>"""
    else:
        next_match_html = ""

    # Group position cards
    group_cards_html = ""
    for gs in group_stats:
        medal = _medal(gs["rank"], 32)
        role_pill = ' <span class="role-pill">Admin</span>' if gs["role"] == "admin" else ""
        group_cards_html += f"""<a href="/groups/{_esc(gs['slug'])}" class="group-pos-card">
  <div class="gpc-left">
    <div class="gpc-rank">{medal}</div>
    <div>
      <div class="gpc-name">{_esc(gs['name'])}{role_pill}</div>
      <div class="gpc-meta">{gs['pts']} pts · {gs['scored']} matches · {gs['exact_sc']} exact</div>
    </div>
  </div>
  <span class="material-symbols-outlined" style="color:var(--muted);font-size:1.1rem">chevron_right</span>
</a>"""

    # Recent predictions list
    recent_html = ""
    for r in recent:
        league_name = ALL_ESPN_LEAGUES.get(r["tournament"] or "", (None, r["tournament"] or ""))[1]
        if r["final_home"] is not None:
            pts_badge = f'<span class="rec-pts pts-{r["points"] or 0}">{r["points"] or 0}pts</span>'
            result_txt = f'{r["final_home"]}–{r["final_away"]}'
            exact_tick = ' ✓' if r["exact_score"] else ''
        else:
            pts_badge = '<span class="rec-pts pts-pending">—</span>'
            result_txt = "Pending"
            exact_tick = ""
        recent_html += f"""<div class="rec-row">
  <div class="rec-main">
    <div class="rec-match">{_esc(r['team_home'])} vs {_esc(r['team_away'])}</div>
    <div class="rec-meta">{_esc(league_name)} · Your pick: {r['score_home']}–{r['score_away']} · Result: {result_txt}{exact_tick}</div>
  </div>
  {pts_badge}
</div>"""

    # Pending invites section
    invite_html = ""
    if invites:
        invite_cards = ""
        for inv in invites:
            invite_cards += f"""<div class="invite-card">
  <div class="inv-info">
    <div class="inv-group">{_esc(inv['name'])}</div>
    <div class="inv-from">Invited by {_esc(_ucfirst(inv['created_by']))}</div>
  </div>
  <div class="inv-actions">
    <form method="post" action="/groups/invite/{_esc(inv['token'])}/accept" style="display:inline">
      <button class="btn btn-sm inv-accept">Accept</button>
    </form>
    <form method="post" action="/groups/invite/{_esc(inv['token'])}/decline" style="display:inline">
      <button class="btn btn-sm btn-ghost inv-decline">Decline</button>
    </form>
  </div>
</div>"""
        invite_html = f"""<div class="section-block">
  <div class="section-title">Pending Invites <span class="inv-count">{len(invites)}</span></div>
  {invite_cards}
</div>"""

    return HTMLResponse(f"""<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>Me · Scrum</title>
{_FONTS}{_PWA_META}<style>{_BASE_CSS}
.page-body{{max-width:600px;margin:0 auto;padding:1.25rem 1rem 5rem}}
.me-header{{display:flex;align-items:center;gap:.75rem;margin-bottom:1.5rem}}
.me-avatar{{width:48px;height:48px;border-radius:50%;background:var(--accent3);display:flex;align-items:center;justify-content:center;font-family:'Barlow Condensed',sans-serif;font-size:1.4rem;font-weight:700;color:#fff;flex-shrink:0}}
.me-name{{font-family:'Barlow Condensed',sans-serif;font-size:1.4rem;font-weight:700;letter-spacing:.03em}}
.me-sub{{font-size:.8rem;color:var(--muted)}}
.stats-grid{{display:grid;grid-template-columns:1fr 1fr;gap:.75rem;margin-bottom:1.25rem}}
.stat-card{{background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:1rem;text-align:center}}
.sc-val{{font-family:'Barlow Condensed',sans-serif;font-size:2rem;font-weight:700;color:var(--accent3)}}
.sc-label{{font-size:.75rem;font-weight:700;letter-spacing:.07em;text-transform:uppercase;color:var(--muted);margin-top:.1rem}}
.sc-sub{{font-size:.7rem;color:var(--muted);margin-top:.1rem}}
.section-block{{margin-bottom:1.25rem}}
.section-title{{font-family:'Barlow Condensed',sans-serif;font-size:.78rem;font-weight:700;letter-spacing:.1em;text-transform:uppercase;color:var(--muted);margin-bottom:.6rem;display:flex;align-items:center;gap:.5rem}}
.inv-count{{background:var(--accent3);color:#fff;font-size:.65rem;padding:.1rem .4rem;border-radius:99px}}
.group-pos-card{{display:flex;align-items:center;justify-content:space-between;background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:.9rem 1rem;margin-bottom:.6rem;transition:background .15s}}
.group-pos-card:active{{background:var(--surface2)}}
.gpc-left{{display:flex;align-items:center;gap:.75rem}}
.gpc-rank{{font-size:1.5rem;min-width:2rem;text-align:center}}
.gpc-name{{font-weight:600;font-size:.95rem}}
.gpc-meta{{font-size:.78rem;color:var(--muted);margin-top:.15rem}}
.role-pill{{font-size:.6rem;font-weight:700;text-transform:uppercase;letter-spacing:.06em;color:var(--accent3);border:1px solid var(--accent3);border-radius:4px;padding:.05rem .3rem;margin-left:.35rem;vertical-align:middle}}
.rec-row{{display:flex;align-items:center;justify-content:space-between;padding:.7rem 0;border-bottom:1px solid var(--border)}}
.rec-row:last-child{{border-bottom:none}}
.rec-main{{flex:1;min-width:0;padding-right:.75rem}}
.rec-match{{font-size:.9rem;font-weight:500;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}}
.rec-meta{{font-size:.75rem;color:var(--muted);margin-top:.15rem}}
.rec-pts{{font-family:'Barlow Condensed',sans-serif;font-size:.9rem;font-weight:700;letter-spacing:.04em;padding:.25rem .55rem;border-radius:6px;flex-shrink:0}}
.pts-5{{background:rgba(34,197,94,.15);color:#22c55e}}
.pts-3{{background:rgba(77,158,247,.12);color:var(--accent)}}
.pts-1{{background:rgba(136,150,176,.1);color:var(--muted)}}
.pts-0{{background:rgba(136,150,176,.1);color:var(--muted)}}
.pts-pending{{color:var(--muted)}}
.friend-chips{{display:flex;flex-wrap:wrap;gap:.5rem;margin-bottom:.75rem}}
.friend-chip{{display:flex;align-items:center;gap:.45rem;background:var(--surface2);border:1px solid var(--border);border-radius:99px;padding:.3rem .6rem .3rem .4rem}}
.fc-avatar{{width:22px;height:22px;border-radius:50%;background:var(--header);color:#fff;display:flex;align-items:center;justify-content:center;font-size:.65rem;font-weight:700;flex-shrink:0}}
.fc-name{{font-size:.82rem;font-weight:500}}
.fc-remove{{background:none;border:none;color:var(--muted);cursor:pointer;font-size:1rem;line-height:1;padding:0 .1rem;transition:color .15s}}.fc-remove:hover{{color:var(--danger)}}
.friend-add-form{{display:flex;gap:.5rem;align-items:flex-start;margin-top:.5rem}}
.fac-wrap{{position:relative;flex:1;overflow:visible}}
.fac-arrow{{position:absolute;right:.65rem;top:50%;transform:translateY(-50%);color:var(--muted);font-size:.85rem;pointer-events:none;line-height:1}}
.fac-dropdown{{display:none;position:fixed;background:#fff;border:1.5px solid var(--border);border-radius:8px;margin-top:2px;box-shadow:0 4px 20px rgba(0,0,0,.15);z-index:9999;min-width:200px;overflow:hidden}}
.fac-item{{padding:.7rem 1rem;font-size:.9rem;cursor:pointer;transition:background .1s}}
.fac-item:hover,.fac-item:active{{background:var(--surface2)}}
.next-match-card{{display:block;background:var(--header);color:#fff;border-radius:12px;padding:1rem 1.1rem;text-decoration:none;transition:opacity .15s}}
.next-match-card:active{{opacity:.85}}
.nm-league{{font-size:.72rem;font-weight:700;letter-spacing:.08em;text-transform:uppercase;opacity:.7;margin-bottom:.3rem}}
.nm-teams{{font-family:'Barlow Condensed',sans-serif;font-size:1.2rem;font-weight:700;letter-spacing:.03em;margin-bottom:.5rem}}
.nm-vs{{opacity:.6;font-size:1rem;margin:0 .35rem}}
.nm-foot{{display:flex;align-items:center;justify-content:space-between}}
.nm-countdown{{font-size:.82rem;opacity:.8}}
.nm-cta{{font-family:'Barlow Condensed',sans-serif;font-size:.85rem;font-weight:700;letter-spacing:.05em;background:rgba(255,255,255,.15);padding:.25rem .7rem;border-radius:6px}}
.acc-row{{display:flex;align-items:center;gap:.65rem;padding:.5rem 0;border-bottom:1px solid var(--border)}}
.acc-row:last-child{{border-bottom:none}}
.acc-label{{font-size:.82rem;font-weight:500;min-width:130px;flex-shrink:0}}
.acc-bar-wrap{{flex:1;height:6px;background:var(--surface2);border-radius:99px;overflow:hidden}}
.acc-bar{{height:100%;background:var(--accent3);border-radius:99px;transition:width .4s}}
.acc-nums{{font-size:.8rem;font-weight:600;color:var(--text);min-width:60px;text-align:right;flex-shrink:0}}
.acc-pct{{color:var(--muted);font-weight:400;font-size:.75rem}}
.invite-card{{background:var(--surface);border:1px solid rgba(232,103,28,.3);border-radius:12px;padding:.9rem 1rem;margin-bottom:.6rem;display:flex;align-items:center;justify-content:space-between;gap:.75rem;flex-wrap:wrap}}
.inv-info{{flex:1}}
.inv-group{{font-weight:600;font-size:.95rem}}
.inv-from{{font-size:.78rem;color:var(--muted);margin-top:.15rem}}
.inv-actions{{display:flex;gap:.5rem;flex-shrink:0}}
.inv-accept{{background:var(--accent3)}}
.inv-decline{{}}
</style></head><body>
{_nav(username, is_admin, "me")}
<div class="page-body">
  <div class="me-header">
    <div class="me-avatar-wrap" title="Change photo" style="cursor:pointer;position:relative;flex-shrink:0" onclick="document.getElementById('av-input').click()">
      {_avatar_el(username, 52)}
      <div style="position:absolute;bottom:0;right:0;background:var(--accent3);border-radius:50%;width:18px;height:18px;display:flex;align-items:center;justify-content:center">
        <span class="material-symbols-outlined" style="font-size:.7rem;color:#fff">photo_camera</span>
      </div>
    </div>
    <input id="av-input" type="file" accept="image/*" style="display:none" onchange="avOpenCrop(this.files[0])">
    <div id="av-modal" style="display:none;position:fixed;inset:0;background:rgba(0,0,0,.88);z-index:10000;flex-direction:column;align-items:center;justify-content:center;gap:.9rem">
      <div style="color:#fff;font-size:.82rem;opacity:.8">Drag to position · Pinch or scroll to zoom</div>
      <div id="av-vp" style="width:280px;height:280px;border-radius:50%;overflow:hidden;border:3px solid rgba(255,255,255,.8);position:relative;cursor:grab;touch-action:none;flex-shrink:0">
        <img id="av-img" style="position:absolute;transform-origin:0 0;pointer-events:none;user-select:none">
      </div>
      <input id="av-zoom" type="range" min="100" max="400" value="100" style="width:200px;accent-color:#22a84a" oninput="avSetScale(this.value/100)">
      <div style="display:flex;gap:.75rem">
        <button onclick="avCancel()" style="background:rgba(255,255,255,.15);border:none;color:#fff;padding:.55rem 1.2rem;border-radius:8px;font-size:.9rem;cursor:pointer">Cancel</button>
        <button onclick="avSave()" style="background:#22a84a;border:none;color:#fff;padding:.55rem 1.4rem;border-radius:8px;font-size:.9rem;font-weight:700;cursor:pointer">Save</button>
      </div>
    </div>
    <canvas id="av-canvas" style="display:none"></canvas>
    <script>
    (function(){{
      var VP=280, OUT=256;
      var img=document.getElementById('av-img');
      var modal=document.getElementById('av-modal');
      var vp=document.getElementById('av-vp');
      var zoomSlider=document.getElementById('av-zoom');
      var natW=0,natH=0,posX=0,posY=0,scale=1,minScale=1;
      var dragging=false,lastX=0,lastY=0,lastPinch=0;

      function clamp(){{
        var dw=natW*scale, dh=natH*scale;
        posX=Math.min(0,Math.max(VP-dw,posX));
        posY=Math.min(0,Math.max(VP-dh,posY));
      }}
      function render(){{
        img.style.left=posX+'px';
        img.style.top=posY+'px';
        img.style.width=(natW*scale)+'px';
        img.style.height=(natH*scale)+'px';
      }}
      window.avSetScale=function(s){{
        s=Math.max(minScale,Math.min(4,s));
        var cx=VP/2, cy=VP/2;
        var rx=(cx-posX)/(natW*scale), ry=(cy-posY)/(natH*scale);
        scale=s;
        posX=cx-rx*natW*scale;
        posY=cy-ry*natH*scale;
        clamp(); render();
        zoomSlider.value=Math.round(s*100);
      }};
      window.avOpenCrop=function(file){{
        if(!file)return;
        var url=URL.createObjectURL(file);
        img.onload=function(){{
          natW=img.naturalWidth; natH=img.naturalHeight;
          minScale=Math.max(VP/natW,VP/natH);
          scale=minScale;
          posX=(VP-natW*scale)/2; posY=(VP-natH*scale)/2;
          clamp(); render();
          zoomSlider.min=Math.round(minScale*100);
          zoomSlider.value=Math.round(scale*100);
          modal.style.display='flex';
        }};
        img.src=url;
      }};
      window.avCancel=function(){{
        modal.style.display='none';
        document.getElementById('av-input').value='';
      }};
      window.avSave=function(){{
        var c=document.getElementById('av-canvas');
        c.width=OUT; c.height=OUT;
        var ctx=c.getContext('2d');
        var srcX=(-posX)/scale, srcY=(-posY)/scale, srcS=VP/scale;
        ctx.drawImage(img,srcX,srcY,srcS,srcS,0,0,OUT,OUT);
        c.toBlob(function(blob){{
          var fd=new FormData(); fd.append('file',blob,'avatar.jpg');
          fetch('/me/avatar',{{method:'POST',body:fd}}).then(function(r){{
            if(r.ok)location.reload(); else alert('Upload failed, try a different photo.');
          }});
        }},'image/jpeg',0.88);
        modal.style.display='none';
      }};
      vp.addEventListener('mousedown',function(e){{dragging=true;lastX=e.clientX;lastY=e.clientY;vp.style.cursor='grabbing';}});
      window.addEventListener('mouseup',function(){{dragging=false;vp.style.cursor='grab';}});
      window.addEventListener('mousemove',function(e){{
        if(!dragging)return;
        posX+=e.clientX-lastX; posY+=e.clientY-lastY;
        lastX=e.clientX; lastY=e.clientY;
        clamp(); render();
      }});
      vp.addEventListener('wheel',function(e){{
        e.preventDefault();
        avSetScale(scale*(e.deltaY<0?1.1:0.9));
      }},{{passive:false}});
      vp.addEventListener('touchstart',function(e){{
        if(e.touches.length===1){{dragging=true;lastX=e.touches[0].clientX;lastY=e.touches[0].clientY;}}
        else if(e.touches.length===2){{dragging=false;lastPinch=Math.hypot(e.touches[0].clientX-e.touches[1].clientX,e.touches[0].clientY-e.touches[1].clientY);}}
        e.preventDefault();
      }},{{passive:false}});
      vp.addEventListener('touchmove',function(e){{
        if(e.touches.length===1&&dragging){{
          posX+=e.touches[0].clientX-lastX; posY+=e.touches[0].clientY-lastY;
          lastX=e.touches[0].clientX; lastY=e.touches[0].clientY;
          clamp(); render();
        }} else if(e.touches.length===2){{
          var d=Math.hypot(e.touches[0].clientX-e.touches[1].clientX,e.touches[0].clientY-e.touches[1].clientY);
          if(lastPinch)avSetScale(scale*d/lastPinch);
          lastPinch=d;
        }}
        e.preventDefault();
      }},{{passive:false}});
      vp.addEventListener('touchend',function(){{dragging=false;lastPinch=0;}});
    }})();
    </script>
    <div style="flex:1">
      <div class="me-name">{_esc(_ucfirst(username))}</div>
      <div class="me-sub">{'Site Admin · ' if is_admin else ''}Member since {datetime.fromtimestamp(user["created_at"]).strftime("%b %Y") if user else ""}</div>
    </div>
    <a href="/me/card" style="font-size:.75rem;color:var(--muted);display:flex;align-items:center;gap:.25rem;text-decoration:none;flex-shrink:0"><span class="material-symbols-outlined" style="font-size:1rem">share</span>Card</a>
  </div>
  {invite_html}
  {next_match_html}
  <div class="stats-grid">{stats_html}</div>
  {'<div style="text-align:center;font-size:.82rem;color:var(--danger);margin:-1rem 0 1rem">⚠ You missed '+str(missed_count)+' prediction window'+ ('s' if missed_count!=1 else '') +'</div>' if missed_count else ''}
  {accuracy_section}
  {friends_section}
  <div class="section-block">
    <div class="section-title">Your Groups</div>
    {group_cards_html if group_cards_html else '<div style="color:var(--muted);font-size:.88rem">Not in any groups yet.</div>'}
  </div>
  <div class="section-block">
    <div class="section-title">Recent Predictions</div>
    <div style="background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:.25rem 1rem">
      {recent_html if recent_html else '<div style="color:var(--muted);font-size:.88rem;padding:.75rem 0">No predictions yet.</div>'}
    </div>
  </div>
  <div class="section-block">
    <div class="section-title">Account</div>
    <div style="background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:.9rem 1rem;display:flex;flex-direction:column;gap:1rem">
      <form method="post" action="/me/username" style="display:flex;flex-direction:column;gap:.6rem">
        <div style="font-size:.8rem;font-weight:600;color:var(--muted);text-transform:uppercase;letter-spacing:.06em">Change Username</div>
        {'<div style="font-size:.82rem;color:var(--danger);margin:.2rem 0">'+({'taken':'That username is already taken — choose another.','invalid':'Only letters, numbers and _ . - allowed.','empty':'Username cannot be empty.','error':'Something went wrong — username not changed. Try again.'}.get(request.query_params.get('un',''),''))+'</div>' if request.query_params.get('un','') not in ('ok','') else ''}
        {'<div style="font-size:.82rem;color:var(--accent);margin:.2rem 0">Username updated ✓</div>' if request.query_params.get('un') == 'ok' else ''}
        <input type="text" name="new_username" placeholder="New username" value="{_esc(username)}" required autocomplete="username" autocapitalize="none" style="width:100%">
        <button type="submit" class="btn btn-sm btn-ghost" style="align-self:flex-start">Update Username</button>
      </form>
      <div style="border-top:1px solid var(--border);padding-top:1rem">
        <form method="post" action="/me/password" style="display:flex;flex-direction:column;gap:.6rem">
          <div style="font-size:.8rem;font-weight:600;color:var(--muted);text-transform:uppercase;letter-spacing:.06em">Change Password</div>
          {'<div style="font-size:.82rem;color:var(--danger)">'+({'wrong':'Current password incorrect.','short':'Password must be at least 6 characters.'}.get(request.query_params.get('pw',''),''))+'</div>' if request.query_params.get('pw','') not in ('ok','') else ''}
          {'<div style="font-size:.82rem;color:var(--accent)">Password updated ✓</div>' if request.query_params.get('pw') == 'ok' else ''}
          <input type="password" name="current_password" placeholder="Current password" required autocomplete="current-password" style="width:100%">
          <input type="password" name="new_password" placeholder="New password (6+ chars)" required autocomplete="new-password" style="width:100%">
          <button type="submit" class="btn btn-sm btn-ghost" style="align-self:flex-start">Update Password</button>
        </form>
      </div>
    </div>
  </div>
</div>
{_bnav("me", pending_invites, is_admin)}
</body></html>""")


# ── Shareable stats card ──────────────────────────────────────────────────────
@app.get("/me/card", response_class=HTMLResponse)
async def me_card(request: Request):
    username = _get_session_user(request)
    user = await _get_user(username)
    is_admin = bool(user and user["is_admin"])

    async with _db.execute(
        """SELECT COALESCE(SUM(points),0) as pts, COUNT(*) as matches,
                  COALESCE(SUM(exact_score),0) as exact_count
           FROM leaderboard WHERE username=? AND group_id=1""",
        (username,),
    ) as cur:
        s = dict(await cur.fetchone())

    async with _db.execute(
        "SELECT points FROM leaderboard WHERE username=? ORDER BY created_at DESC LIMIT 50",
        (username,),
    ) as cur:
        streak = 0
        for r in await cur.fetchall():
            if r["points"] > 1: streak += 1
            else: break

    acc = f"{round(s['exact_count']/s['matches']*100)}%" if s["matches"] else "—"

    # Best group position
    my_groups = await _get_user_groups(username)
    best_rank = None
    best_group = None
    for g in my_groups:
        if g["id"] == 1:
            continue
        async with _db.execute(
            """SELECT COUNT(*)+1 as rank FROM (
                   SELECT username, SUM(points) as tp FROM leaderboard
                   WHERE group_id=? GROUP BY username
                   HAVING tp > (SELECT COALESCE(SUM(points),0) FROM leaderboard WHERE group_id=? AND username=?)
               )""",
            (g["id"], g["id"], username),
        ) as cur:
            row = await cur.fetchone()
            rank = row["rank"] if row else 1
            if best_rank is None or rank < best_rank:
                best_rank = rank
                best_group = g["name"]

    rank_line = f'#{best_rank} in {_esc(best_group)}' if best_rank else f'#{len(my_groups)} group{"s" if len(my_groups)!=1 else ""}'
    medal = ["🥇","🥈","🥉"][best_rank-1] if best_rank and best_rank <= 3 else ""

    return HTMLResponse(f"""<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Stats Card · Scrum</title>{_FONTS}<style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{background:#f2f7f3;display:flex;flex-direction:column;align-items:center;justify-content:center;min-height:100vh;font-family:'Barlow',sans-serif;padding:1rem}}
.card{{background:#1f6e3a;color:#fff;border-radius:20px;padding:2rem 1.75rem;width:100%;max-width:360px;box-shadow:0 8px 40px rgba(0,0,0,.2)}}
.card-logo{{display:flex;justify-content:center;margin-bottom:1.5rem}}
.card-logo img{{height:36px;filter:brightness(0) invert(1)}}
.card-name{{font-family:'Barlow Condensed',sans-serif;font-size:1.6rem;font-weight:700;letter-spacing:.04em;text-align:center;margin-bottom:.25rem}}
.card-rank{{text-align:center;font-size:.85rem;opacity:.7;margin-bottom:1.75rem}}
.stats-grid{{display:grid;grid-template-columns:1fr 1fr;gap:.85rem;margin-bottom:1.5rem}}
.stat-box{{background:rgba(255,255,255,.12);border-radius:12px;padding:.85rem .75rem;text-align:center}}
.stat-val{{font-family:'Barlow Condensed',sans-serif;font-size:2rem;font-weight:700;line-height:1;color:#f5a623}}
.stat-lbl{{font-size:.7rem;font-weight:700;letter-spacing:.08em;text-transform:uppercase;opacity:.65;margin-top:.3rem}}
.card-footer{{text-align:center;font-size:.72rem;opacity:.45;letter-spacing:.06em;text-transform:uppercase}}
.back-btn{{margin-top:1.25rem;display:inline-block;background:var(--header,#1f6e3a);color:#fff;border:none;border-radius:8px;font-family:'Barlow Condensed',sans-serif;font-size:.85rem;font-weight:700;letter-spacing:.06em;text-transform:uppercase;padding:.5rem 1.1rem;cursor:pointer;text-decoration:none}}
</style></head><body>
<div class="card">
  <div class="card-logo"><img src="/static/wordmark.png" alt="Scrum"></div>
  <div style="display:flex;justify-content:center;margin-bottom:.75rem">
    {'<img src="/avatar/'+_esc(username)+'" style="width:72px;height:72px;border-radius:50%;object-fit:cover;border:3px solid rgba(255,255,255,.3)">' if _avatar_url(username) else '<div style="width:72px;height:72px;border-radius:50%;background:rgba(255,255,255,.15);display:flex;align-items:center;justify-content:center;font-family:Barlow Condensed,sans-serif;font-size:2rem;font-weight:700">'+_esc(username[0].upper())+'</div>'}
  </div>
  <div class="card-name">{medal} {_esc(_ucfirst(username))}</div>
  <div class="card-rank">{rank_line}</div>
  <div class="stats-grid">
    <div class="stat-box"><div class="stat-val">{s["pts"]}</div><div class="stat-lbl">Points</div></div>
    <div class="stat-box"><div class="stat-val">{s["matches"]}</div><div class="stat-lbl">Predictions</div></div>
    <div class="stat-box"><div class="stat-val">{s["exact_count"]}</div><div class="stat-lbl">Exact Scores</div></div>
    <div class="stat-box"><div class="stat-val">{streak}{"🔥" if streak>=2 else ""}</div><div class="stat-lbl">Streak</div></div>
  </div>
  <div class="card-footer">scrum · rugby predictions</div>
</div>
<div style="margin-top:1rem;font-size:.82rem;color:#5a7a62;text-align:center">Screenshot this card and share it 📲</div>
<a href="/me" class="back-btn">← Back to Me</a>
</body></html>""")


# ── Head-to-head ──────────────────────────────────────────────────────────────
@app.get("/h2h/{target}", response_class=HTMLResponse)
async def h2h_page(request: Request, target: str):
    username = _get_session_user(request)
    user = await _get_user(username)
    is_admin = bool(user and user["is_admin"])
    target_user = await _get_user(target)
    if not target_user:
        return HTMLResponse("<h2>User not found</h2>", status_code=404)

    # All matches both users predicted on
    async with _db.execute(
        """SELECT r.match_id, r.match_title, r.team_home, r.team_away,
                  r.tournament, r.kickoff_ts, r.final_home, r.final_away,
                  me.score_home as my_home, me.score_away as my_away,
                  them.score_home as th_home, them.score_away as th_away,
                  lme.points as my_pts, lthem.points as th_pts,
                  lme.exact_score as my_exact, lthem.exact_score as th_exact
           FROM match_results r
           JOIN predictions me ON me.match_id=r.match_id AND me.username=?
           JOIN predictions them ON them.match_id=r.match_id AND them.username=?
           LEFT JOIN leaderboard lme ON lme.match_id=r.match_id AND lme.username=? AND lme.group_id=1
           LEFT JOIN leaderboard lthem ON lthem.match_id=r.match_id AND lthem.username=? AND lthem.group_id=1
           ORDER BY r.kickoff_ts DESC""",
        (username, target, username, target),
    ) as cur:
        shared = [dict(r) for r in await cur.fetchall()]

    my_wins = them_wins = draws = my_total = them_total = 0
    for m in shared:
        mp = m["my_pts"] or 0
        tp = m["th_pts"] or 0
        my_total += mp
        them_total += tp
        if mp > tp: my_wins += 1
        elif tp > mp: them_wins += 1
        else: draws += 1

    summary_html = f"""<div class="h2h-summary">
  <div class="h2h-side">
    <div style="display:flex;justify-content:center;margin-bottom:.5rem">{_avatar_el(username, 52)}</div>
    <div class="h2h-name">{_esc(_ucfirst(username))}</div>
    <div class="h2h-wins">{my_wins}</div>
    <div class="h2h-pts">{my_total} pts</div>
  </div>
  <div class="h2h-mid">
    <div class="h2h-mid-label">Head to Head</div>
    <div class="h2h-draws">{draws} draw{"s" if draws!=1 else ""}</div>
    <div class="h2h-played">{len(shared)} matches</div>
  </div>
  <div class="h2h-side h2h-right">
    <div style="display:flex;justify-content:flex-end;margin-bottom:.5rem">{_avatar_el(target, 52)}</div>
    <div class="h2h-name">{_esc(_ucfirst(target))}</div>
    <div class="h2h-wins">{them_wins}</div>
    <div class="h2h-pts">{them_total} pts</div>
  </div>
</div>"""

    match_rows = ""
    for m in shared:
        mp = m["my_pts"] or 0
        tp = m["th_pts"] or 0
        me_win = "h2h-win" if mp > tp else ("h2h-draw" if mp == tp else "")
        th_win = "h2h-win" if tp > mp else ("h2h-draw" if tp == mp else "")
        league_name = ALL_ESPN_LEAGUES.get(m["tournament"] or "", (None, ""))[1]
        kick_str = f'<span class="fix-kick" data-ts="{int(m["kickoff_ts"])}" data-fmt="date"></span>' if m["kickoff_ts"] else ""
        exact_me = " ✓" if m["my_exact"] else ""
        exact_th = " ✓" if m["th_exact"] else ""
        match_rows += f"""<div class="h2h-row">
  <div class="h2h-cell {me_win}">
    <div class="h2h-pred">{m['my_home']}–{m['my_away']}{exact_me}</div>
    <div class="h2h-rowpts">{mp} pts</div>
  </div>
  <div class="h2h-match-info">
    <div class="h2h-match-title">{_esc(m['team_home'])} {m['final_home']}–{m['final_away']} {_esc(m['team_away'])}</div>
    <div class="h2h-match-meta">{_esc(league_name)} · {kick_str}</div>
  </div>
  <div class="h2h-cell h2h-cell-r {th_win}">
    <div class="h2h-pred">{m['th_home']}–{m['th_away']}{exact_th}</div>
    <div class="h2h-rowpts">{tp} pts</div>
  </div>
</div>"""

    if not match_rows:
        match_rows = '<div style="color:var(--muted);font-size:.9rem;text-align:center;padding:2rem 0">No shared predictions yet.</div>'

    return HTMLResponse(f"""<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{_esc(_ucfirst(username))} vs {_esc(_ucfirst(target))} · Scrum</title>
{_FONTS}{_PWA_META}<style>{_BASE_CSS}
.page-body{{max-width:640px;margin:0 auto;padding:1.25rem 1rem 5rem}}
.page-title{{font-family:'Barlow Condensed',sans-serif;font-size:1.4rem;font-weight:700;letter-spacing:.04em;text-transform:uppercase;margin-bottom:1.1rem}}
.h2h-summary{{display:grid;grid-template-columns:1fr auto 1fr;background:var(--header);color:#fff;border-radius:14px;padding:1.25rem 1rem;margin-bottom:1.25rem;text-align:center;gap:.5rem}}
.h2h-name{{font-size:.8rem;opacity:.7;font-weight:600;letter-spacing:.04em;text-transform:uppercase;margin-bottom:.25rem}}
.h2h-wins{{font-family:'Barlow Condensed',sans-serif;font-size:2.5rem;font-weight:700;line-height:1}}
.h2h-pts{{font-size:.78rem;opacity:.65;margin-top:.2rem}}
.h2h-mid{{display:flex;flex-direction:column;align-items:center;justify-content:center;gap:.2rem}}
.h2h-mid-label{{font-size:.68rem;font-weight:700;letter-spacing:.08em;text-transform:uppercase;opacity:.6}}
.h2h-draws{{font-family:'Barlow Condensed',sans-serif;font-size:1.1rem;font-weight:700}}
.h2h-played{{font-size:.72rem;opacity:.6}}
.h2h-right{{text-align:right}}
.h2h-row{{display:grid;grid-template-columns:1fr 1.6fr 1fr;gap:.5rem;background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:.65rem .75rem;margin-bottom:.5rem;align-items:center}}
.h2h-cell{{text-align:left}}
.h2h-cell-r{{text-align:right}}
.h2h-pred{{font-weight:600;font-size:.88rem}}
.h2h-rowpts{{font-size:.75rem;color:var(--muted);margin-top:.1rem}}
.h2h-win .h2h-pred{{color:var(--header)}}
.h2h-win .h2h-rowpts{{color:var(--accent)}}
.h2h-draw .h2h-pred{{color:var(--accent3)}}
.h2h-match-info{{text-align:center}}
.h2h-match-title{{font-size:.8rem;font-weight:600;line-height:1.3}}
.h2h-match-meta{{font-size:.72rem;color:var(--muted);margin-top:.15rem}}
</style></head><body>
{_nav(username, is_admin, "me")}
<div class="page-body">
  <div class="page-title">{_esc(_ucfirst(username))} vs {_esc(_ucfirst(target))}</div>
  {summary_html}
  {match_rows}
</div>
{_bnav("me", 0, is_admin)}
<script>
document.querySelectorAll('.fix-kick[data-ts]').forEach(el=>{{
  const ts=parseInt(el.dataset.ts)*1000;
  if(!ts)return;
  const d=new Date(ts);
  el.textContent=d.toLocaleDateString('en-GB',{{day:'numeric',month:'short'}});
}});
</script>
</body></html>""")


# ── Friends ───────────────────────────────────────────────────────────────────
@app.post("/friends/add")
async def friends_add(request: Request, target: str = Form(...)):
    username = _get_session_user(request)
    target = target.strip()
    if target and target != username:
        async with _db.execute("SELECT username FROM users WHERE username=?", (target,)) as cur:
            if await cur.fetchone():
                await _db.execute(
                    "INSERT OR IGNORE INTO friends (username,friend_username,created_at) VALUES(?,?,?)",
                    (username, target, time.time()),
                )
                await _db.commit()
    return RedirectResponse(url="/me", status_code=303)


@app.post("/friends/remove")
async def friends_remove(request: Request, target: str = Form(...)):
    username = _get_session_user(request)
    target = target.strip()
    if target:
        await _db.execute(
            "DELETE FROM friends WHERE username=? AND friend_username=?", (username, target)
        )
        await _db.commit()
    return RedirectResponse(url="/me", status_code=303)


# ── Groups listing ────────────────────────────────────────────────────────────
@app.get("/groups", response_class=HTMLResponse)
async def groups_page(request: Request):
    username = _get_session_user(request)
    user = await _get_user(username)
    is_admin = user and user["is_admin"]
    my_groups = await _get_user_groups(username)

    pending_invites = await _pending_invite_count(username)

    cards = ""
    for g in my_groups:
        async with _db.execute(
            "SELECT COUNT(*) as n FROM group_members WHERE group_id=?", (g["id"],)
        ) as cur:
            member_count = (await cur.fetchone())["n"]
        role_badge = '<span style="color:var(--accent3);font-size:.7rem;font-weight:700;text-transform:uppercase;letter-spacing:.06em">Admin</span>' if g["role"] == "admin" else ""
        desc = f'<div style="color:var(--muted);font-size:.85rem;margin:.3rem 0 .6rem">{_esc(g["description"] or "")}</div>' if g["description"] else '<div style="margin:.4rem 0"></div>'
        cards += f"""<a href="/groups/{_esc(g['slug'])}" class="group-card">
  <div class="gc-header">
    <div class="gc-name">{_esc(g['name'])}</div>
    {role_badge}
  </div>
  {desc}
  <div class="gc-meta">{member_count} member{"s" if member_count != 1 else ""}</div>
</a>"""

    create_btn = '<a href="/groups/create" class="btn" style="display:inline-flex;align-items:center;gap:.4rem"><span class="material-symbols-outlined" style="font-size:1rem">add</span>Create Group</a>'

    invite_banner = ""
    if pending_invites:
        invite_banner = f'<a href="/me" style="display:flex;align-items:center;gap:.6rem;background:rgba(232,103,28,.08);border:1px solid rgba(232,103,28,.3);border-radius:10px;padding:.75rem 1rem;margin-bottom:1rem;font-size:.9rem;color:var(--text)"><span class="material-symbols-outlined" style="color:var(--accent3)">mail</span>You have <strong style="color:var(--accent3);margin:0 .25rem">{pending_invites}</strong> pending group invite{"s" if pending_invites != 1 else ""} — tap to view</a>'

    return HTMLResponse(f"""<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>Groups · Scrum</title>
{_FONTS}{_PWA_META}<style>{_BASE_CSS}
.page-body{{max-width:600px;margin:0 auto;padding:1.25rem 1rem 5rem}}
.page-title{{font-family:'Barlow Condensed',sans-serif;font-size:1.5rem;font-weight:700;letter-spacing:.04em;text-transform:uppercase;margin-bottom:1rem;display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:.75rem}}
.group-card{{display:block;background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:1rem 1.1rem;margin-bottom:.6rem;transition:border-color .15s,background .15s}}
.group-card:active{{background:var(--surface2)}}
.gc-header{{display:flex;align-items:center;justify-content:space-between;gap:.5rem}}
.gc-name{{font-family:'Barlow Condensed',sans-serif;font-size:1.1rem;font-weight:700;letter-spacing:.03em}}
.gc-meta{{font-size:.8rem;color:var(--muted)}}
.empty{{text-align:center;color:var(--muted);padding:3rem 1rem;font-size:.95rem}}
</style></head><body>
{_nav(username, is_admin, "groups")}
<div class="page-body">
  {invite_banner}
  <div class="page-title">
    <span>Your Groups</span>
    {create_btn}
  </div>
  {''.join(cards) if cards else '<div class="empty">You\'re not in any groups yet.</div>'}
</div>
{_bnav("groups", pending_invites, is_admin)}
</body></html>""")


# ── Create group ──────────────────────────────────────────────────────────────
@app.get("/groups/create", response_class=HTMLResponse)
async def groups_create_get(request: Request):
    username = _get_session_user(request)
    user = await _get_user(username)
    is_admin = user and user["is_admin"]

    league_options = ""
    for slug, (_, label) in ALL_ESPN_LEAGUES.items():
        checked = "checked" if slug in ESPN_LEAGUES else ""
        league_options += f'<label class="check-row"><input type="checkbox" name="leagues" value="{slug}" {checked}><span>{label}</span></label>'

    pred_rows = ""
    for pt_key, pt_label, pt_desc, resolve in PREDICTION_TYPES:
        if resolve == "auto":
            badge = '<span class="res-badge res-auto">Auto-resolves</span>'
        else:
            badge = '<span class="res-badge res-manual">May need manual entry</span>'
        pred_rows += f"""<label class="check-row">
  <input type="checkbox" name="pred_types" value="{pt_key}" checked>
  <span>
    <strong>{pt_label}</strong> {badge}
    <span class="pred-desc">{pt_desc}</span>
  </span>
</label>"""

    return HTMLResponse(f"""<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>Create Group · Scrum</title>
{_FONTS}{_PWA_META}<style>{_BASE_CSS}
.page-body{{max-width:620px;margin:0 auto;padding:2rem 1.5rem}}
.page-title{{font-family:'Barlow Condensed',sans-serif;font-size:1.6rem;font-weight:700;letter-spacing:.04em;text-transform:uppercase;margin-bottom:1.5rem}}
.form-section{{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:1.25rem;margin-bottom:1rem}}
.section-label{{font-family:'Barlow Condensed',sans-serif;font-size:.8rem;font-weight:700;letter-spacing:.1em;text-transform:uppercase;color:var(--muted);margin-bottom:.9rem}}
.field{{display:flex;flex-direction:column;gap:.4rem;margin-bottom:.9rem}}
.field label{{font-size:.85rem;color:var(--muted)}}
.field input,.field textarea{{width:100%}}
.field textarea{{resize:vertical;min-height:60px;padding:.55rem .85rem}}
.check-row{{display:flex;align-items:flex-start;gap:.6rem;padding:.5rem 0;border-bottom:1px solid var(--border);cursor:pointer;line-height:1.4}}
.check-row:last-child{{border-bottom:none}}
.check-row input[type=checkbox]{{margin-top:.2rem;accent-color:var(--accent3);width:16px;height:16px;flex-shrink:0}}
.check-row span{{font-size:.88rem}}
.pred-desc{{display:block;color:var(--muted);font-size:.78rem;margin-top:.15rem}}
.res-badge{{display:inline-block;font-size:.65rem;font-weight:700;letter-spacing:.07em;text-transform:uppercase;padding:.1rem .4rem;border-radius:4px;vertical-align:middle;margin-left:.35rem}}
.res-auto{{background:rgba(34,197,94,.12);color:#22c55e;border:1px solid rgba(34,197,94,.25)}}
.res-manual{{background:rgba(251,191,36,.1);color:#f59e0b;border:1px solid rgba(251,191,36,.25)}}
.actions{{display:flex;gap:.75rem;margin-top:1.25rem}}
</style></head><body>
{_nav(username, is_admin, "groups")}
<div class="page-body">
  <div class="page-title">Create a Group</div>
  <form method="post" action="/groups/create">
    <div class="form-section">
      <div class="section-label">Group Details</div>
      <div class="field"><label>Group Name</label><input type="text" name="name" required maxlength="60" placeholder="e.g. Family League 2025"></div>
      <div class="field"><label>Description (optional)</label><textarea name="description" placeholder="What's this group about?"></textarea></div>
    </div>
    <div class="form-section">
      <div class="section-label">Competitions to Follow</div>
      {league_options}
    </div>
    <div class="form-section">
      <div class="section-label">Prediction Types</div>
      {pred_rows}
    </div>
    <div class="actions">
      <button type="submit" class="btn">Create Group</button>
      <a href="/groups" class="btn btn-ghost">Cancel</a>
    </div>
  </form>
</div>
{_bnav("groups", 0, is_admin)}
</body></html>""")


@app.post("/groups/create", response_class=HTMLResponse)
async def groups_create_post(request: Request):
    username = _get_session_user(request)
    form = await request.form()
    name = (form.get("name") or "").strip()
    description = (form.get("description") or "").strip()
    leagues = form.getlist("leagues")
    pred_types = form.getlist("pred_types")

    if not name:
        return RedirectResponse(url="/groups/create", status_code=303)

    slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")[:60]
    base_slug = slug
    i = 2
    while True:
        async with _db.execute("SELECT id FROM groups WHERE slug=?", (slug,)) as cur:
            if not await cur.fetchone():
                break
        slug = f"{base_slug}-{i}"
        i += 1

    await _db.execute(
        "INSERT INTO groups (name,slug,created_by,created_at,description) VALUES(?,?,?,?,?)",
        (name, slug, username, time.time(), description or None),
    )
    await _db.commit()
    async with _db.execute("SELECT id FROM groups WHERE slug=?", (slug,)) as cur:
        gid = (await cur.fetchone())["id"]

    await _db.execute(
        "INSERT OR IGNORE INTO group_members (group_id,username,role,joined_at) VALUES(?,?,?,?)",
        (gid, username, "admin", time.time()),
    )
    for ls in leagues:
        if ls in ALL_ESPN_LEAGUES:
            await _db.execute(
                "INSERT OR IGNORE INTO group_leagues (group_id,league_slug) VALUES(?,?)", (gid, ls)
            )
    for pt in pred_types:
        if pt in {k for k, *_ in PREDICTION_TYPES}:
            await _db.execute(
                "INSERT OR IGNORE INTO group_prediction_types (group_id,prediction_type) VALUES(?,?)", (gid, pt)
            )
    await _db.commit()
    return RedirectResponse(url=f"/groups/{slug}", status_code=303)


# ── Group home page ────────────────────────────────────────────────────────────
@app.get("/groups/{slug}", response_class=HTMLResponse)
async def group_home(request: Request, slug: str):
    username = _get_session_user(request)
    user = await _get_user(username)
    is_admin = user and user["is_admin"]
    g = await _get_group(slug)
    if not g:
        return HTMLResponse("<h2>Group not found</h2>", status_code=404)
    role = await _group_member_role(g["id"], username)
    if not role:
        return HTMLResponse("<h2>You are not a member of this group.</h2>", status_code=403)
    is_group_admin = role == "admin"

    leagues = await _group_leagues(g["id"])
    pred_types = await _group_pred_types(g["id"])

    # Members + full points breakdown
    async with _db.execute(
        """SELECT gm.username, gm.role,
                  COALESCE(SUM(l.points), 0)      as total_points,
                  COUNT(l.id)                      as played,
                  COALESCE(SUM(l.exact_score), 0)  as exact_count,
                  COALESCE(SUM(l.pts_score), 0)    as pts_score,
                  COALESCE(SUM(l.pts_winner), 0)   as pts_winner,
                  COALESCE(SUM(l.pts_margin), 0)   as pts_margin,
                  COALESCE(SUM(l.pts_btts), 0)     as pts_btts,
                  COALESCE(SUM(l.pts_try_any), 0)  as pts_try_any,
                  COALESCE(SUM(l.pts_try_first), 0) as pts_try_first,
                  COALESCE(SUM(l.pts_motm), 0)     as pts_motm,
                  COALESCE(SUM(l.pts_banker), 0)   as pts_banker,
                  COALESCE(SUM(l.diff), 999999)     as total_diff
           FROM group_members gm
           LEFT JOIN leaderboard l ON l.group_id=? AND l.username=gm.username
           WHERE gm.group_id=?
           GROUP BY gm.username
           ORDER BY total_points DESC, exact_count DESC, total_diff ASC, gm.username""",
        (g["id"], g["id"]),
    ) as cur:
        members = [dict(r) for r in await cur.fetchall()]

    # Custom competitions for this group
    custom_comps = await _group_custom_comps(g["id"])
    custom_match_ids: dict[str, int] = await _all_group_custom_matches(g["id"])  # {match_id: comp_id}

    # Upcoming fixtures: league matches + custom competition matches
    espn_upcoming = []
    try:
        all_up = await _fetch_espn_upcoming()
        now_ts = time.time()
        seen_ids: set[str] = set()
        for m in all_up:
            kts = m.get("kickoff_ts") or 0
            if kts and now_ts >= kts:
                continue
            m_slug = m.get("slug") or re.sub(r"[^a-z0-9]+", "-", f"{m['team_home']}-vs-{m['team_away']}".lower()).strip("-")
            m["slug"] = m_slug
            in_league = m["tournament"] in leagues
            in_custom = m_slug in custom_match_ids
            if not in_league and not in_custom:
                continue
            m["_custom"] = in_custom and not in_league
            seen_ids.add(m_slug)
            espn_upcoming.append(m)
        # Add custom comp matches not yet in ESPN window
        for match_id in custom_match_ids:
            if match_id not in seen_ids:
                comp_id = custom_match_ids[match_id]
                for cc in custom_comps:
                    if cc["id"] == comp_id:
                        for cm in await _custom_comp_matches(comp_id):
                            if cm["match_id"] == match_id and cm["kickoff_ts"] > now_ts:
                                espn_upcoming.append({
                                    "tournament": cm["tournament"],
                                    "tournament_name": ALL_ESPN_LEAGUES.get(cm["tournament"], (None, cm["tournament"]))[1],
                                    "espn_id": cm["espn_id"] or "", "league_id": cm["league_id"],
                                    "team_home": cm["team_home"], "team_away": cm["team_away"],
                                    "kickoff_ts": cm["kickoff_ts"], "in_progress": False,
                                    "slug": cm["match_id"], "_custom": True,
                                })
        espn_upcoming.sort(key=lambda m: m.get("kickoff_ts") or 0)
    except Exception as exc:
        logger.warning("Group ESPN upcoming: %s", exc)

    # Which matches the user has already predicted (in this group)
    espn_slugs = [
        m.get("slug") or re.sub(r"[^a-z0-9]+", "-", f"{m['team_home']}-vs-{m['team_away']}".lower()).strip("-")
        for m in espn_upcoming
    ]
    user_pred_slugs: set[str] = set()
    if espn_slugs:
        ph = ",".join("?" * len(espn_slugs))
        async with _db.execute(
            f"SELECT match_id FROM predictions WHERE username=? AND match_id IN ({ph})",
            [username] + espn_slugs,
        ) as cur:
            user_pred_slugs = {r["match_id"] for r in await cur.fetchall()}

    now_ts = time.time()

    def _fix_card(m: dict, pred_url_extra: str = "") -> str:
        kts = m["kickoff_ts"]
        th, ta = m["team_home"], m["team_away"]
        t_slug = m.get("tournament", "")
        m_slug = m.get("slug") or re.sub(r"[^a-z0-9]+", "-", f"{th}-vs-{ta}".lower()).strip("-")
        extra = pred_url_extra or f"&group={_esc(slug)}"
        pred_url = f"/predict/{_esc(m_slug)}?th={_esc(th)}&ta={_esc(ta)}&kts={int(kts)}&title={_esc(th+' vs '+ta)}&tournament={_esc(t_slug)}{extra}"
        t_str = f'<span class="fix-kick" data-ts="{kts}"></span>'
        window_open = (not kts) or (kts - 172800 <= now_ts < kts)
        already = m_slug in user_pred_slugs
        if already:
            status = f'<a class="fix-status fix-done" href="{pred_url}">Predicted ✓</a>'
        elif kts and now_ts < kts - 172800:
            status = f'<span class="fix-status fix-soon">Opens {_time_until(kts - 172800)}</span>'
        elif m.get("in_progress"):
            status = f'<a class="fix-status fix-live" href="{pred_url}">Live · Predict</a>'
        elif window_open:
            status = f'<a class="fix-status fix-open" href="{pred_url}">Predict</a>'
        else:
            status = f'<a class="fix-status fix-closed" href="{pred_url}">View</a>'
        return (f'<div class="fix-row">'
                f'<span class="fix-teams">{_esc(th)} <span class="fix-vs">vs</span> {_esc(ta)}</span>'
                f'<span class="fix-foot">{t_str}{status}</span>'
                f'</div>')

    def _sb(label: str, collapsible: bool = False, collapsed: bool = False, star: bool = False,
             admin_btns: str = "", sub: bool = False) -> str:
        """Render a section-banner. sub=True uses the blue accent for sub-section headers."""
        icon = "★ " if star else ""
        sub_cls = " sub" if sub else ""
        if collapsible:
            col_cls = " collapsed" if collapsed else ""
            return (f'<div class="section-banner{sub_cls} collapsible{col_cls}" onclick="toggleSection(this)">'
                    f'{icon}{_esc(label)}'
                    f'{"" if not admin_btns else f"<span>{admin_btns}</span>"}'
                    f'<span class="material-symbols-outlined sb-chevron">expand_more</span></div>')
        return f'<div class="section-banner{sub_cls}">{icon}{_esc(label)}</div>'

    # ── Build per-league sections matching predictions page structure ──────────
    all_sections_html = ""
    shown_slugs: set[str] = set()
    def _lb_table(rows: list, gid_str: str, show_cols: dict) -> str:
        """Render simple+detail standings table matching predictions page style."""
        if not rows:
            return '<div class="empty-msg">No results yet — standings appear once the first match resolves.</div>'
        has_winner = show_cols.get("winner") and any(r.get("pts_winner") for r in rows)
        has_margin = show_cols.get("margin") and any(r.get("pts_margin") for r in rows)
        has_btts   = show_cols.get("btts")   and any(r.get("pts_btts") for r in rows)
        has_try    = (show_cols.get("try_anytime") or show_cols.get("try_first")) and any((r.get("pts_try_any",0) or 0)+(r.get("pts_try_first",0) or 0) for r in rows)
        has_banker = any(r.get("pts_banker") for r in rows)
        simple_s = detail_s = ""
        for i, r in enumerate(rows, 1):
            medal = _medal(i, 22)
            you = ' class="lb-you"' if r["username"] == username else ""
            role_dot = '<span style="display:inline-block;width:6px;height:6px;border-radius:50%;background:var(--accent3);margin-left:.3rem;vertical-align:middle"></span>' if r.get("role") == "admin" else ""
            avg = f'{r["total_points"]/r["played"]:.1f}' if r.get("played") else "—"
            name_cell = f'{_avatar_el(r["username"],20)} <span>{_esc(_ucfirst(r["username"]))}{role_dot}</span>'
            simple_s += (f'<tr{you}><td class="lb-rank">{medal}</td>'
                         f'<td class="lb-name">{name_cell}</td>'
                         f'<td class="lb-num">{r.get("played",0)}</td>'
                         f'<td class="lb-num">{r.get("exact_count",0) or r.get("exact_scores",0)}</td>'
                         f'<td class="lb-num" style="font-size:.78rem">{avg}</td>'
                         f'<td class="lb-total">{r["total_points"]}</td></tr>')
            opt = ""
            if has_winner: opt += f'<td class="lb-num">{r.get("pts_winner",0) or 0}</td>'
            if has_margin: opt += f'<td class="lb-num">{r.get("pts_margin",0) or 0}</td>'
            if has_btts:   opt += f'<td class="lb-num">{r.get("pts_btts",0) or 0}</td>'
            if has_try:    opt += f'<td class="lb-num">{(r.get("pts_try_any",0) or 0)+(r.get("pts_try_first",0) or 0)}</td>'
            bkr = r.get("pts_banker", 0) or 0
            if has_banker: opt += f'<td class="lb-num" style="color:var(--accent3)">+{bkr}</td>'
            detail_s += (f'<tr{you}><td class="lb-rank">{medal}</td>'
                         f'<td class="lb-name">{name_cell}</td>'
                         f'<td class="lb-num">{r.get("played",0)}</td>'
                         f'<td class="lb-num">{r.get("exact_count",0) or r.get("exact_scores",0)}</td>'
                         f'<td class="lb-num">{r.get("pts_score",0) or 0}</td>{opt}'
                         f'<td class="lb-num" style="font-size:.78rem">{avg}</td>'
                         f'<td class="lb-total">{r["total_points"]}</td></tr>')
        opt_hdr = ""
        if has_winner: opt_hdr += '<th class="lb-num">Win</th>'
        if has_margin: opt_hdr += '<th class="lb-num">Mrg</th>'
        if has_btts:   opt_hdr += '<th class="lb-num">BTS</th>'
        if has_try:    opt_hdr += '<th class="lb-num">Try</th>'
        if has_banker: opt_hdr += '<th class="lb-num">🔒</th>'
        sid = f"lbs-{gid_str}"
        did = f"lbd-{gid_str}"
        return f"""<div style="display:flex;justify-content:flex-end;margin-bottom:.4rem">
  <button class="lb-toggle-btn" onclick="const s=document.getElementById('{sid}'),d=document.getElementById('{did}'),o=d.style.display!=='none';d.style.display=o?'none':'block';s.style.display=o?'block':'none';this.textContent=o?'Full breakdown ▾':'Simple view ▴'">Full breakdown ▾</button>
</div>
<div id="{sid}"><div class="lb-scroll"><table class="lb-table"><thead><tr>
  <th class="lb-rank"></th><th style="text-align:left">Player</th>
  <th class="lb-num">P</th><th class="lb-num">Exact</th><th class="lb-num">Avg</th><th class="lb-total">Pts</th>
</tr></thead><tbody>{simple_s}</tbody></table></div></div>
<div id="{did}" style="display:none"><div class="lb-scroll"><table class="lb-table"><thead><tr>
  <th class="lb-rank"></th><th style="text-align:left">Player</th>
  <th class="lb-num">P</th><th class="lb-num">✓</th><th class="lb-num">Score</th>{opt_hdr}
  <th class="lb-num">Avg</th><th class="lb-total">Pts</th>
</tr></thead><tbody>{detail_s}</tbody></table></div></div>"""

    show_cols = {pt: pt in pred_types for pt in ("winner","margin","btts","try_anytime","try_first")}

    for league_slug in leagues:
        league_name = ALL_ESPN_LEAGUES.get(league_slug, (None, league_slug))[1]

        # 1. Upcoming fixtures for this league
        lm = []
        for m in espn_upcoming:
            ms = m.get("slug") or re.sub(r"[^a-z0-9]+", "-", f"{m['team_home']}-vs-{m['team_away']}".lower()).strip("-")
            if m.get("tournament") == league_slug and ms not in shown_slugs:
                shown_slugs.add(ms)
                lm.append(m)
        fix_cards = "".join(_fix_card(m) for m in lm) if lm else '<div class="empty-msg" style="padding:.75rem">No upcoming fixtures.</div>'
        upcoming_section = f'<div class="page-section"><div class="section-banner sub collapsible" onclick="toggleSection(this)">Upcoming Fixtures<span class="material-symbols-outlined sb-chevron">expand_more</span></div><div class="collapsible-body"><div class="fix-section">{fix_cards}</div></div></div>'

        # 2. Standings for this group + league
        async with _db.execute(
            """SELECT gm.username, gm.role,
                      COALESCE(SUM(l.points),0)     as total_points,
                      COUNT(l.id)                    as played,
                      COALESCE(SUM(l.exact_score),0) as exact_count,
                      COALESCE(SUM(l.pts_score),0)   as pts_score,
                      COALESCE(SUM(l.pts_winner),0)  as pts_winner,
                      COALESCE(SUM(l.pts_margin),0)  as pts_margin,
                      COALESCE(SUM(l.pts_btts),0)    as pts_btts,
                      COALESCE(SUM(l.pts_try_any),0) as pts_try_any,
                      COALESCE(SUM(l.pts_try_first),0) as pts_try_first,
                      COALESCE(SUM(l.pts_motm),0)    as pts_motm,
                      COALESCE(SUM(l.pts_banker),0)  as pts_banker,
                      COALESCE(AVG(l.diff),0)        as avg_diff
               FROM group_members gm
               LEFT JOIN leaderboard l ON l.group_id=? AND l.username=gm.username AND l.tournament=?
               WHERE gm.group_id=?
               GROUP BY gm.username ORDER BY total_points DESC, exact_count DESC, COALESCE(SUM(l.diff),999999) ASC""",
            (g["id"], league_slug, g["id"]),
        ) as cur:
            lb_rows = [dict(r) for r in await cur.fetchall()]
        standings_section = f"""<div class="page-section">
  {_sb("Standings", collapsible=True, collapsed=True, sub=True)}
  <div class="collapsible-body collapsed">{_lb_table(lb_rows, f"{g['id']}-{league_slug}", show_cols)}</div>
</div>"""

        # 3. Awaiting results — group members predicted, no result yet
        async with _db.execute(
            """SELECT DISTINCT p.match_id, p.match_title, p.kickoff_ts,
                      COUNT(p.id) as pred_count
               FROM predictions p
               JOIN group_members gm ON p.username=gm.username AND gm.group_id=?
               LEFT JOIN match_results r ON p.match_id=r.match_id
               WHERE r.match_id IS NULL AND p.tournament=?
               GROUP BY p.match_id ORDER BY p.kickoff_ts DESC""",
            (g["id"], league_slug),
        ) as cur:
            awaiting = [dict(r) for r in await cur.fetchall()]
        awaiting_html = ""
        for a in awaiting:
            dt = datetime.fromtimestamp(a["kickoff_ts"], tz=timezone.utc).strftime("%d %b") if a["kickoff_ts"] else "—"
            mid_esc = _esc(a["match_id"])
            awaiting_html += (
                f'<div class="match-result-row" data-mid="{mid_esc}" onclick="togglePreds(this)">'
                f'<span class="mr-date">{dt}</span>'
                f'<span class="mr-title">{_esc(a["match_title"])}</span>'
                f'<span class="mr-score" style="color:var(--muted);font-size:.8rem">Pending</span>'
                f'<span class="mr-cnt">{a["pred_count"]} pick{"s" if a["pred_count"] != 1 else ""}</span>'
                f'<span class="mr-chevron material-symbols-outlined">chevron_right</span>'
                f'</div>'
                f'<div class="mr-breakdown" id="bd-{mid_esc}"></div>'
            )
        _aw_empty = '<div class="empty-msg">All caught up — no pending results.</div>'
        awaiting_section = (f'<div class="page-section">'
                            f'{_sb("Awaiting Results", collapsible=True, collapsed=True, sub=True)}'
                            f'<div class="collapsible-body collapsed">'
                            f'<div class="results-section">{awaiting_html}</div>'
                            f'</div></div>') if awaiting else ""

        # 4. Recent results for this group + league
        async with _db.execute(
            """SELECT r.match_id, r.match_title, r.final_home, r.final_away, r.kickoff_ts,
                      COUNT(p.id) as pred_count
               FROM match_results r
               JOIN predictions p ON r.match_id=p.match_id
               JOIN group_members gm ON p.username=gm.username AND gm.group_id=?
               WHERE r.tournament=?
               GROUP BY r.match_id ORDER BY r.kickoff_ts DESC LIMIT 10""",
            (g["id"], league_slug),
        ) as cur:
            recent = [dict(r) for r in await cur.fetchall()]
        m_rows = ""
        for m in recent:
            dt = datetime.fromtimestamp(m["kickoff_ts"], tz=timezone.utc).strftime("%d %b %Y") if m["kickoff_ts"] else ""
            mid = _esc(m["match_id"])
            m_rows += (f'<div class="match-result-row" data-mid="{mid}" onclick="togglePreds(this)">'
                       f'<span class="mr-date">{dt}</span>'
                       f'<span class="mr-title">{_esc(m["match_title"])}</span>'
                       f'<span class="mr-score">{m["final_home"]}—{m["final_away"]}</span>'
                       f'<span class="mr-cnt">{m["pred_count"]} pick{"s" if m["pred_count"]!=1 else ""}</span>'
                       f'<span class="mr-chevron material-symbols-outlined">chevron_right</span>'
                       f'</div><div class="mr-breakdown" id="bd-{mid}"></div>')
        recent_section = (f'<div class="page-section">'
                          f'{_sb("Recent Results", collapsible=True, collapsed=True, sub=True)}'
                          f'<div class="collapsible-body collapsed">'
                          f'<div class="results-section">{m_rows}</div>'
                          f'</div></div>') if m_rows else ""

        # Wrap all 4 sub-sections inside a collapsible league block
        inner = upcoming_section + standings_section + awaiting_section + recent_section
        all_sections_html += f"""<div class="page-section league-block">
  {_sb(league_name, collapsible=True, collapsed=True)}
  <div class="collapsible-body collapsed" style="padding-top:.5rem">{inner}</div>
</div>"""

    # Custom competitions — same structure
    for cc in custom_comps:
        cc_matches_all = await _custom_comp_matches(cc["id"])  # fetch once, reuse below
        cc_match_ids = [cm["match_id"] for cm in cc_matches_all]
        cc_fix_matches = []
        for cm in cc_matches_all:
            if cm["kickoff_ts"] <= now_ts:
                continue
            found = next((m for m in espn_upcoming if m.get("slug") == cm["match_id"]), None)
            cc_fix_matches.append(found or {"team_home": cm["team_home"], "team_away": cm["team_away"],
                "kickoff_ts": cm["kickoff_ts"], "tournament": cm["tournament"],
                "slug": cm["match_id"], "in_progress": False})
        cc_fix_html = "".join(_fix_card(m) for m in cc_fix_matches) if cc_fix_matches else '<div class="empty-msg" style="padding:.75rem">No upcoming fixtures.</div>'
        cc_upcoming = f'<div class="page-section"><div class="section-banner sub collapsible" onclick="toggleSection(this)">Upcoming Fixtures<span class="material-symbols-outlined sb-chevron">expand_more</span></div><div class="collapsible-body"><div class="fix-section">{cc_fix_html}</div></div></div>'
        if cc_match_ids:
            ph = ",".join("?" * len(cc_match_ids))
            async with _db.execute(
                f"""SELECT gm.username, gm.role,
                           COALESCE(SUM(l.points),0) as total_points, COUNT(l.id) as played,
                           COALESCE(SUM(l.exact_score),0) as exact_count,
                           COALESCE(SUM(l.pts_score),0) as pts_score, COALESCE(SUM(l.pts_winner),0) as pts_winner,
                           COALESCE(SUM(l.pts_margin),0) as pts_margin, COALESCE(SUM(l.pts_btts),0) as pts_btts,
                           COALESCE(SUM(l.pts_try_any),0) as pts_try_any, COALESCE(SUM(l.pts_try_first),0) as pts_try_first,
                           COALESCE(SUM(l.pts_motm),0) as pts_motm, COALESCE(SUM(l.pts_banker),0) as pts_banker
                    FROM group_members gm LEFT JOIN leaderboard l ON l.group_id=? AND l.username=gm.username AND l.match_id IN ({ph})
                    WHERE gm.group_id=? GROUP BY gm.username ORDER BY total_points DESC, exact_count DESC""",
                [g["id"]] + cc_match_ids + [g["id"]],
            ) as cur:
                cc_rows = [dict(r) for r in await cur.fetchall()]
        else:
            cc_rows = list(members)
        admin_btn_html = ""
        if is_group_admin:
            _cc_del_confirm = json.dumps(f"Delete {cc['name']}?")
            admin_btn_html = (f'<a href="/groups/{_esc(slug)}/competitions/{cc["id"]}/matches" class="btn btn-sm btn-ghost" style="font-size:.7rem">Edit</a>'
                              f'<form method="post" action="/groups/{_esc(slug)}/competitions/{cc["id"]}/delete" style="display:inline" onsubmit="return confirm({_cc_del_confirm})">'
                              f'<button class="btn btn-sm btn-danger" style="font-size:.7rem">Delete</button></form>')
        cc_standings = f"""<div class="page-section">
  {_sb("Standings", collapsible=True, collapsed=True, sub=True)}
  <div class="collapsible-body collapsed">{_lb_table(cc_rows, f"cc-{cc['id']}", show_cols)}</div>
</div>"""
        cc_inner = cc_upcoming + cc_standings
        all_sections_html += f"""<div class="page-section league-block">
  {_sb("★ " + cc["name"], collapsible=True, collapsed=True)}
  <div class="collapsible-body collapsed" style="padding-top:.5rem">
    {cc_inner}
    {f'<div style="display:flex;gap:.5rem;margin-top:.5rem">{admin_btn_html}</div>' if admin_btn_html else ""}
  </div>
</div>"""

    # Overall combined standings
    overall_table = _lb_table(members, f"overall-{g['id']}", show_cols)
    overall_section = f"""<div class="page-section">
  {_sb("Overall Standings", collapsible=True, collapsed=False)}
  <div class="collapsible-body">{overall_table}</div>
</div>"""

    # Admin controls
    admin_section = ""
    if is_group_admin:
        admin_section = f"""<div style="margin-top:1.5rem;padding-top:1rem;border-top:1px solid var(--border);display:flex;gap:.75rem;flex-wrap:wrap">
  <a href="/groups/{_esc(slug)}/invite" class="btn btn-sm">Invite People</a>
  <a href="/groups/{_esc(slug)}/settings" class="btn btn-sm btn-ghost">Settings</a>
</div>"""

    type_labels = " · ".join(PRED_LABEL.get(pt, pt) for pt in pred_types if pt != "score")
    pred_types_line = f'<div style="color:var(--muted);font-size:.8rem;margin-bottom:1.25rem">Scoring: Score Prediction{(", " + type_labels) if type_labels else ""}</div>' if pred_types else ""

    return HTMLResponse(f"""<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>{_esc(g['name'])} · Scrum</title>
{_FONTS}{_PWA_META}<style>{_BASE_CSS}
.page-main{{max-width:760px;margin:0 auto;padding:1.25rem 1rem 5rem}}
.page-title{{font-family:'Barlow Condensed',sans-serif;font-size:1.7rem;font-weight:800;letter-spacing:.04em;text-transform:uppercase;margin-bottom:.15rem}}
.section-banner{{font-family:'Barlow Condensed',sans-serif;font-size:1rem;font-weight:800;letter-spacing:.1em;text-transform:uppercase;color:var(--accent3);border-left:3px solid var(--accent3);padding:.1rem 0 .1rem .75rem;margin-bottom:.85rem;width:100%;box-sizing:border-box}}
.section-banner.sub{{color:var(--accent);border-left-color:var(--accent);font-size:.85rem;font-weight:700;margin-bottom:.65rem}}
.section-banner.collapsible{{cursor:pointer;display:flex;align-items:center;justify-content:space-between;user-select:none;padding-right:.5rem}}
.section-banner.collapsible:hover{{opacity:.85}}
.section-banner .sb-chevron{{font-size:.9rem;transition:transform .2s;opacity:.6}}
.section-banner.collapsible.collapsed .sb-chevron{{transform:rotate(-90deg)}}
.collapsible-body{{overflow:hidden}}
.collapsible-body.collapsed{{display:none}}
.page-section{{margin-bottom:1.5rem}}
.league-block .page-section{{margin-bottom:1rem}}
.league-block .collapsible-body{{padding-top:.25rem}}
.fix-section{{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:.85rem 1rem}}
.fix-group{{margin-bottom:.9rem}}.fix-group:last-child{{margin-bottom:0}}
.fix-t-name{{font-family:'Barlow Condensed',sans-serif;font-size:.72rem;font-weight:700;letter-spacing:.08em;text-transform:uppercase;color:var(--accent);margin-bottom:.3rem}}
.fix-row{{display:flex;align-items:center;gap:.5rem;padding:.5rem .6rem;border-radius:5px;background:var(--surface2);margin-bottom:.3rem;font-size:.83rem}}
.fix-teams{{flex:1;font-family:'Barlow Condensed',sans-serif;font-weight:600;text-align:center}}
.fix-vs{{color:var(--muted);font-weight:400;margin:0 .2rem}}
.fix-foot{{display:flex;align-items:center;gap:.4rem;flex-shrink:0}}
@media(max-width:600px){{.fix-row{{flex-direction:column;align-items:stretch;gap:.3rem}}.fix-foot{{justify-content:space-between}}}}
.fix-kick{{font-size:.7rem;color:var(--muted);white-space:nowrap}}
.fix-status{{font-family:'Barlow Condensed',sans-serif;font-size:.65rem;font-weight:700;letter-spacing:.05em;text-transform:uppercase;padding:.1rem .35rem;border-radius:3px}}
.fix-open{{background:rgba(77,158,247,.12);color:var(--accent)}}
.fix-live{{background:rgba(34,197,94,.12);color:var(--live)}}
.fix-done{{background:rgba(34,197,94,.08);color:var(--live)}}
.fix-soon,.fix-closed{{color:var(--muted)}}
.lb-scroll{{overflow-x:auto;-webkit-overflow-scrolling:touch}}
.lb-table{{width:100%;border-collapse:collapse;font-size:.84rem;white-space:nowrap}}
.lb-table thead th{{font-family:'Barlow Condensed',sans-serif;font-size:.72rem;font-weight:700;letter-spacing:.07em;text-transform:uppercase;color:var(--muted);padding:.45rem .5rem;text-align:center;border-bottom:2px solid var(--border);background:var(--surface2)}}
.lb-table td{{padding:.6rem .5rem;border-bottom:1px solid var(--border)}}
.lb-table tbody tr:hover td{{background:var(--surface2)}}
.lb-table tbody tr:last-child td{{border-bottom:none}}
.lb-you td{{background:rgba(31,110,58,.06)!important;font-weight:600}}
.lb-rank{{width:2rem;text-align:center}}
.lb-name{{text-align:left;display:flex;align-items:center;gap:.4rem}}
.lb-num{{text-align:center;color:var(--muted)}}
.lb-total{{text-align:center;font-family:'Barlow Condensed',sans-serif;font-size:1.05rem;font-weight:700;color:var(--accent3)}}
.lb-toggle-btn{{background:none;border:1px solid var(--border);color:var(--muted);font-family:'Barlow Condensed',sans-serif;font-size:.7rem;font-weight:700;letter-spacing:.06em;text-transform:uppercase;padding:.2rem .55rem;border-radius:5px;cursor:pointer}}
.empty-msg{{color:var(--muted);font-size:.88rem;padding:1rem;text-align:center;background:var(--surface);border-radius:8px;border:1px solid var(--border)}}
.my-preds-wrap{{background:var(--surface);border:1px solid var(--border);border-radius:8px;overflow:hidden;margin-bottom:.5rem}}
.my-preds-hdr{{font-family:'Barlow Condensed',sans-serif;font-size:.78rem;font-weight:700;letter-spacing:.07em;text-transform:uppercase;color:var(--muted);padding:.55rem .9rem}}
.results-section{{margin-top:.25rem}}
.match-result-row{{display:flex;align-items:center;gap:.75rem;padding:.6rem .75rem;background:var(--surface);border-radius:6px;margin-bottom:.3rem;border:1px solid var(--border);font-size:.85rem;cursor:pointer;user-select:none;transition:background .15s}}
.match-result-row:hover{{background:var(--surface2)}}
.match-result-row.open{{border-bottom-left-radius:0;border-bottom-right-radius:0;border-bottom-color:transparent;border-color:rgba(0,176,255,.4)}}
.mr-date{{color:var(--muted);font-size:.72rem;min-width:58px;flex-shrink:0}}
.mr-title{{flex:1;font-family:'Barlow Condensed',sans-serif;font-weight:600}}
.mr-score{{font-family:'Barlow Condensed',sans-serif;font-size:1rem;font-weight:700;color:var(--accent)}}
.mr-cnt{{font-size:.72rem;color:var(--muted);white-space:nowrap}}
.mr-chevron{{color:var(--accent);font-size:1rem;transition:transform .2s;flex-shrink:0;opacity:.7}}
.match-result-row.open .mr-chevron{{transform:rotate(90deg);opacity:1}}
.mr-breakdown{{display:none;border:1px solid var(--border);border-top:none;border-radius:0 0 6px 6px;margin-bottom:.3rem}}
.mr-breakdown.open{{display:block}}
.mr-bd-row{{display:flex;align-items:center;gap:.75rem;padding:.5rem .75rem;border-bottom:1px solid var(--border);font-size:.84rem}}.mr-bd-row:last-child{{border-bottom:none}}
.mr-bd-row.me{{background:rgba(0,176,255,.04)}}
.mr-bd-name{{font-family:'Barlow Condensed',sans-serif;font-weight:600;flex:1}}
.mr-bd-pred{{font-family:'Barlow Condensed',sans-serif;font-weight:700;color:var(--accent3);min-width:50px;text-align:right}}
.mr-bd-diff{{font-size:.75rem;color:var(--muted);min-width:45px;text-align:right}}
.mr-bd-pts{{font-family:'Barlow Condensed',sans-serif;font-weight:700;font-size:.9rem;min-width:38px;text-align:right}}
.mr-bd-loading{{padding:.75rem;color:var(--muted);font-size:.83rem;text-align:center}}
</style></head><body>
{_nav(username, is_admin, "groups")}
<div class="page-main">
  <div class="page-title">{_esc(g['name'])}</div>
  {'<div style="color:var(--muted);font-size:.88rem;margin-bottom:.5rem">'+_esc(g['description'])+'</div>' if g['description'] else ''}
  {pred_types_line}
  {all_sections_html}
  {overall_section}
  {admin_section}
</div>
{_bnav("groups", 0, is_admin)}
<script>
document.querySelectorAll('.fix-kick[data-ts]').forEach(el=>{{
  const ts=parseInt(el.dataset.ts)*1000;if(!ts)return;
  const d=new Date(ts);
  el.textContent=d.toLocaleDateString('en-GB',{{weekday:'short',month:'short',day:'numeric'}})+' '+d.toLocaleTimeString('en-GB',{{hour:'2-digit',minute:'2-digit'}});
}});
function toggleSection(banner){{
  banner.classList.toggle('collapsed');
  const body=banner.nextElementSibling;
  if(body&&body.classList.contains('collapsible-body'))body.classList.toggle('collapsed');
}}
const _predCache={{}};
async function togglePreds(row){{
  const mid=row.dataset.mid,bd=document.getElementById('bd-'+mid);
  if(row.classList.contains('open')){{row.classList.remove('open');bd.classList.remove('open');return;}}
  row.classList.add('open');bd.classList.add('open');
  if(_predCache[mid]){{bd.innerHTML=_predCache[mid];return;}}
  bd.innerHTML='<div class="mr-bd-loading">Loading…</div>';
  try{{
    const r=await fetch('/api/match-predictions/'+encodeURIComponent(mid));
    const d=await r.json();
    if(!d.preds||!d.preds.length){{bd.innerHTML='<div class="mr-bd-loading">No predictions recorded.</div>';return;}}
    let html='';
    d.preds.forEach(p=>{{
      const isMe=p.username===d.me,exact=p.exact_score?'<span class="mr-bd-exact" style="font-size:.65rem;font-weight:700;color:var(--accent3);background:rgba(255,145,0,.12);border:1px solid rgba(255,145,0,.25);border-radius:3px;padding:.05rem .25rem;margin-left:.25rem">EXACT</span>':'';
      html+=`<div class="mr-bd-row${{isMe?' me':''}}"><span class="mr-bd-name">${{p.username.charAt(0).toUpperCase()+p.username.slice(1)}}${{exact}}</span><span class="mr-bd-pred">${{p.score_home}}–${{p.score_away}}</span><span class="mr-bd-diff">${{p.diff!=null?'diff '+p.diff:'—'}}</span><span class="mr-bd-pts">${{p.points!=null?p.points+'pts':'—'}}</span></div>`;
    }});
    if(d.result&&(d.result.winner||d.result.try_scorers?.length)){{
      const r=d.result;
      const winLabel=r.winner?r.winner.charAt(0).toUpperCase()+r.winner.slice(1):'—';
      const bttsLabel=r.btts!=null?(r.btts?'Yes':'No'):null;
      html+='<div class="mr-result-block">';
      html+='<div class="mr-result-hdr">Match Result</div>';
      const basic=[['Winner',winLabel],['Margin',r.margin||null],['Both Teams Scored',bttsLabel],['First Try Scorer',r.first_try||null]];
      basic.forEach(([l,v])=>{{if(v)html+=`<div class="mr-result-row"><span class="mr-result-label">${{l}}</span><span class="mr-result-val">${{v}}</span></div>`;}});
      if(r.try_details&&r.try_details.length){{
        const byTeam={{}};
        r.try_details.forEach(s=>{{
          if(!byTeam[s.team])byTeam[s.team]=[];
          byTeam[s.team].push(s.clock?`${{s.name}} (${{s.clock}})`:s.name);
        }});
        Object.entries(byTeam).forEach(([team,scorers])=>{{
          html+=`<div class="mr-result-row"><span class="mr-result-label">${{team}}</span><span class="mr-result-val">${{scorers.join(', ')}}</span></div>`;
        }});
      }}else if(r.try_scorers&&r.try_scorers.length){{
        html+=`<div class="mr-result-row"><span class="mr-result-label">Try Scorers</span><span class="mr-result-val">${{r.try_scorers.join(', ')}}</span></div>`;
      }}
      html+='</div>';
    }}
    _predCache[mid]=html;bd.innerHTML=html;
  }}catch(e){{bd.innerHTML='<div class="mr-bd-loading">Failed to load.</div>';}}
}}
</script>
</body></html>""")


# ── Group invite — user picker ────────────────────────────────────────────────
@app.get("/groups/{slug}/invite", response_class=HTMLResponse)
async def group_invite_get(request: Request, slug: str, sent: str = "", link: str = ""):
    username = _get_session_user(request)
    user = await _get_user(username)
    is_admin = user and user["is_admin"]
    g = await _get_group(slug)
    if not g:
        return HTMLResponse("<h2>Group not found</h2>", status_code=404)
    role = await _group_member_role(g["id"], username)
    if role != "admin":
        return RedirectResponse(url=f"/groups/{slug}", status_code=303)

    # All registered users not already in this group and not already pending invite
    async with _db.execute(
        """SELECT u.username FROM users u
           WHERE u.username NOT IN (SELECT username FROM group_members WHERE group_id=?)
             AND u.username NOT IN (
               SELECT invited_username FROM group_invites
               WHERE group_id=? AND used_at IS NULL AND invited_username IS NOT NULL
             )
           ORDER BY u.username""",
        (g["id"], g["id"]),
    ) as cur:
        available = [r["username"] for r in await cur.fetchall()]

    # Already pending invites
    async with _db.execute(
        """SELECT invited_username, created_at FROM group_invites
           WHERE group_id=? AND used_at IS NULL AND invited_username IS NOT NULL
           ORDER BY created_at DESC""",
        (g["id"],),
    ) as cur:
        pending = [dict(r) for r in await cur.fetchall()]

    sent_banner = '<div style="background:rgba(34,197,94,.12);border:1px solid rgba(34,197,94,.25);border-radius:8px;padding:.7rem 1rem;color:#22a84a;font-size:.88rem;margin-bottom:1rem">Invites sent ✓</div>' if sent else ""

    my_friends = set(await _get_friends(username))
    friends_available  = [u for u in available if u in my_friends]
    others_available   = [u for u in available if u not in my_friends]

    def _user_row(u):
        return f"""<label class="user-pick-row">
  <input type="checkbox" name="users" value="{_esc(u)}" class="upick-cb">
  <div class="upick-avatar">{_esc(u[0].upper())}</div>
  <span class="upick-name">{_esc(_ucfirst(u))}</span>
</label>"""

    user_rows = ""
    if friends_available:
        user_rows += f'<div class="invite-group-label">Friends</div>'
        user_rows += "".join(_user_row(u) for u in friends_available)
    if others_available:
        label = "Other Players" if friends_available else "Players"
        user_rows += f'<div class="invite-group-label" style="margin-top:.75rem">{label}</div>'
        user_rows += "".join(_user_row(u) for u in others_available)

    pending_rows = ""
    for p in pending:
        pending_rows += f'<div class="pending-row"><span class="material-symbols-outlined" style="font-size:.9rem;color:var(--muted)">schedule</span> {_esc(_ucfirst(p["invited_username"]))} — waiting for response</div>'

    return HTMLResponse(f"""<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>Invite · {_esc(g['name'])}</title>
{_FONTS}{_PWA_META}<style>{_BASE_CSS}
.page-body{{max-width:560px;margin:0 auto;padding:1.25rem 1rem 5rem}}
.page-title{{font-family:'Barlow Condensed',sans-serif;font-size:1.5rem;font-weight:700;letter-spacing:.04em;text-transform:uppercase;margin-bottom:1.1rem;display:flex;align-items:center;gap:.75rem}}
.back-btn{{color:var(--muted);font-size:.85rem;display:flex;align-items:center;gap:.25rem;margin-bottom:1rem}}
.section-label{{font-family:'Barlow Condensed',sans-serif;font-size:.75rem;font-weight:700;letter-spacing:.1em;text-transform:uppercase;color:var(--muted);margin-bottom:.6rem}}
.user-pick-row{{display:flex;align-items:center;gap:.85rem;padding:.85rem .9rem;background:var(--surface);border:1px solid var(--border);border-radius:12px;margin-bottom:.5rem;cursor:pointer;transition:border-color .15s}}
.user-pick-row:active{{background:var(--surface2)}}
.upick-cb{{width:20px;height:20px;accent-color:var(--accent3);flex-shrink:0;cursor:pointer}}
.upick-avatar{{width:36px;height:36px;border-radius:50%;background:var(--surface2);display:flex;align-items:center;justify-content:center;font-family:'Barlow Condensed',sans-serif;font-size:1rem;font-weight:700;flex-shrink:0}}
.upick-name{{font-size:.95rem;font-weight:500}}
.send-btn{{width:100%;font-size:1rem;padding:.85rem;margin-top:.75rem;display:none}}
.invite-group-label{{font-family:'Barlow Condensed',sans-serif;font-size:.72rem;font-weight:700;letter-spacing:.1em;text-transform:uppercase;color:var(--muted);margin-bottom:.35rem}}
.pending-row{{font-size:.82rem;color:var(--muted);display:flex;align-items:center;gap:.4rem;padding:.45rem 0;border-bottom:1px solid var(--border)}}
.pending-row:last-child{{border-bottom:none}}
.pending-section{{background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:.5rem .9rem;margin-bottom:1rem}}
.empty-msg{{color:var(--muted);font-size:.88rem;padding:.5rem 0}}
</style></head><body>
{_nav(username, is_admin, "groups")}
<div class="page-body">
  <a href="/groups/{_esc(slug)}" class="back-btn"><span class="material-symbols-outlined" style="font-size:1rem">arrow_back</span> {_esc(g['name'])}</a>
  <div class="page-title">Invite People</div>
  {sent_banner}
  <div class="section-label">Shareable Link</div>
  <div style="background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:1rem;margin-bottom:1rem">
    <div style="font-size:.85rem;color:var(--muted);margin-bottom:.75rem">Anyone with this link can create an account and join. Share it on WhatsApp or wherever.</div>
    {'<div style="background:var(--surface2);border:1px solid var(--border);border-radius:8px;padding:.65rem .9rem;font-size:.82rem;word-break:break-all;color:var(--header);margin-bottom:.65rem">'+_esc(link)+'</div><button class="btn btn-sm" onclick="navigator.clipboard.writeText(\''+_esc(link)+'\').then(()=>this.textContent=\'Copied!\')">Copy Link</button>' if link else f'<form method="post" action="/groups/{_esc(slug)}/invite/link" style="display:flex;gap:.5rem;align-items:center;flex-wrap:wrap"><select name="expiry_days" style="flex:1"><option value="0">No expiry</option><option value="7">Expires in 7 days</option><option value="14">Expires in 14 days</option><option value="30">Expires in 30 days</option></select><button type="submit" class="btn btn-sm">Generate Link</button></form>'}
  </div>
  {'<div class="section-label">Pending Invites</div><div class="pending-section">' + pending_rows + '</div>' if pending_rows else ''}
  <div class="section-label" style="margin-top:.5rem">Notify Existing Users</div>
  <div style="font-size:.82rem;color:var(--muted);margin-bottom:.6rem">Send an in-app notification to people who already have an account.</div>
  <form method="post" action="/groups/{_esc(slug)}/invite">
    {user_rows if user_rows else '<div class="empty-msg">All registered users are already in this group or have a pending invite.</div>'}
    {'<button type="submit" class="btn send-btn" id="send-btn">Send Invites</button>' if user_rows else ''}
  </form>
</div>
{_bnav("groups", 0, is_admin)}
<script>
document.querySelectorAll('.upick-cb').forEach(cb=>{{
  cb.addEventListener('change',()=>{{
    const any=document.querySelector('.upick-cb:checked');
    document.getElementById('send-btn').style.display=any?'block':'none';
  }});
}});
</script>
</body></html>""")


@app.post("/groups/{slug}/invite/link")
async def group_invite_link_post(request: Request, slug: str):
    """Generate a multi-use shareable link for self-registration."""
    username = _get_session_user(request)
    g = await _get_group(slug)
    if not g:
        return HTMLResponse("<h2>Group not found</h2>", status_code=404)
    role = await _group_member_role(g["id"], username)
    if role != "admin":
        return RedirectResponse(url=f"/groups/{slug}", status_code=303)
    token = secrets.token_urlsafe(24)
    form = await request.form()
    expiry_days = int(form.get("expiry_days") or 0)
    expires_at = time.time() + expiry_days * 86400 if expiry_days else None
    await _db.execute(
        """INSERT INTO group_invites
           (token,group_id,created_by,created_at,invited_username,max_uses,use_count,expires_at)
           VALUES(?,?,?,?,NULL,50,0,?)""",
        (token, g["id"], username, time.time(), expires_at),
    )
    await _db.commit()
    base = _base_url(request)
    link = f"{base}/groups/join/{token}"
    return RedirectResponse(url=f"/groups/{slug}/invite?link={link}", status_code=303)


@app.post("/groups/{slug}/invite")
async def group_invite_post(request: Request, slug: str):
    username = _get_session_user(request)
    g = await _get_group(slug)
    if not g:
        return HTMLResponse("<h2>Group not found</h2>", status_code=404)
    role = await _group_member_role(g["id"], username)
    if role != "admin":
        return RedirectResponse(url=f"/groups/{slug}", status_code=303)

    form = await request.form()
    users = form.getlist("users")
    for u in users:
        u = u.strip()
        if not u:
            continue
        async with _db.execute("SELECT username FROM users WHERE username=?", (u,)) as cur:
            if not await cur.fetchone():
                continue
        if await _group_member_role(g["id"], u):
            continue
        token = secrets.token_urlsafe(24)
        await _db.execute(
            """INSERT OR IGNORE INTO group_invites
               (token,group_id,created_by,created_at,invited_username)
               VALUES(?,?,?,?,?)""",
            (token, g["id"], username, time.time(), u),
        )
    await _db.commit()
    return RedirectResponse(url=f"/groups/{slug}/invite?sent=1", status_code=303)


# ── Accept / Decline invite ───────────────────────────────────────────────────
@app.post("/groups/invite/{token}/accept")
async def invite_accept(request: Request, token: str):
    username = _get_session_user(request)
    async with _db.execute(
        "SELECT gi.*, g.slug FROM group_invites gi JOIN groups g ON gi.group_id=g.id WHERE gi.token=?",
        (token,),
    ) as cur:
        inv = await cur.fetchone()
    if not inv or inv["used_at"]:
        return RedirectResponse(url="/me", status_code=303)
    inv = dict(inv)
    if inv.get("invited_username") and inv["invited_username"] != username:
        return RedirectResponse(url="/me", status_code=303)
    await _db.execute(
        "INSERT OR IGNORE INTO group_members (group_id,username,role,joined_at) VALUES(?,?,?,?)",
        (inv["group_id"], username, "member", time.time()),
    )
    await _db.execute(
        "UPDATE group_invites SET used_by=?, used_at=? WHERE token=?",
        (username, time.time(), token),
    )
    await _db.commit()
    return RedirectResponse(url=f"/groups/{inv['slug']}", status_code=303)


@app.post("/groups/invite/{token}/decline")
async def invite_decline(request: Request, token: str):
    username = _get_session_user(request)
    async with _db.execute(
        "SELECT invited_username FROM group_invites WHERE token=?", (token,)
    ) as cur:
        inv = await cur.fetchone()
    if inv and (not inv["invited_username"] or inv["invited_username"] == username):
        await _db.execute(
            "UPDATE group_invites SET used_by=?, used_at=? WHERE token=?",
            (username, time.time(), token),
        )
        await _db.commit()
    return RedirectResponse(url="/me", status_code=303)


# ── Group join via shareable link (open to non-logged-in users) ───────────────
def _join_register_page(token: str, group_name: str, error: str = "") -> str:
    err = f'<div style="color:var(--danger);font-size:.85rem;margin-top:.25rem">{_esc(error)}</div>' if error else ""
    return f"""<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Join {_esc(group_name)} · Scrum</title>{_FONTS}{_PWA_META}<style>{_BASE_CSS}
.wrap{{display:flex;align-items:center;justify-content:center;min-height:100vh;padding:1.5rem}}
.box{{background:var(--surface);border:1px solid var(--border);border-radius:16px;padding:2rem 1.75rem;max-width:380px;width:100%;display:flex;flex-direction:column;gap:1.1rem;box-shadow:0 4px 24px rgba(0,0,0,.08)}}
.join-logo{{display:flex;justify-content:center;margin-bottom:.25rem}}
.join-logo img{{height:32px;width:auto}}
.join-title{{text-align:center}}
.join-title h2{{font-family:'Barlow Condensed',sans-serif;font-size:1.4rem;font-weight:700;letter-spacing:.03em}}
.join-title p{{color:var(--muted);font-size:.88rem;margin-top:.25rem}}
.field{{display:flex;flex-direction:column;gap:.4rem}}
.field label{{font-size:.82rem;color:var(--muted);font-weight:500}}
.field input{{width:100%}}
.divider{{display:flex;align-items:center;gap:.75rem;color:var(--muted);font-size:.8rem}}
.divider::before,.divider::after{{content:'';flex:1;height:1px;background:var(--border)}}
</style></head><body>
<div class="wrap">
  <div class="box">
    <div class="join-logo"><img src="/static/wordmark.png" alt="Scrum"></div>
    <div class="join-title">
      <h2>You're invited</h2>
      <p>Join <strong>{_esc(group_name)}</strong> on Scrum</p>
    </div>
    <form method="post" action="/groups/join/{_esc(token)}" style="display:flex;flex-direction:column;gap:.85rem">
      <input type="hidden" name="action" value="register">
      <div class="field"><label>Choose a username</label><input type="text" name="new_username" required placeholder="e.g. rugbyfan42" autocomplete="username" autocapitalize="none"></div>
      <div class="field"><label>Password</label><input type="password" name="new_password" required placeholder="At least 6 characters" autocomplete="new-password"></div>
      {err}
      <button type="submit" class="btn" style="width:100%;font-size:1rem;padding:.8rem">Create Account &amp; Join</button>
    </form>
    <div class="divider">Already have an account?</div>
    <form method="post" action="/groups/join/{_esc(token)}" style="display:flex;flex-direction:column;gap:.85rem">
      <input type="hidden" name="action" value="login">
      <div class="field"><label>Username</label><input type="text" name="username" required autocomplete="username" autocapitalize="none"></div>
      <div class="field"><label>Password</label><input type="password" name="password" required autocomplete="current-password"></div>
      <button type="submit" class="btn btn-ghost" style="width:100%;padding:.75rem">Log In &amp; Join</button>
    </form>
  </div>
</div>
<script>if('serviceWorker' in navigator)navigator.serviceWorker.register('/sw.js').catch(()=>{{}});</script>
</body></html>"""


@app.get("/groups/join/{token}", response_class=HTMLResponse)
async def group_join_link_get(request: Request, token: str):
    async with _db.execute(
        "SELECT gi.*, g.name, g.slug FROM group_invites gi JOIN groups g ON gi.group_id=g.id WHERE gi.token=?",
        (token,),
    ) as cur:
        inv = await cur.fetchone()
    if not inv:
        return HTMLResponse("<h2>Invite link not found or expired.</h2>", status_code=404)
    inv = dict(inv)

    # Check expiry and capacity
    if inv.get("expires_at") and time.time() > inv["expires_at"]:
        return HTMLResponse("<h2>This invite link has expired.</h2>", status_code=410)
    if inv["use_count"] >= inv["max_uses"]:
        return HTMLResponse("<h2>This invite link has been fully used.</h2>", status_code=410)

    username = _get_session_user(request)
    if username:
        # Already logged in — just show join button
        already = await _group_member_role(inv["group_id"], username)
        if already:
            return RedirectResponse(url=f"/groups/{inv['slug']}", status_code=303)
        user = await _get_user(username)
        return HTMLResponse(f"""<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Join {_esc(inv['name'])} · Scrum</title>{_FONTS}{_PWA_META}<style>{_BASE_CSS}
.wrap{{display:flex;align-items:center;justify-content:center;min-height:100vh;padding:1.5rem}}
.box{{background:var(--surface);border:1px solid var(--border);border-radius:16px;padding:2rem 1.75rem;max-width:360px;width:100%;text-align:center;display:flex;flex-direction:column;gap:1rem;box-shadow:0 4px 24px rgba(0,0,0,.08)}}
.g-name{{font-family:'Barlow Condensed',sans-serif;font-size:1.5rem;font-weight:700;letter-spacing:.04em}}
</style></head><body>
<div class="wrap"><div class="box">
  <img src="/static/wordmark.png" alt="Scrum" style="height:28px;width:auto;margin:0 auto">
  <div style="color:var(--muted);font-size:.9rem">You've been invited to join</div>
  <div class="g-name">{_esc(inv['name'])}</div>
  <form method="post" action="/groups/join/{_esc(token)}">
    <input type="hidden" name="action" value="join">
    <button type="submit" class="btn" style="width:100%;font-size:1rem;padding:.8rem">Join Group</button>
  </form>
</div></div></body></html>""")

    return HTMLResponse(_join_register_page(token, inv["name"]))


@app.post("/groups/join/{token}", response_class=HTMLResponse)
async def group_join_link_post(request: Request, token: str):
    async with _db.execute(
        "SELECT gi.*, g.name, g.slug FROM group_invites gi JOIN groups g ON gi.group_id=g.id WHERE gi.token=?",
        (token,),
    ) as cur:
        inv = await cur.fetchone()
    if not inv:
        return HTMLResponse("<h2>Invite link not found.</h2>", status_code=404)
    inv = dict(inv)
    if inv["use_count"] >= inv["max_uses"]:
        return HTMLResponse("<h2>This invite link has been fully used.</h2>", status_code=410)

    form = await request.form()
    action = form.get("action", "join")

    async def _do_join(uname: str, response):
        await _db.execute(
            "INSERT OR IGNORE INTO group_members (group_id,username,role,joined_at) VALUES(?,?,?,?)",
            (inv["group_id"], uname, "member", time.time()),
        )
        await _db.execute(
            "UPDATE group_invites SET use_count=use_count+1, used_by=?, used_at=? WHERE token=?",
            (uname, time.time(), token),
        )
        await _db.commit()

    if action == "join":
        # Logged-in user joining directly
        username = _get_session_user(request)
        if not username:
            return RedirectResponse(url=f"/groups/join/{token}", status_code=303)
        resp = RedirectResponse(url=f"/groups/{inv['slug']}", status_code=303)
        await _do_join(username, resp)
        return resp

    elif action == "register":
        new_username = (form.get("new_username") or "").strip().lower()
        new_password = (form.get("new_password") or "").strip()
        if not new_username or not new_password:
            return HTMLResponse(_join_register_page(token, inv["name"], "Username and password are required."))
        if len(new_password) < 6:
            return HTMLResponse(_join_register_page(token, inv["name"], "Password must be at least 6 characters."))
        if not re.match(r'^[a-z0-9][a-z0-9_.-]*[a-z0-9]$|^[a-z0-9]$', new_username):
            return HTMLResponse(_join_register_page(token, inv["name"], "Username can only contain letters, numbers, _ . -"))
        async with _db.execute("SELECT id FROM users WHERE username=?", (new_username,)) as cur:
            if await cur.fetchone():
                return HTMLResponse(_join_register_page(token, inv["name"], f'Username "{new_username}" is already taken.'))
        pw_hash = pwd_ctx.hash(new_password)
        await _db.execute(
            "INSERT INTO users (username,password_hash,is_admin,created_at) VALUES(?,?,0,?)",
            (new_username, pw_hash, time.time()),
        )
        await _db.commit()
        await _do_join(new_username, None)
        # Also add to Global group
        await _db.execute(
            "INSERT OR IGNORE INTO group_members (group_id,username,role,joined_at) VALUES(1,?,?,?)",
            (new_username, "member", time.time()),
        )
        await _db.commit()
        resp = RedirectResponse(url=f"/groups/{inv['slug']}", status_code=303)
        resp.set_cookie(SESSION_COOKIE, _make_token(new_username, permanent=True),
                        max_age=60*60*24*365*5, httponly=True, samesite="lax")
        logger.info("Self-registered via invite: %s → group %s", new_username, inv["slug"])
        return resp

    elif action == "login":
        login_username = (form.get("username") or "").strip().lower()
        login_password = (form.get("password") or "").strip()
        async with _db.execute("SELECT password_hash FROM users WHERE username=?", (login_username,)) as cur:
            row = await cur.fetchone()
        if not row or not pwd_ctx.verify(login_password, row["password_hash"]):
            return HTMLResponse(_join_register_page(token, inv["name"], "Invalid username or password."))
        await _do_join(login_username, None)
        resp = RedirectResponse(url=f"/groups/{inv['slug']}", status_code=303)
        resp.set_cookie(SESSION_COOKIE, _make_token(login_username),
                        max_age=SESSION_MAX_AGE, httponly=True, samesite="lax")
        return resp

    return RedirectResponse(url=f"/groups/join/{token}", status_code=303)


# ── Group settings ─────────────────────────────────────────────────────────────
@app.get("/groups/{slug}/settings", response_class=HTMLResponse)
async def group_settings_get(request: Request, slug: str, delete_error: str = ""):
    username = _get_session_user(request)
    user = await _get_user(username)
    is_admin = user and user["is_admin"]
    g = await _get_group(slug)
    if not g:
        return HTMLResponse("<h2>Group not found</h2>", status_code=404)
    role = await _group_member_role(g["id"], username)
    if role != "admin":
        return RedirectResponse(url=f"/groups/{slug}", status_code=303)

    current_leagues = set(await _group_leagues(g["id"]))
    current_types = set(await _group_pred_types(g["id"]))
    custom_comps = await _group_custom_comps(g["id"])

    league_options = ""
    for ls, (_, label) in ALL_ESPN_LEAGUES.items():
        checked = "checked" if ls in current_leagues else ""
        league_options += f'<label class="check-row"><input type="checkbox" name="leagues" value="{ls}" {checked}><span>{label}</span></label>'

    pred_rows = ""
    for pt_key, pt_label, pt_desc, resolve in PREDICTION_TYPES:
        checked = "checked" if pt_key in current_types else ""
        badge = '<span class="res-badge res-auto">Auto-resolves</span>' if resolve == "auto" else '<span class="res-badge res-manual">May need manual entry</span>'
        pred_rows += f"""<label class="check-row">
  <input type="checkbox" name="pred_types" value="{pt_key}" {checked}>
  <span><strong>{pt_label}</strong> {badge}<span class="pred-desc">{pt_desc}</span></span>
</label>"""

    # Members management
    async with _db.execute(
        "SELECT username, role FROM group_members WHERE group_id=? ORDER BY role DESC, username",
        (g["id"],),
    ) as cur:
        members = [dict(r) for r in await cur.fetchall()]
    member_rows = ""
    for m in members:
        is_self = m["username"] == username
        role_text = "Admin" if m["role"] == "admin" else "Member"
        promote_btn = "" if is_self or m["role"] == "admin" else f'<form method="post" action="/groups/{_esc(slug)}/settings/promote" style="display:inline;margin-right:.35rem"><input type="hidden" name="target" value="{_esc(m["username"])}"><button class="btn btn-sm btn-ghost" type="submit">Make Admin</button></form>'
        remove_btn  = "" if is_self else f'<form method="post" action="/groups/{_esc(slug)}/settings/remove" style="display:inline"><input type="hidden" name="target" value="{_esc(m["username"])}"><button class="btn btn-danger btn-sm" type="submit">Remove</button></form>'
        member_rows += f'<tr><td>{_esc(_ucfirst(m["username"]))}</td><td style="color:var(--muted)">{role_text}</td><td style="text-align:right">{promote_btn}{remove_btn}</td></tr>'

    archived_banner = '<div style="background:rgba(34,168,74,.1);border:1px solid rgba(34,168,74,.25);border-radius:8px;padding:.65rem 1rem;color:var(--accent);font-size:.85rem;margin-bottom:.75rem">Season archived ✓ — leaderboard reset.</div>' if request.query_params.get("archived") else ""
    async with _db.execute(
        "SELECT COUNT(*) as n FROM seasons WHERE group_id=?", (g["id"],)
    ) as cur:
        past_season_count = (await cur.fetchone())["n"]
    season_section_html = f"""{archived_banner}<div class="form-section" style="margin-top:1rem">
  <div class="section-label">Season Management</div>
  <p style="font-size:.85rem;color:var(--muted);margin-bottom:.85rem">Snapshot the current leaderboard and start fresh. History is preserved and viewable.</p>
  <form method="post" action="/groups/{_esc(slug)}/season/archive" style="display:flex;gap:.5rem;flex-wrap:wrap;margin-bottom:.5rem">
    <input type="text" name="season_name" placeholder='Season name (e.g. "2025")' style="flex:1;min-width:140px">
    <button type="submit" class="btn btn-sm">Archive &amp; Reset</button>
  </form>
  {'<a href="/groups/'+_esc(slug)+'/seasons" class="btn btn-sm btn-ghost">View Past Seasons ('+str(past_season_count)+')</a>' if past_season_count else ''}
</div>"""
    err_html = '<div style="color:var(--danger);font-size:.85rem;margin-bottom:.5rem">Group name didn\'t match — try again.</div>' if delete_error else ""
    if g["id"] != 1:
        delete_section_html = f"""{err_html}<div class="form-section" style="margin-top:1rem;border-color:rgba(220,38,38,.3)">
  <div class="section-label" style="color:var(--danger)">Danger Zone</div>
  <p style="font-size:.85rem;color:var(--muted);margin-bottom:.85rem">Permanently deletes this group and its leaderboard. Member predictions are not affected.</p>
  <form method="post" action="/groups/{_esc(slug)}/delete">
    <input type="text" name="confirm" placeholder='Type "{_esc(g["name"])}" to confirm' required style="width:100%;margin-bottom:.6rem">
    <button type="submit" class="btn btn-danger" style="width:100%">Delete Group</button>
  </form>
</div>"""
    else:
        delete_section_html = ""

    return HTMLResponse(f"""<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>Settings · {_esc(g['name'])}</title>
{_FONTS}{_PWA_META}<style>{_BASE_CSS}
.page-body{{max-width:620px;margin:0 auto;padding:2rem 1.5rem}}
.page-title{{font-family:'Barlow Condensed',sans-serif;font-size:1.5rem;font-weight:700;letter-spacing:.04em;text-transform:uppercase;margin-bottom:1.25rem}}
.form-section{{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:1.25rem;margin-bottom:1rem}}
.section-label{{font-family:'Barlow Condensed',sans-serif;font-size:.78rem;font-weight:700;letter-spacing:.1em;text-transform:uppercase;color:var(--muted);margin-bottom:.9rem}}
.check-row{{display:flex;align-items:flex-start;gap:.6rem;padding:.5rem 0;border-bottom:1px solid var(--border);cursor:pointer;line-height:1.4}}
.check-row:last-child{{border-bottom:none}}
.check-row input[type=checkbox]{{margin-top:.2rem;accent-color:var(--accent3);width:16px;height:16px;flex-shrink:0}}
.check-row span{{font-size:.88rem}}
.pred-desc{{display:block;color:var(--muted);font-size:.78rem;margin-top:.15rem}}
.res-badge{{display:inline-block;font-size:.65rem;font-weight:700;letter-spacing:.07em;text-transform:uppercase;padding:.1rem .4rem;border-radius:4px;vertical-align:middle;margin-left:.35rem}}
.res-auto{{background:rgba(34,197,94,.12);color:#22c55e;border:1px solid rgba(34,197,94,.25)}}
.res-manual{{background:rgba(251,191,36,.1);color:#f59e0b;border:1px solid rgba(251,191,36,.25)}}
table{{width:100%;border-collapse:collapse}}td{{padding:.5rem .3rem;border-bottom:1px solid var(--border)}}tr:last-child td{{border-bottom:none}}
</style></head><body>
{_nav(username, is_admin, "groups")}
<div class="page-body">
  <div class="page-title">Settings — {_esc(g['name'])}</div>
  <form method="post" action="/groups/{_esc(slug)}/settings">
    <div class="form-section">
      <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:.9rem">
        <div class="section-label" style="margin-bottom:0">Competitions</div>
        <a href="/groups/{_esc(slug)}/competitions" class="btn btn-sm btn-ghost" style="font-size:.72rem">
          ★ Custom{(' (' + str(len(custom_comps)) + ')') if custom_comps else ''}
        </a>
      </div>
      {league_options}
    </div>
    <div class="form-section">
      <div class="section-label">Prediction Types</div>
      {pred_rows}
    </div>
    <div style="display:flex;gap:.75rem;margin-top:.5rem">
      <button type="submit" class="btn">Save Changes</button>
      <a href="/groups/{_esc(slug)}" class="btn btn-ghost">Cancel</a>
    </div>
  </form>
  <div class="form-section" style="margin-top:1rem">
    <div class="section-label">Members</div>
    <table><tbody>{member_rows}</tbody></table>
  </div>
  {season_section_html}
  {delete_section_html}
</div>
{_bnav("groups", 0, is_admin)}
</body></html>""")


@app.post("/groups/{slug}/settings", response_class=HTMLResponse)
async def group_settings_post(request: Request, slug: str):
    username = _get_session_user(request)
    g = await _get_group(slug)
    if not g:
        return HTMLResponse("<h2>Group not found</h2>", status_code=404)
    role = await _group_member_role(g["id"], username)
    if role != "admin":
        return RedirectResponse(url=f"/groups/{slug}", status_code=303)

    form = await request.form()
    leagues = form.getlist("leagues")
    pred_types = form.getlist("pred_types")

    await _db.execute("DELETE FROM group_leagues WHERE group_id=?", (g["id"],))
    await _db.execute("DELETE FROM group_prediction_types WHERE group_id=?", (g["id"],))
    for ls in leagues:
        if ls in ALL_ESPN_LEAGUES:
            await _db.execute(
                "INSERT OR IGNORE INTO group_leagues (group_id,league_slug) VALUES(?,?)", (g["id"], ls)
            )
    for pt in pred_types:
        if pt in {k for k, *_ in PREDICTION_TYPES}:
            await _db.execute(
                "INSERT OR IGNORE INTO group_prediction_types (group_id,prediction_type) VALUES(?,?)", (g["id"], pt)
            )
    await _db.commit()
    return RedirectResponse(url=f"/groups/{slug}", status_code=303)


# ── Custom competitions manager ───────────────────────────────────────────────
@app.get("/groups/{slug}/competitions", response_class=HTMLResponse)
async def group_competitions_get(request: Request, slug: str):
    username = _get_session_user(request)
    user = await _get_user(username)
    is_admin = user and user["is_admin"]
    g = await _get_group(slug)
    if not g:
        return HTMLResponse("<h2>Group not found</h2>", status_code=404)
    role = await _group_member_role(g["id"], username)
    if role != "admin":
        return RedirectResponse(url=f"/groups/{slug}", status_code=303)

    custom_comps = await _group_custom_comps(g["id"])
    comps_html = ""
    for cc in custom_comps:
        cc_matches = await _custom_comp_matches(cc["id"])
        match_list = "".join(
            f'<div style="font-size:.82rem;padding:.2rem 0;color:var(--text)">'
            f'{_esc(cm["team_home"])} vs {_esc(cm["team_away"])} '
            f'<span style="color:var(--muted);font-size:.75rem">'
            f'{datetime.fromtimestamp(cm["kickoff_ts"],tz=timezone.utc).strftime("%d %b")}</span></div>'
            for cm in cc_matches
        ) or '<div style="color:var(--muted);font-size:.82rem">No matches added yet.</div>'
        comps_html += f"""<div style="background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:1rem 1.25rem;margin-bottom:.75rem">
  <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:.5rem;gap:.5rem;flex-wrap:wrap">
    <div style="font-family:'Barlow Condensed',sans-serif;font-weight:700;font-size:1rem;letter-spacing:.03em">★ {_esc(cc['name'])}</div>
    <div style="display:flex;gap:.4rem">
      <a href="/groups/{_esc(slug)}/competitions/{cc['id']}/matches" class="btn btn-sm btn-ghost">Edit Matches</a>
      <form method="post" action="/groups/{_esc(slug)}/competitions/{cc['id']}/delete" style="display:inline" onsubmit="return confirm({json.dumps('Delete ' + cc['name'] + '?')})">
        <button class="btn btn-sm btn-danger" type="submit">Delete</button>
      </form>
    </div>
  </div>
  <div style="font-size:.75rem;color:var(--muted);margin-bottom:.5rem">{len(cc_matches)} match{"es" if len(cc_matches)!=1 else ""}</div>
  {match_list}
</div>"""

    created_msg = '<div style="background:rgba(34,168,74,.1);border:1px solid rgba(34,168,74,.25);border-radius:8px;padding:.6rem 1rem;color:var(--accent);font-size:.85rem;margin-bottom:.75rem">Competition created ✓</div>' if request.query_params.get("created") else ""
    return HTMLResponse(f"""<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Custom Competitions · {_esc(g['name'])}</title>
{_FONTS}{_PWA_META}<style>{_BASE_CSS}
.page-body{{max-width:660px;margin:0 auto;padding:2rem 1.5rem 4rem}}
.page-title{{font-family:'Barlow Condensed',sans-serif;font-size:1.5rem;font-weight:700;letter-spacing:.04em;text-transform:uppercase;margin-bottom:.25rem}}
.page-sub{{color:var(--muted);font-size:.88rem;margin-bottom:1.5rem}}
.new-comp-form{{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:1rem 1.25rem;margin-bottom:1.25rem;display:flex;gap:.75rem;align-items:center;flex-wrap:wrap}}
@media(max-width:600px){{.page-body{{padding:1rem 1rem 4rem}}}}
</style></head><body>
{_nav(username, is_admin, "groups")}
<div class="page-body">
  <div style="display:flex;align-items:center;gap:.75rem;margin-bottom:.25rem">
    <a href="/groups/{_esc(slug)}/settings" style="color:var(--muted);font-size:.85rem">← Settings</a>
  </div>
  <div class="page-title">Custom Competitions</div>
  <div class="page-sub">{_esc(g['name'])} · Build named mini-tournaments from any matches in the ESPN feed. Each gets its own leaderboard.</div>
  {created_msg}
  <form method="post" action="/groups/{_esc(slug)}/competitions" class="new-comp-form">
    <input type="text" name="name" placeholder="Competition name (e.g. NZ Tour of SA 2026)" required maxlength="60" style="flex:1;min-width:200px">
    <button type="submit" class="btn">Create</button>
  </form>
  {comps_html if comps_html else '<div style="color:var(--muted);font-size:.88rem;text-align:center;padding:2rem 0">No custom competitions yet. Create one above.</div>'}
</div>
{_bnav("groups", 0, is_admin)}
</body></html>""")


@app.post("/groups/{slug}/competitions", response_class=HTMLResponse)
async def group_competitions_post(request: Request, slug: str):
    username = _get_session_user(request)
    g = await _get_group(slug)
    if not g:
        return HTMLResponse("<h2>Group not found</h2>", status_code=404)
    role = await _group_member_role(g["id"], username)
    if role != "admin":
        return RedirectResponse(url=f"/groups/{slug}", status_code=303)
    form = await request.form()
    name = (form.get("name") or "").strip()[:60]
    if not name:
        return RedirectResponse(url=f"/groups/{slug}/competitions", status_code=303)
    comp_slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")[:50]
    try:
        await _db.execute(
            "INSERT INTO custom_competitions (group_id,name,slug,created_at) VALUES(?,?,?,?)",
            (g["id"], name, comp_slug, time.time()),
        )
        await _db.commit()
    except Exception:
        pass
    return RedirectResponse(url=f"/groups/{slug}/competitions?created=1", status_code=303)


@app.post("/groups/{slug}/competitions/{comp_id}/delete", response_class=HTMLResponse)
async def group_competition_delete(request: Request, slug: str, comp_id: int):
    username = _get_session_user(request)
    g = await _get_group(slug)
    if not g:
        return HTMLResponse("<h2>Group not found</h2>", status_code=404)
    role = await _group_member_role(g["id"], username)
    if role != "admin":
        return RedirectResponse(url=f"/groups/{slug}", status_code=303)
    await _db.execute("DELETE FROM custom_competition_matches WHERE comp_id=?", (comp_id,))
    await _db.execute("DELETE FROM custom_competitions WHERE id=? AND group_id=?", (comp_id, g["id"]))
    await _db.commit()
    return RedirectResponse(url=f"/groups/{slug}/competitions", status_code=303)


@app.get("/groups/{slug}/competitions/{comp_id}/matches", response_class=HTMLResponse)
async def group_comp_matches_get(request: Request, slug: str, comp_id: int):
    username = _get_session_user(request)
    user = await _get_user(username)
    is_admin = user and user["is_admin"]
    g = await _get_group(slug)
    if not g:
        return HTMLResponse("<h2>Group not found</h2>", status_code=404)
    role = await _group_member_role(g["id"], username)
    if role != "admin":
        return RedirectResponse(url=f"/groups/{slug}", status_code=303)
    async with _db.execute(
        "SELECT * FROM custom_competitions WHERE id=? AND group_id=?", (comp_id, g["id"])
    ) as cur:
        cc = await cur.fetchone()
    if not cc:
        return RedirectResponse(url=f"/groups/{slug}/competitions", status_code=303)
    cc = dict(cc)

    existing = {cm["match_id"] for cm in await _custom_comp_matches(comp_id)}
    try:
        all_upcoming = await _fetch_espn_upcoming()
    except Exception:
        all_upcoming = []

    now_ts = time.time()
    by_comp: dict[str, list] = {}
    seen: set[str] = set()
    for m in all_upcoming:
        kts = m.get("kickoff_ts") or 0
        if kts and kts <= now_ts:
            continue
        m_slug = m.get("slug") or re.sub(r"[^a-z0-9]+", "-", f"{m['team_home']}-vs-{m['team_away']}".lower()).strip("-")
        m["slug"] = m_slug
        seen.add(m_slug)
        comp_name = ALL_ESPN_LEAGUES.get(m["tournament"], (None, m["tournament"]))[1]
        by_comp.setdefault(comp_name, []).append(m)
    # Add already-selected matches not in current ESPN window
    for cm in await _custom_comp_matches(comp_id):
        if cm["match_id"] not in seen:
            comp_name = ALL_ESPN_LEAGUES.get(cm["tournament"], (None, cm["tournament"]))[1]
            by_comp.setdefault(comp_name, []).append({
                "tournament": cm["tournament"], "team_home": cm["team_home"],
                "team_away": cm["team_away"], "kickoff_ts": cm["kickoff_ts"],
                "slug": cm["match_id"], "espn_id": cm["espn_id"], "league_id": cm["league_id"],
                "_stale": True,
            })

    rows_html = ""
    for comp_name in sorted(by_comp.keys()):
        matches = sorted(by_comp[comp_name], key=lambda m: m.get("kickoff_ts") or 0)
        rows_html += f'<div class="mp-comp-header">{_esc(comp_name)}</div>'
        for m in matches:
            m_slug = m["slug"]
            kts = m.get("kickoff_ts") or 0
            checked = "checked" if m_slug in existing else ""

            stale = ' <span class="mp-stale">outside window</span>' if m.get("_stale") else ""
            rows_html += f"""<label class="mp-row" data-comp="{_esc(comp_name.lower())}">
  <input type="checkbox" name="match_ids" value="{_esc(m_slug)}" {checked}
    data-home="{_esc(m['team_home'])}" data-away="{_esc(m['team_away'])}"
    data-kts="{kts}" data-tournament="{_esc(m['tournament'])}"
    data-espn-id="{_esc(m.get('espn_id',''))}" data-league-id="{m.get('league_id') or ''}">
  <span class="mp-teams">{_esc(m['team_home'])} <span class="mp-vs">vs</span> {_esc(m['team_away'])}</span>
  <span class="mp-date fix-kick" data-ts="{kts}" data-fmt="dt">{stale}</span>
</label>"""

    sel_count = len(existing)
    return HTMLResponse(f"""<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{_esc(cc['name'])} · Match Picker</title>
{_FONTS}{_PWA_META}<style>{_BASE_CSS}
.page-body{{max-width:680px;margin:0 auto;padding:2rem 1.5rem 6rem}}
.page-title{{font-family:'Barlow Condensed',sans-serif;font-size:1.4rem;font-weight:700;letter-spacing:.04em;text-transform:uppercase;margin-bottom:.15rem}}
.page-sub{{color:var(--muted);font-size:.85rem;margin-bottom:1.25rem}}
.mp-toolbar{{display:flex;gap:.75rem;align-items:center;flex-wrap:wrap;margin-bottom:1rem}}
.mp-search{{flex:1;min-width:160px}}
.mp-filter{{min-width:140px}}
.mp-count{{font-family:'Barlow Condensed',sans-serif;font-size:.85rem;font-weight:700;color:var(--accent3);white-space:nowrap}}
.mp-comp-header{{font-family:'Barlow Condensed',sans-serif;font-size:.7rem;font-weight:700;letter-spacing:.1em;text-transform:uppercase;color:var(--muted);padding:.6rem 0 .3rem;border-bottom:1px solid var(--border);margin-top:1rem}}
.mp-comp-header:first-child{{margin-top:0}}
.mp-row{{display:flex;align-items:center;gap:.75rem;padding:.55rem .25rem;border-bottom:1px solid var(--border);cursor:pointer;transition:background .15s}}
.mp-row:hover{{background:var(--surface2);border-radius:6px}}
.mp-row input[type=checkbox]{{width:18px;height:18px;accent-color:var(--accent3);flex-shrink:0;cursor:pointer}}
.mp-teams{{flex:1;font-size:.88rem;font-weight:500}}
.mp-vs{{color:var(--muted);font-weight:400;margin:0 .25rem;font-size:.78rem}}
.mp-date{{font-size:.75rem;color:var(--muted);white-space:nowrap}}
.mp-stale{{background:rgba(245,166,35,.15);color:var(--accent3);font-size:.65rem;font-weight:700;letter-spacing:.05em;text-transform:uppercase;padding:.1rem .3rem;border-radius:3px;margin-left:.3rem}}
.mp-row.hidden{{display:none}}
.save-bar{{position:fixed;bottom:0;left:0;right:0;background:#fff;border-top:1.5px solid var(--border);padding:.85rem 1.25rem;display:flex;align-items:center;justify-content:space-between;gap:1rem;z-index:20;box-shadow:0 -2px 12px rgba(0,0,0,.06)}}
.save-bar-left{{font-size:.85rem;color:var(--muted)}}
.save-bar-count{{font-family:'Barlow Condensed',sans-serif;font-weight:700;color:var(--accent3)}}
@media(max-width:600px){{.page-body{{padding:1rem 1rem 5rem}}.mp-date{{display:none}}}}
</style></head><body>
{_nav(username, is_admin, "groups")}
<div class="page-body">
  <div style="margin-bottom:.5rem"><a href="/groups/{_esc(slug)}/competitions" style="color:var(--muted);font-size:.85rem">← Competitions</a></div>
  <div class="page-title">★ {_esc(cc['name'])}</div>
  <div class="page-sub">Pick the matches that count toward this competition. Scores from these matches are tallied separately.</div>
  <div class="mp-toolbar">
    <input type="text" class="mp-search" id="mp-search" placeholder="Search teams…" autocomplete="off">
    <select class="mp-filter" id="mp-filter">
      <option value="">All competitions</option>
      {''.join(f'<option value="{_esc(n)}">{_esc(n)}</option>' for n in sorted(by_comp.keys()))}
    </select>
    <span class="mp-count" id="mp-count">{sel_count} selected</span>
  </div>
  <form id="picker-form" method="post" action="/groups/{_esc(slug)}/competitions/{comp_id}/matches">
    <div id="mp-list">
      {rows_html if rows_html else '<div style="color:var(--muted);font-size:.88rem;padding:2rem;text-align:center">No upcoming matches in the ESPN feed.</div>'}
    </div>
    <div class="save-bar">
      <span class="save-bar-left"><span class="save-bar-count" id="save-count">{sel_count}</span> matches selected</span>
      <div style="display:flex;gap:.6rem">
        <a href="/groups/{_esc(slug)}/competitions" class="btn btn-ghost btn-sm">Cancel</a>
        <button type="submit" class="btn">Save</button>
      </div>
    </div>
  </form>
</div>
<script>
const search=document.getElementById('mp-search');
const filter=document.getElementById('mp-filter');
const countEl=document.getElementById('mp-count');
const saveEl=document.getElementById('save-count');
const rows=document.querySelectorAll('.mp-row');
function updateCount(){{const n=document.querySelectorAll('.mp-row input:checked').length;countEl.textContent=n+' selected';saveEl.textContent=n;}}
function applyFilters(){{
  const q=search.value.toLowerCase();const comp=filter.value.toLowerCase();
  rows.forEach(row=>{{
    const teams=row.querySelector('.mp-teams').textContent.toLowerCase();
    const rowComp=(row.dataset.comp||'').toLowerCase();
    row.classList.toggle('hidden',!((!q||teams.includes(q))&&(!comp||rowComp.includes(comp))));
  }});
}}
rows.forEach(r=>r.querySelector('input').addEventListener('change',updateCount));
search.addEventListener('input',applyFilters);
filter.addEventListener('change',applyFilters);
updateCount();
document.querySelectorAll('.fix-kick[data-ts]').forEach(el=>{{
  const ts=parseInt(el.dataset.ts)*1000;
  if(!ts)return;
  const d=new Date(ts);
  if(el.dataset.fmt==='date'){{el.textContent=d.toLocaleDateString('en-GB',{{day:'numeric',month:'short'}});}}
  else{{const stale=el.querySelector('.mp-stale');const staleHtml=stale?stale.outerHTML:'';el.innerHTML=d.toLocaleDateString('en-GB',{{weekday:'short',month:'short',day:'numeric'}})+' · '+d.toLocaleTimeString('en-GB',{{hour:'2-digit',minute:'2-digit'}})+staleHtml;}}
}});
</script>
</body></html>""")


@app.post("/groups/{slug}/competitions/{comp_id}/matches", response_class=HTMLResponse)
async def group_comp_matches_post(request: Request, slug: str, comp_id: int):
    username = _get_session_user(request)
    g = await _get_group(slug)
    if not g:
        return HTMLResponse("<h2>Group not found</h2>", status_code=404)
    role = await _group_member_role(g["id"], username)
    if role != "admin":
        return RedirectResponse(url=f"/groups/{slug}", status_code=303)
    async with _db.execute(
        "SELECT id FROM custom_competitions WHERE id=? AND group_id=?", (comp_id, g["id"])
    ) as cur:
        if not await cur.fetchone():
            return RedirectResponse(url=f"/groups/{slug}/competitions", status_code=303)

    form = await request.form()
    selected = set(form.getlist("match_ids"))

    try:
        all_upcoming = await _fetch_espn_upcoming()
    except Exception:
        all_upcoming = []
    now_ts = time.time()
    by_slug: dict[str, dict] = {}
    for m in all_upcoming:
        kts = m.get("kickoff_ts") or 0
        m_slug = m.get("slug") or re.sub(r"[^a-z0-9]+", "-", f"{m['team_home']}-vs-{m['team_away']}".lower()).strip("-")
        by_slug[m_slug] = m
    existing_by_id = {cm["match_id"]: cm for cm in await _custom_comp_matches(comp_id)}

    await _db.execute("DELETE FROM custom_competition_matches WHERE comp_id=?", (comp_id,))
    for ms in selected:
        if ms in by_slug:
            m = by_slug[ms]
            await _db.execute(
                "INSERT OR REPLACE INTO custom_competition_matches "
                "(comp_id,match_id,espn_id,league_id,team_home,team_away,kickoff_ts,tournament) "
                "VALUES(?,?,?,?,?,?,?,?)",
                (comp_id, ms, m.get("espn_id",""), m.get("league_id"), m["team_home"], m["team_away"], m.get("kickoff_ts",0), m["tournament"]),
            )
        elif ms in existing_by_id:
            cm = existing_by_id[ms]
            await _db.execute(
                "INSERT OR REPLACE INTO custom_competition_matches "
                "(comp_id,match_id,espn_id,league_id,team_home,team_away,kickoff_ts,tournament) "
                "VALUES(?,?,?,?,?,?,?,?)",
                (comp_id, ms, cm["espn_id"], cm["league_id"], cm["team_home"], cm["team_away"], cm["kickoff_ts"], cm["tournament"]),
            )
    await _db.commit()
    logger.info("Custom comp %d matches updated: %d", comp_id, len(selected))
    return RedirectResponse(url=f"/groups/{slug}/competitions", status_code=303)


@app.post("/groups/{slug}/settings/remove", response_class=HTMLResponse)
async def group_remove_member(request: Request, slug: str):
    username = _get_session_user(request)
    g = await _get_group(slug)
    if not g:
        return HTMLResponse("<h2>Group not found</h2>", status_code=404)
    role = await _group_member_role(g["id"], username)
    if role != "admin":
        return RedirectResponse(url=f"/groups/{slug}", status_code=303)
    form = await request.form()
    target = (form.get("target") or "").strip()
    if target and target != username:
        await _db.execute(
            "DELETE FROM group_members WHERE group_id=? AND username=?", (g["id"], target)
        )
        await _db.commit()
    return RedirectResponse(url=f"/groups/{slug}/settings", status_code=303)


@app.post("/groups/{slug}/settings/promote", response_class=HTMLResponse)
async def group_promote_member(request: Request, slug: str):
    username = _get_session_user(request)
    g = await _get_group(slug)
    if not g:
        return HTMLResponse("<h2>Group not found</h2>", status_code=404)
    role = await _group_member_role(g["id"], username)
    if role != "admin":
        return RedirectResponse(url=f"/groups/{slug}", status_code=303)
    form = await request.form()
    target = (form.get("target") or "").strip()
    if target and target != username:
        await _db.execute(
            "UPDATE group_members SET role='admin' WHERE group_id=? AND username=?",
            (g["id"], target),
        )
        await _db.commit()
    return RedirectResponse(url=f"/groups/{slug}/settings", status_code=303)


@app.post("/groups/{slug}/season/archive")
async def group_archive_season(request: Request, slug: str, season_name: str = Form(default="")):
    username = _get_session_user(request)
    g = await _get_group(slug)
    if not g:
        return RedirectResponse(url="/groups", status_code=303)
    role = await _group_member_role(g["id"], username)
    if role != "admin":
        return RedirectResponse(url=f"/groups/{slug}", status_code=303)

    name = season_name.strip() or f"Season {datetime.now().strftime('%Y')}"

    # Snapshot current leaderboard
    await _db.execute(
        "INSERT INTO seasons (group_id,name,archived_at) VALUES(?,?,?)",
        (g["id"], name, time.time()),
    )
    await _db.commit()
    async with _db.execute("SELECT id FROM seasons WHERE group_id=? ORDER BY id DESC LIMIT 1", (g["id"],)) as cur:
        season_id = (await cur.fetchone())["id"]

    async with _db.execute(
        """SELECT username, SUM(points) as tp, COUNT(*) as m, SUM(exact_score) as ex
           FROM leaderboard WHERE group_id=? GROUP BY username""",
        (g["id"],),
    ) as cur:
        rows = [dict(r) for r in await cur.fetchall()]
    for r in rows:
        await _db.execute(
            "INSERT INTO season_leaderboard (season_id,username,total_points,matches,exact_scores) VALUES(?,?,?,?,?)",
            (season_id, r["username"], r["tp"] or 0, r["m"] or 0, r["ex"] or 0),
        )

    # Clear current leaderboard for this group
    await _db.execute("DELETE FROM leaderboard WHERE group_id=?", (g["id"],))
    await _db.commit()
    logger.info("Season archived: group %s season %s", slug, name)
    return RedirectResponse(url=f"/groups/{slug}/settings?archived=1", status_code=303)


@app.get("/groups/{slug}/seasons", response_class=HTMLResponse)
async def group_seasons(request: Request, slug: str):
    username = _get_session_user(request)
    user = await _get_user(username)
    is_admin = bool(user and user["is_admin"])
    g = await _get_group(slug)
    if not g:
        return HTMLResponse("<h2>Group not found</h2>", status_code=404)
    if not await _group_member_role(g["id"], username):
        return RedirectResponse(url="/groups", status_code=303)

    async with _db.execute(
        "SELECT id, name, archived_at FROM seasons WHERE group_id=? ORDER BY archived_at DESC",
        (g["id"],),
    ) as cur:
        seasons = [dict(r) for r in await cur.fetchall()]

    seasons_html = ""
    for s in seasons:
        async with _db.execute(
            "SELECT username, total_points, matches, exact_scores FROM season_leaderboard WHERE season_id=? ORDER BY total_points DESC, exact_scores DESC, matches DESC",
            (s["id"],),
        ) as cur:
            lb = [dict(r) for r in await cur.fetchall()]
        rows = ""
        for i, r in enumerate(lb, 1):
            medal = _medal(i, 22)
            rows += f'<tr><td>{medal}</td><td>{_esc(_ucfirst(r["username"]))}</td><td style="text-align:right;font-weight:700;color:var(--accent3)">{r["total_points"]}</td><td style="text-align:right;color:var(--muted)">{r["matches"]}m · {r["exact_scores"]}✓</td></tr>'
        date_str = datetime.fromtimestamp(s["archived_at"]).strftime("%d %b %Y")
        seasons_html += f"""<div style="background:var(--surface);border:1px solid var(--border);border-radius:12px;margin-bottom:1rem;overflow:hidden">
  <div style="padding:.85rem 1rem;border-bottom:1px solid var(--border);display:flex;justify-content:space-between;align-items:center">
    <span style="font-family:'Barlow Condensed',sans-serif;font-weight:700;font-size:1.05rem">{_esc(s['name'])}</span>
    <span style="font-size:.78rem;color:var(--muted)">Archived {date_str}</span>
  </div>
  <table style="width:100%;border-collapse:collapse;font-size:.85rem">
    <tbody>{rows}</tbody>
  </table>
</div>"""

    if not seasons_html:
        seasons_html = '<div style="color:var(--muted);font-size:.9rem;text-align:center;padding:2rem">No past seasons yet.</div>'

    return HTMLResponse(f"""<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Seasons · {_esc(g['name'])}</title>{_FONTS}{_PWA_META}<style>{_BASE_CSS}
.page-body{{max-width:600px;margin:0 auto;padding:1.25rem 1rem 5rem}}
.page-title{{font-family:'Barlow Condensed',sans-serif;font-size:1.5rem;font-weight:700;letter-spacing:.04em;text-transform:uppercase;margin-bottom:1.1rem}}
table td{{padding:.45rem .75rem;border-bottom:1px solid var(--border)}}tr:last-child td{{border-bottom:none}}
</style></head><body>
{_nav(username, is_admin, "groups")}
<div class="page-body">
  <a href="/groups/{_esc(slug)}/settings" style="font-size:.85rem;color:var(--muted);display:inline-flex;align-items:center;gap:.25rem;margin-bottom:.85rem">← Settings</a>
  <div class="page-title">Past Seasons — {_esc(g['name'])}</div>
  {seasons_html}
</div>
{_bnav("groups", 0, is_admin)}
</body></html>""")


@app.post("/groups/{slug}/delete")
async def group_delete(request: Request, slug: str, confirm: str = Form(default="")):
    username = _get_session_user(request)
    g = await _get_group(slug)
    if not g:
        return RedirectResponse(url="/groups", status_code=303)
    if g["id"] == 1:
        return RedirectResponse(url=f"/groups/{slug}/settings", status_code=303)
    role = await _group_member_role(g["id"], username)
    user = await _get_user(username)
    is_site_admin = user and user["is_admin"]
    if role != "admin" and not is_site_admin:
        return RedirectResponse(url=f"/groups/{slug}", status_code=303)
    if confirm != g["name"]:
        return RedirectResponse(url=f"/groups/{slug}/settings?delete_error=1", status_code=303)

    await _db.execute("DELETE FROM group_prediction_types WHERE group_id=?", (g["id"],))
    await _db.execute("DELETE FROM group_leagues WHERE group_id=?", (g["id"],))
    await _db.execute("DELETE FROM group_invites WHERE group_id=?", (g["id"],))
    await _db.execute("DELETE FROM group_members WHERE group_id=?", (g["id"],))
    await _db.execute("DELETE FROM leaderboard WHERE group_id=?", (g["id"],))
    await _db.execute(
        "DELETE FROM season_leaderboard WHERE season_id IN (SELECT id FROM seasons WHERE group_id=?)",
        (g["id"],)
    )
    await _db.execute("DELETE FROM seasons WHERE group_id=?", (g["id"],))
    await _db.execute("DELETE FROM groups WHERE id=?", (g["id"],))
    await _db.commit()
    logger.info("Group deleted: %s by %s", slug, username)
    return RedirectResponse(url="/groups", status_code=303)


# ── Predictions ───────────────────────────────────────────────────────────────
@app.get("/predict/{slug}", response_class=HTMLResponse)
async def predict_page(request: Request, slug: str, th: str = "", ta: str = "", kts: float = 0, title: str = "", tournament: str = "", group: str = ""):
    username = _get_session_user(request)
    if not username:
        return RedirectResponse(url="/login", status_code=303)
    user = await _get_user(username)
    is_admin = user and user["is_admin"]

    async with _db.execute(
        "SELECT match_title, team_home, team_away, kickoff_ts FROM predictions WHERE match_id=? LIMIT 1",
        (slug,),
    ) as cur:
        row = await cur.fetchone()
    if row:
        d = dict(row)
        title, team_home, team_away, kickoff_ts = d["match_title"], d["team_home"], d["team_away"], d["kickoff_ts"]
    elif th and ta:
        team_home, team_away = th.strip(), ta.strip()
        title = title.strip() or f"{team_home} vs {team_away}"
        kickoff_ts = float(kts)
    else:
        return HTMLResponse(f"""<!DOCTYPE html><html><head><meta charset="utf-8">{_FONTS}
<style>{_BASE_CSS}</style></head><body>{_nav(username, is_admin)}
<div style="padding:3rem 2rem;text-align:center;color:var(--muted)">Match not found or no predictions yet.</div>
</body></html>""")

    now = time.time()
    window_open = (kickoff_ts == 0) or (kickoff_ts - 172800 <= now < kickoff_ts)

    # Determine which prediction types to show — union across all user's groups following this league
    my_groups = await _get_user_groups(username, include_global=True)
    active_pred_types: set[str] = set()
    for g in my_groups:
        g_leagues = await _group_leagues(g["id"])
        if not tournament or tournament in g_leagues:
            g_types = await _group_pred_types(g["id"])
            active_pred_types.update(g_types)
    active_pred_types.discard("score")  # score is always shown

    # Player pool for try scorer predictions
    player_pool_home: list[tuple] = []  # (name, jersey)
    player_pool_away: list[tuple] = []
    if "try_anytime" in active_pred_types or "try_first" in active_pred_types:
        async with _db.execute(
            "SELECT name, jersey FROM players WHERE team_name=? ORDER BY COALESCE(jersey,99), name",
            (team_home,),
        ) as cur:
            player_pool_home = [(r["name"], r["jersey"]) for r in await cur.fetchall()]
        async with _db.execute(
            "SELECT name, jersey FROM players WHERE team_name=? ORDER BY COALESCE(jersey,99), name",
            (team_away,),
        ) as cur:
            player_pool_away = [(r["name"], r["jersey"]) for r in await cur.fetchall()]

    # My prediction
    async with _db.execute(
        """SELECT score_home, score_away, pred_winner, pred_margin,
                  pred_try_any, pred_try_first, pred_btts
           FROM predictions WHERE match_id=? AND username=?""",
        (slug, username),
    ) as cur:
        my_pred = await cur.fetchone()
    my_pred = dict(my_pred) if my_pred else None

    # Result (if entered)
    async with _db.execute("SELECT * FROM match_results WHERE match_id=?", (slug,)) as cur:
        result_row = await cur.fetchone()
    result = dict(result_row) if result_row else None

    # All predictions — always visible
    all_preds = []
    async with _db.execute(
        "SELECT username, score_home, score_away, created_at FROM predictions WHERE match_id=? ORDER BY created_at",
        (slug,),
    ) as cur:
        all_preds = [dict(r) for r in await cur.fetchall()]
    if result:
        fh, fa = result["final_home"], result["final_away"]
        for p in all_preds:
            p["diff"] = abs(p["score_home"] - fh) + abs(p["score_away"] - fa)
            p["exact"] = p["diff"] == 0
        min_diff = min((p["diff"] for p in all_preds), default=9999)
        for p in all_preds:
            p["winner_score"] = p["exact"] or (p["diff"] == min_diff)
        all_preds.sort(key=lambda p: p["diff"])

    # Build extra prediction fields for the form
    def _player_select(name: str, pool_home: list, pool_away: list, current: str = "") -> str:
        if not pool_home and not pool_away:
            return f'<input type="text" name="{name}" placeholder="Player name" value="{_esc(current or "")}" style="width:100%">'
        def _opt(player_name: str, jersey) -> str:
            label = f"#{jersey} {player_name}" if jersey is not None else player_name
            sel = " selected" if player_name == current else ""
            return f'<option value="{_esc(player_name)}"{sel}>{_esc(label)}</option>'
        opts = '<option value="">— Select player —</option>'
        if pool_home:
            opts += f'<optgroup label="{_esc(team_home)}">'
            for player_name, jersey in pool_home:
                opts += _opt(player_name, jersey)
            opts += '</optgroup>'
        if pool_away:
            opts += f'<optgroup label="{_esc(team_away)}">'
            for player_name, jersey in pool_away:
                opts += _opt(player_name, jersey)
            opts += '</optgroup>'
        return f'<select name="{name}" style="width:100%">{opts}</select>'

    extra_fields = ""
    if "winner" in active_pred_types:
        cur_w = (my_pred or {}).get("pred_winner") or ""
        extra_fields += f"""<div class="pred-extra-field">
  <div class="pef-label">Winner</div>
  <div class="pef-opts">
    <label class="radio-opt"><input type="radio" name="pred_winner" value="home" {'checked' if cur_w=='home' else ''}> {_esc(team_home)}</label>
    <label class="radio-opt"><input type="radio" name="pred_winner" value="draw" {'checked' if cur_w=='draw' else ''}> Draw</label>
    <label class="radio-opt"><input type="radio" name="pred_winner" value="away" {'checked' if cur_w=='away' else ''}> {_esc(team_away)}</label>
  </div>
</div>"""
    if "margin" in active_pred_types:
        cur_m = (my_pred or {}).get("pred_margin") or ""
        margin_opts = ""
        for band in ["1-7", "8-14", "15-21", "22+"]:
            sel = " selected" if band == cur_m else ""
            margin_opts += f'<option value="{band}"{sel}>{band} pts</option>'
        extra_fields += f"""<div class="pred-extra-field">
  <div class="pef-label">Winning Margin</div>
  <select name="pred_margin" style="width:100%"><option value="">— Select band —</option>{margin_opts}</select>
</div>"""
    if "btts" in active_pred_types:
        cur_b = (my_pred or {}).get("pred_btts")
        extra_fields += f"""<div class="pred-extra-field">
  <div class="pef-label">Both Teams to Score a Try?</div>
  <div class="pef-opts">
    <label class="radio-opt"><input type="radio" name="pred_btts" value="1" {'checked' if cur_b==1 else ''}> Yes</label>
    <label class="radio-opt"><input type="radio" name="pred_btts" value="0" {'checked' if cur_b==0 else ''}> No</label>
  </div>
</div>"""
    if "try_anytime" in active_pred_types:
        cur_ta = (my_pred or {}).get("pred_try_any") or ""
        extra_fields += f"""<div class="pred-extra-field">
  <div class="pef-label">Anytime Try Scorer</div>
  {_player_select("pred_try_any", player_pool_home, player_pool_away, cur_ta)}
</div>"""
    if "try_first" in active_pred_types:
        cur_tf = (my_pred or {}).get("pred_try_first") or ""
        extra_fields += f"""<div class="pred-extra-field">
  <div class="pef-label">First Try Scorer</div>
  {_player_select("pred_try_first", player_pool_home, player_pool_away, cur_tf)}
</div>"""

    # Build prediction form or status
    if my_pred:
        form_html = f"""<div class="pred-box pred-done">
  <div class="pb-label">Your Prediction</div>
  <div class="pb-score">
    <div class="pb-team">{_esc(team_home)}</div>
    <div class="pb-num">{my_pred['score_home']}</div>
    <div class="pb-dash">—</div>
    <div class="pb-num">{my_pred['score_away']}</div>
    <div class="pb-team">{_esc(team_away)}</div>
  </div>
  {''.join(f'<div class="pred-summary-row"><span class="psr-label">{PRED_LABEL.get(k,"")}</span><span class="psr-val">{_esc(str(v))}</span></div>' for k,v in [("winner",my_pred.get("pred_winner")),("margin",my_pred.get("pred_margin")),("btts","Yes" if my_pred.get("pred_btts")==1 else ("No" if my_pred.get("pred_btts")==0 else None)),("try_anytime",my_pred.get("pred_try_any")),("try_first",my_pred.get("pred_try_first"))] if v)}
  <div style="font-size:.8rem;color:var(--muted);margin-top:.5rem">Locked — predictions cannot be changed.</div>
</div>"""
    elif window_open:
        mins_left = ""
        if kickoff_ts > 0:
            secs = max(0, int(kickoff_ts - now))
            if secs == 0:
                mins_left = " · Closes at kickoff"
            elif secs < 3600:
                mins_left = f" · {secs // 60}m until kickoff"
            elif secs < 86400:
                mins_left = f" · {secs // 3600}h until kickoff"
            else:
                mins_left = f" · {secs // 86400}d {(secs % 86400) // 3600}h until kickoff"
        form_html = f"""<div class="pred-box" id="pred-form-wrap">
  <div class="pb-label">Make Your Prediction{mins_left}</div>
  <form id="pred-form">
    <input type="hidden" name="match_title" value="{_esc(title)}">
    <input type="hidden" name="kickoff_ts" value="{kickoff_ts}">
    <input type="hidden" name="team_home" value="{_esc(team_home)}">
    <input type="hidden" name="team_away" value="{_esc(team_away)}">
    <input type="hidden" name="tournament" value="{_esc(tournament)}">
    <div class="pef-label" style="margin-bottom:.4rem">Score</div>
    <div class="pb-score" style="margin:.5rem 0 1rem">
      <div class="pb-team">{_esc(team_home)}</div>
      <input type="number" name="score_home" min="0" max="200" placeholder="0" required
             style="width:60px;text-align:center;font-size:1.2rem">
      <div class="pb-dash">—</div>
      <input type="number" name="score_away" min="0" max="200" placeholder="0" required
             style="width:60px;text-align:center;font-size:1.2rem">
      <div class="pb-team">{_esc(team_away)}</div>
    </div>
    {extra_fields}
    <label class="banker-toggle">
      <input type="checkbox" name="is_banker" value="1" id="banker-cb">
      <div class="banker-label">
        <span class="banker-icon">🔒</span>
        <div>
          <div style="font-weight:600;font-size:.88rem">Banker Pick</div>
          <div style="font-size:.75rem;color:var(--muted)">Double points if correct — one per week</div>
        </div>
      </div>
    </label>
    <button type="submit" class="btn" style="width:100%;margin-top:.75rem">Lock In Prediction</button>
  </form>
</div>
<script>
document.getElementById('pred-form').addEventListener('submit',async function(e){{
  e.preventDefault();
  const btn=this.querySelector('button[type=submit]');
  btn.disabled=true; btn.textContent='Saving…';
  const fd=new FormData(this);
  try{{
    const r=await fetch('/api/predict/{_esc(slug)}',{{method:'POST',body:fd}});
    const d=await r.json();
    if(d.ok){{
      document.getElementById('pred-form-wrap').innerHTML=
        '<div class="pb-label">Your Prediction</div>'
        +'<div class="pb-score" style="margin:.75rem 0">'
        +'<div class="pb-team">'+d.team_home+'</div>'
        +'<div style="font-size:2rem;font-weight:700;color:var(--accent3)">'+d.home+'</div>'
        +'<div class="pb-dash">—</div>'
        +'<div style="font-size:2rem;font-weight:700;color:var(--accent3)">'+d.away+'</div>'
        +'<div class="pb-team">'+d.team_away+'</div></div>'
        +'<div style="text-align:center;color:var(--muted);font-size:.85rem">Locked in ✓</div>';
    }}else{{
      btn.disabled=false; btn.textContent='Lock In Prediction';
      alert(d.error||'Something went wrong');
    }}
  }}catch{{
    btn.disabled=false; btn.textContent='Lock In Prediction';
    alert('Network error — please try again');
  }}
}});
</script>"""
    else:
        admin_enter = ""
        if is_admin and not result:
            admin_enter = f' <a href="/admin" class="btn btn-sm" style="margin-top:.6rem;display:inline-block">Enter Result →</a>'
        form_html = f'<div class="pred-box pred-closed"><div class="pb-label">Prediction Window Closed</div>{admin_enter}</div>'

    # Build predictions list
    preds_html = ""
    if all_preds:
        result_banner = ""
        if result:
            t_name = TOURNAMENTS.get(result["tournament"], result["tournament"])
            result_banner = f"""<div class="result-banner">
  <div class="rb-label">Final Score · {_esc(t_name)}</div>
  <div class="pb-score" style="justify-content:center;gap:1.2rem">
    <div class="pb-team">{_esc(result['team_home'])}</div>
    <div class="rb-num">{result['final_home']}</div>
    <div class="pb-dash">—</div>
    <div class="rb-num">{result['final_away']}</div>
    <div class="pb-team">{_esc(result['team_away'])}</div>
  </div>
</div>"""
        rows = ""
        for i, p in enumerate(all_preds):
            winner_cls = " pred-winner" if result and p.get("winner") else ""
            exact_badge = ' <span class="exact-badge">EXACT</span>' if result and p.get("exact") else ""
            diff_str = f'<span class="pred-diff">diff: {p["diff"]}</span>' if result else ""
            rows += f"""<div class="pred-row{winner_cls}">
  <span class="pred-rank">{i+1}</span>
  <span class="pred-uname">{_esc(_ucfirst(p['username']))}</span>
  <span class="pred-sc">{_esc(team_home)} <strong>{p['score_home']}</strong> — <strong>{p['score_away']}</strong> {_esc(team_away)}</span>
  {diff_str}{exact_badge}
</div>"""
        preds_html = f"""<div class="preds-section">
  {result_banner}
  <div class="preds-title">{len(all_preds)} Prediction{"s" if len(all_preds)!=1 else ""}</div>
  {rows}
</div>"""
    else:
        preds_html = '<div style="text-align:center;color:var(--muted);font-size:.85rem;padding:1rem">No predictions yet — be the first!</div>'

    kick_str = ""
    if kickoff_ts:
        kick_dt = datetime.fromtimestamp(kickoff_ts, tz=timezone.utc)
        kick_str = f'<div class="kick-time">Kickoff: {kick_dt.strftime("%d %b %Y %H:%M")} UTC</div>'

    return HTMLResponse(f"""<!DOCTYPE html><html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Predict · {_esc(title)}</title>{_FONTS}<style>{_BASE_CSS}
main{{max-width:600px;margin:2rem auto;padding:0 1.5rem}}
.match-header{{text-align:center;margin-bottom:1.5rem}}
.match-title{{font-family:'Barlow Condensed',sans-serif;font-size:1.5rem;font-weight:700;margin-bottom:.35rem}}
.kick-time{{font-size:.82rem;color:var(--muted)}}
.pred-box{{background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:1.25rem;margin-bottom:1.2rem}}
.pred-done{{border-color:rgba(0,230,118,.3);background:rgba(0,230,118,.04)}}
.pred-closed{{border-color:var(--border);background:var(--surface2)}}
.pb-label{{font-family:'Barlow Condensed',sans-serif;font-size:.78rem;font-weight:700;letter-spacing:.08em;text-transform:uppercase;color:var(--muted);margin-bottom:.6rem}}
.pb-score{{display:flex;align-items:center;gap:.75rem;flex-wrap:wrap}}
.pb-team{{font-family:'Barlow Condensed',sans-serif;font-size:1rem;font-weight:600;flex:1;text-align:center}}
.pb-num{{font-family:'Barlow Condensed',sans-serif;font-size:1.8rem;font-weight:700;color:var(--accent2);min-width:36px;text-align:center}}
.pb-dash{{color:var(--muted);font-size:1.4rem}}
.preds-section{{margin-top:1rem}}
.preds-title{{font-family:'Barlow Condensed',sans-serif;font-size:.78rem;font-weight:700;letter-spacing:.08em;text-transform:uppercase;color:var(--muted);margin-bottom:.6rem}}
.pred-row{{display:flex;align-items:center;gap:.65rem;padding:.55rem .75rem;background:var(--surface);border-radius:6px;margin-bottom:.35rem;border:1px solid var(--border);font-size:.87rem;flex-wrap:wrap}}
.pred-winner{{border-color:rgba(0,230,118,.4);background:rgba(0,230,118,.05)}}
.pred-rank{{font-family:'Barlow Condensed',sans-serif;font-size:.8rem;color:var(--muted);min-width:18px}}
.pred-uname{{font-family:'Barlow Condensed',sans-serif;font-weight:700;color:var(--accent);min-width:80px}}
.pred-sc{{flex:1;font-size:.85rem}}
.pred-diff{{font-size:.72rem;color:var(--muted)}}
.exact-badge{{font-family:'Barlow Condensed',sans-serif;font-size:.68rem;font-weight:700;letter-spacing:.08em;background:rgba(0,230,118,.2);color:var(--accent2);border:1px solid rgba(0,230,118,.4);padding:.1rem .4rem;border-radius:3px}}
.result-banner{{background:rgba(0,176,255,.06);border:1px solid rgba(0,176,255,.25);border-radius:8px;padding:1rem;margin-bottom:1rem;text-align:center}}
.rb-label{{font-family:'Barlow Condensed',sans-serif;font-size:.78rem;font-weight:700;letter-spacing:.08em;text-transform:uppercase;color:var(--accent);margin-bottom:.5rem}}
.rb-num{{font-family:'Barlow Condensed',sans-serif;font-size:2rem;font-weight:700;color:var(--text)}}
</style></head><body>
{_nav(username, is_admin)}
<main>
  <div class="match-header">
    <div class="match-title">{_esc(title)}</div>
    {kick_str}
  </div>
  {form_html}
  {preds_html}
</main></body></html>""")


@app.post("/api/predict/{slug}")
async def api_predict(
    request: Request,
    slug: str,
    score_home: int = Form(...),
    score_away: int = Form(...),
    team_home: str = Form(...),
    team_away: str = Form(...),
    match_title: str = Form(...),
    kickoff_ts: float = Form(default=0),
    tournament: str = Form(default=""),
    pred_winner: str = Form(default=""),
    pred_margin: str = Form(default=""),
    pred_btts: str = Form(default=""),
    pred_try_any: str = Form(default=""),
    pred_try_first: str = Form(default=""),
    is_banker: str = Form(default=""),
):
    username = _get_session_user(request)
    if not username:
        return JSONResponse({"error": "not authenticated"}, status_code=401)

    now = time.time()
    if kickoff_ts > 0 and not (kickoff_ts - 172800 <= now < kickoff_ts):
        return JSONResponse({"error": "prediction window closed"}, status_code=400)

    btts_val = int(pred_btts) if pred_btts in ("0", "1") else None

    # Banker: one per user per 7-day window, clear existing banker in same window if set
    banker_val = 1 if is_banker == "1" else 0
    if banker_val:
        week_start = now - (now % 604800)  # start of current UTC week
        async with _db.execute(
            "SELECT match_id FROM predictions WHERE username=? AND is_banker=1 AND kickoff_ts>=?",
            (username, week_start),
        ) as cur:
            existing_banker = await cur.fetchone()
        if existing_banker:
            await _db.execute(
                "UPDATE predictions SET is_banker=0 WHERE username=? AND match_id=?",
                (username, existing_banker["match_id"]),
            )

    try:
        await _db.execute(
            """INSERT INTO predictions
               (match_id,match_title,team_home,team_away,kickoff_ts,tournament,username,
                score_home,score_away,pred_winner,pred_margin,pred_btts,pred_try_any,pred_try_first,is_banker,created_at)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (slug, match_title.strip(), team_home.strip(), team_away.strip(),
             kickoff_ts, tournament.strip(), username,
             score_home, score_away,
             pred_winner.strip() or None, pred_margin.strip() or None, btts_val,
             pred_try_any.strip() or None, pred_try_first.strip() or None,
             banker_val, time.time()),
        )
        await _db.commit()
        return JSONResponse({"ok": True, "home": score_home, "away": score_away,
                             "team_home": team_home, "team_away": team_away})
    except aiosqlite.IntegrityError:
        async with _db.execute(
            "SELECT score_home, score_away, team_home, team_away FROM predictions WHERE match_id=? AND username=?",
            (slug, username),
        ) as cur:
            row = await cur.fetchone()
        if row:
            if tournament.strip():
                await _db.execute(
                    "UPDATE predictions SET tournament=? WHERE match_id=? AND username=? AND (tournament IS NULL OR tournament='')",
                    (tournament.strip(), slug, username),
                )
                await _db.commit()
            d = dict(row)
            return JSONResponse({"ok": True, "home": d["score_home"], "away": d["score_away"],
                                 "team_home": d["team_home"], "team_away": d["team_away"]})
        return JSONResponse({"error": "already predicted"}, status_code=409)


# ── Standings ─────────────────────────────────────────────────────────────────
async def _fetch_espn_standings(league_id: int) -> list[dict]:
    """Fetch standings from ESPN. Returns list of groups: [{name, rows}]."""
    cache_key = f"standings:espn:{league_id}"
    cached = await _redis.get(cache_key)
    if cached:
        return json.loads(cached)
    try:
        url = f"https://site.api.espn.com/apis/v2/sports/rugby/{league_id}/standings"
        resp = await _http_client.get(url, timeout=10)
        if resp.status_code != 200:
            return []
        data = resp.json()
        children = data.get("children", [])
        if not children:
            return []

        def _parse_entries(entries):
            rows = []
            for entry in entries:
                team = entry.get("team", {})
                stats = {s["name"]: s.get("displayValue", "") for s in entry.get("stats", []) if "name" in s}
                rows.append({
                    "pos":  stats.get("rank", str(len(rows)+1)),
                    "team": team.get("displayName") or team.get("name", ""),
                    "p":    stats.get("gamesPlayed", ""),
                    "w":    stats.get("gamesWon", ""),
                    "d":    stats.get("gamesDrawn", ""),
                    "l":    stats.get("gamesLost", ""),
                    "pf":   stats.get("pointsFor", ""),
                    "pa":   stats.get("pointsAgainst", ""),
                    "pd":   stats.get("pointsDifference", ""),
                    "bp":   stats.get("bonusPoints", ""),
                    "pts":  stats.get("points", ""),
                })
            return rows

        groups = []
        for child in children:
            entries = child.get("standings", {}).get("entries", [])
            if entries:
                groups.append({
                    "name": child.get("name", ""),
                    "rows": _parse_entries(entries),
                })

        await _redis.set(cache_key, json.dumps(groups), ex=1800)
        return groups
    except Exception as exc:
        logger.warning("ESPN standings %s: %s", league_id, exc)
        return []


async def _fetch_world_rankings() -> list[dict]:
    """Fetch World Rugby men's rankings (top 15)."""
    cache_key = "standings:world-rankings"
    cached = await _redis.get(cache_key)
    if cached:
        return json.loads(cached)
    try:
        url = "https://api.wr-rims-prod.pulselive.com/rugby/v3/rankings/mru?language=en"
        resp = await _http_client.get(url, timeout=10)
        if resp.status_code != 200:
            return []
        data = resp.json()
        rows = []
        for entry in (data.get("entries") or [])[:15]:
            team = entry.get("team", {})
            prev = entry.get("previousPos", entry.get("pos", 0))
            curr = entry.get("pos", 0)
            rows.append({
                "pos":    curr,
                "team":   team.get("name", ""),
                "abbr":   team.get("abbreviation", ""),
                "pts":    f"{entry.get('pts', 0):.2f}",
                "change": prev - curr,
            })
        await _redis.set(cache_key, json.dumps(rows), ex=1800)
        return rows
    except Exception as exc:
        logger.warning("World rankings fetch: %s", exc)
        return []


async def _fetch_rugby_news() -> list[dict]:
    """Fetch rugby union news from multiple RSS sources, merged and sorted."""
    cache_key = "rugby:news"
    cached = await _redis.get(cache_key)
    if cached:
        return json.loads(cached)

    import xml.etree.ElementTree as ET
    from email.utils import parsedate_to_datetime

    FEEDS = [
        ("BBC Sport", "https://feeds.bbci.co.uk/sport/rugby-union/rss.xml"),
    ]

    def _parse_feed(text: str, source: str) -> list[dict]:
        ns = {"media": "http://search.yahoo.com/mrss/"}
        try:
            root = ET.fromstring(text)
        except Exception:
            return []
        items = []
        for item in root.findall(".//item")[:15]:
            title = (item.findtext("title") or "").strip()
            link  = (item.findtext("link") or "").strip()
            desc  = re.sub(r"<[^>]+>", "", item.findtext("description") or "").strip()
            pub   = (item.findtext("pubDate") or "").strip()
            thumb = ""
            for tag in ("media:thumbnail", "media:content"):
                el = item.find(tag, ns)
                if el is not None:
                    thumb = el.get("url", "")
                    if thumb:
                        break
            # parse timestamp for sorting
            try:
                ts = int(parsedate_to_datetime(pub).timestamp()) if pub else 0
            except Exception:
                ts = 0
            if title and link:
                items.append({"title": title, "link": link, "desc": desc,
                              "pub": pub, "thumb": thumb, "source": source, "ts": ts})
        return items

    results = await asyncio.gather(
        *[_http_client.get(url, timeout=10) for _, url in FEEDS],
        return_exceptions=True,
    )

    all_items = []
    for (source, _), resp in zip(FEEDS, results):
        if isinstance(resp, Exception) or resp.status_code != 200:
            logger.warning("News feed failed (%s): %s", source, resp)
            continue
        all_items.extend(_parse_feed(resp.text, source))

    # Sort by timestamp descending, deduplicate by title
    all_items.sort(key=lambda x: x["ts"], reverse=True)
    seen, deduped = set(), []
    for item in all_items:
        key = item["title"].lower()[:60]
        if key not in seen:
            seen.add(key)
            deduped.append(item)

    await _redis.set(cache_key, json.dumps(deduped[:30]), ex=900)
    return deduped[:30]


@app.get("/standings", response_class=HTMLResponse)
async def standings_page(request: Request, t: str = "world-cup"):
    username = _get_session_user(request)
    user = await _get_user(username)
    is_admin = bool(user and user["is_admin"])

    # Find active tab
    tab_keys = [k for k, _, _ in STANDINGS_TABS]
    if t not in tab_keys:
        t = tab_keys[0]

    # Build tabs HTML
    tabs_html = ""
    for key, label, _ in STANDINGS_TABS:
        active_cls = " s-tab-active" if key == t else ""
        tabs_html += f'<a href="/standings?t={key}" class="s-tab{active_cls}">{label}</a>'

    # Fetch data
    league_id = next((lid for k, _, lid in STANDINGS_TABS if k == t), None)
    table_html = ""
    if t == "world-cup":
        # 2023 RWC knockout results
        ko_2023 = [
            ("Quarter-finals", [
                ("New Zealand", 28, "Ireland", 24),
                ("England", 30, "Fiji", 24),
                ("Wales", 17, "Argentina", 29),
                ("France", 28, "South Africa", 29),
            ]),
            ("Semi-finals", [
                ("New Zealand", 44, "Argentina", 6),
                ("England", 15, "South Africa", 16),
            ]),
            ("Bronze Final", [
                ("Argentina", 26, "England", 23),
            ]),
            ("Final", [
                ("New Zealand", 11, "South Africa", 12),
            ]),
        ]
        ko_rows = ""
        for round_name, matches in ko_2023:
            ko_rows += f'<div class="wc-round-hdr">{round_name}</div>'
            for t1, s1, t2, s2 in matches:
                winner1 = " wc-winner" if s1 > s2 else ""
                winner2 = " wc-winner" if s2 > s1 else ""
                ko_rows += f"""<div class="wc-match">
  <span class="wc-team{winner1}">{_esc(t1)}</span>
  <span class="wc-score">{s1} – {s2}</span>
  <span class="wc-team wc-team-r{winner2}">{_esc(t2)}</span>
</div>"""

        # 2027 RWC pool draw (Australia) — source: rugbyworldcup.com/2027
        pools_2027 = {
            "Pool A": ["New Zealand", "Australia", "Chile", "Hong Kong China"],
            "Pool B": ["South Africa", "Italy", "Georgia", "Romania"],
            "Pool C": ["Argentina", "Fiji", "Spain", "Canada"],
            "Pool D": ["Ireland", "Scotland", "Uruguay", "Portugal"],
            "Pool E": ["France", "Japan", "USA", "Samoa"],
            "Pool F": ["England", "Wales", "Tonga", "Zimbabwe"],
        }
        pool_rows = ""
        for pool_name, teams in pools_2027.items():
            pool_rows += f'<div class="wc-pool-hdr">{pool_name}</div><ul class="wc-pool-list">'
            for team in teams:
                pool_rows += f'<li>{_esc(team)}</li>'
            pool_rows += '</ul>'

        table_html = f"""
<div class="section-banner">2023 RWC — Knockout Results</div>
<div class="wc-ko-wrap">{ko_rows}</div>
<div class="section-banner" style="margin-top:2rem">2027 RWC Pool Draw · Australia</div>
<div class="wc-pools-wrap">{pool_rows}</div>
"""
    elif t == "world-rankings":
        rows = await _fetch_world_rankings()
        if rows:
            r_rows = ""
            for r in rows:
                chg = r.get("change", 0)
                arrow = f'<span style="color:var(--accent2)">▲{chg}</span>' if chg > 0 else (f'<span style="color:#ef4444">▼{abs(chg)}</span>' if chg < 0 else '<span style="color:var(--muted)">—</span>')
                r_rows += f"""<tr>
  <td class="s-pos">{r['pos']}</td>
  <td class="s-team">{_esc(r['team'])}</td>
  <td class="s-pts"><strong>{r['pts']}</strong></td>
  <td class="s-chg">{arrow}</td>
</tr>"""
            table_html = f"""<table class="s-table">
  <thead><tr><th>#</th><th>Team</th><th>Rating</th><th>±</th></tr></thead>
  <tbody>{r_rows}</tbody>
</table>"""
        else:
            table_html = '<div class="s-empty">Rankings unavailable right now.</div>'
    elif league_id:
        groups = await _fetch_espn_standings(league_id)
        if groups:
            table_html = ""
            multi = len(groups) > 1
            for group in groups:
                header = f'<div class="s-group-hdr">{_esc(group["name"])}</div>' if multi else ""
                r_rows = ""
                for r in group["rows"]:
                    r_rows += f"""<tr>
  <td class="s-pos">{r['pos']}</td>
  <td class="s-team">{_esc(str(r['team']))}</td>
  <td>{r['p']}</td><td>{r['w']}</td><td>{r['d']}</td><td>{r['l']}</td>
  <td class="s-hide-sm">{r['pf']}</td><td class="s-hide-sm">{r['pa']}</td><td class="s-hide-sm">{r['pd']}</td>
  <td class="s-hide-sm">{r['bp']}</td><td class="s-pts"><strong>{r['pts']}</strong></td>
</tr>"""
                table_html += f"""{header}<table class="s-table">
  <thead><tr><th>#</th><th>Team</th><th>P</th><th>W</th><th>D</th><th>L</th><th class="s-hide-sm">PF</th><th class="s-hide-sm">PA</th><th class="s-hide-sm">PD</th><th class="s-hide-sm">BP</th><th>Pts</th></tr></thead>
  <tbody>{r_rows}</tbody>
</table>"""
        else:
            table_html = '<div class="s-empty">Standings unavailable — this tournament may be between seasons.</div>'
    else:
        table_html = '<div class="s-empty">No data available.</div>'

    return HTMLResponse(f"""<!DOCTYPE html><html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Standings · Scrum</title>{_FONTS}<style>{_BASE_CSS}
.s-wrap{{max-width:900px;margin:0 auto;padding:1.5rem 2rem}}
.s-tabs{{display:flex;gap:.5rem;flex-wrap:wrap;margin-bottom:1.5rem;padding-bottom:.75rem;border-bottom:1px solid var(--border)}}
.s-tab{{font-family:'Barlow Condensed',sans-serif;font-size:.85rem;font-weight:700;letter-spacing:.05em;text-transform:uppercase;padding:.4rem 1rem;border-radius:6px;border:1px solid var(--border);color:var(--muted);transition:all .2s;white-space:nowrap;display:inline-flex;align-items:center;gap:.4rem}}
@media(max-width:768px){{.s-tabs{{flex-wrap:nowrap;overflow-x:auto;-webkit-overflow-scrolling:touch;scrollbar-width:none}}.s-tabs::-webkit-scrollbar{{display:none}}.s-tab{{flex-shrink:0}}}}
.s-tab:hover{{color:var(--text);border-color:var(--muted)}}
.s-tab-active{{color:var(--header);border-color:var(--header);background:rgba(31,110,58,.08)}}
.s-table{{width:100%;border-collapse:collapse}}
.s-table thead tr{{border-bottom:2px solid var(--border)}}
.s-table th{{font-family:'Barlow Condensed',sans-serif;font-size:.75rem;font-weight:700;letter-spacing:.07em;text-transform:uppercase;color:var(--muted);padding:.5rem .6rem;text-align:left}}
.s-table td{{padding:.6rem .6rem;border-bottom:1px solid var(--border);font-size:.88rem}}
.s-table tbody tr:hover{{background:var(--surface)}}
.s-pos{{font-weight:700;width:32px;color:var(--muted)}}
.s-team{{font-family:'Barlow Condensed',sans-serif;font-size:.95rem;font-weight:600}}
.s-pts td{{font-family:'Barlow Condensed',sans-serif;font-size:1rem;color:var(--accent2)}}
.s-chg{{font-size:.8rem}}
.s-empty{{color:var(--muted);text-align:center;padding:3rem 1rem;background:var(--surface);border-radius:8px;border:1px solid var(--border)}}
.s-group-hdr{{font-family:'Barlow Condensed',sans-serif;font-size:.9rem;font-weight:700;letter-spacing:.08em;text-transform:uppercase;color:var(--accent3);margin:1.5rem 0 .5rem}}
.s-note{{font-size:.78rem;color:var(--muted);background:var(--surface);border:1px solid var(--border);border-radius:6px;padding:.5rem .75rem;margin-bottom:1rem}}
.s-table{{margin-bottom:1.5rem}}
.wc-ko-wrap{{display:flex;flex-direction:column;gap:.5rem;margin-bottom:1rem}}
.wc-round-hdr{{font-family:'Barlow Condensed',sans-serif;font-size:.82rem;font-weight:700;letter-spacing:.08em;text-transform:uppercase;color:var(--muted);margin:.75rem 0 .25rem}}
.wc-match{{display:flex;align-items:center;gap:.75rem;background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:.6rem 1rem;font-size:.92rem}}
.wc-team{{flex:1;font-family:'Barlow Condensed',sans-serif;font-size:.95rem;font-weight:600;color:var(--text-dim)}}
.wc-team-r{{text-align:right}}
.wc-winner{{color:var(--text);font-weight:700}}
.wc-score{{font-family:'Barlow Condensed',sans-serif;font-size:1.1rem;font-weight:700;color:var(--accent3);white-space:nowrap;min-width:60px;text-align:center}}
.wc-pools-wrap{{display:grid;grid-template-columns:repeat(auto-fill,minmax(180px,1fr));gap:1rem;margin-bottom:1rem}}
.wc-pool-hdr{{font-family:'Barlow Condensed',sans-serif;font-size:.9rem;font-weight:700;letter-spacing:.08em;text-transform:uppercase;color:var(--accent3);margin:0 0 .35rem;grid-column:1/-1;display:none}}
.wc-pools-wrap{{display:flex;flex-direction:column;gap:1rem}}
.wc-pool-hdr{{font-family:'Barlow Condensed',sans-serif;font-size:.82rem;font-weight:700;letter-spacing:.08em;text-transform:uppercase;color:var(--muted);margin:.75rem 0 .25rem;display:block}}
.wc-pool-list{{list-style:none;padding:0;margin:0;display:flex;flex-direction:column;gap:.3rem}}
.wc-pool-list li{{font-family:'Barlow Condensed',sans-serif;font-size:.95rem;font-weight:600;background:var(--surface);border:1px solid var(--border);border-radius:6px;padding:.45rem .75rem}}
@media(min-width:600px){{.wc-pools-wrap{{display:grid;grid-template-columns:repeat(2,1fr);gap:1rem}}.wc-pool-hdr{{margin:.25rem 0}}}}
@media(max-width:768px){{.s-wrap{{padding:1rem 1rem 80px}}.s-hide-sm{{display:none}}}}
</style></head><body>
{_nav(username, is_admin, "standings")}
<div class="s-wrap page-body">
  <div class="s-tabs">{tabs_html}</div>
  {table_html}
</div>
{_bnav("standings", 0, is_admin)}
</body></html>""")


@app.get("/news", response_class=HTMLResponse)
async def news_page(request: Request):
    username = _get_session_user(request)
    user = await _get_user(username)
    is_admin = bool(user and user["is_admin"])

    items = await _fetch_rugby_news()

    def _time_ago(pub: str) -> str:
        try:
            from email.utils import parsedate_to_datetime
            dt = parsedate_to_datetime(pub)
            diff = int(time.time()) - int(dt.timestamp())
            if diff < 3600: return f"{diff//60}m ago"
            if diff < 86400: return f"{diff//3600}h ago"
            return f"{diff//86400}d ago"
        except Exception:
            return ""

    cards_html = ""
    for item in items:
        age = _time_ago(item["pub"])
        thumb = f'<img class="news-thumb" src="{_esc(item["thumb"])}" alt="" loading="lazy">' if item.get("thumb") else '<div class="news-thumb news-thumb-ph"><span class="material-symbols-outlined">sports_rugby</span></div>'
        cards_html += f"""<a class="news-card" href="{_esc(item['link'])}" target="_blank" rel="noopener">
  {thumb}
  <div class="news-body">
    <div class="news-title">{_esc(item['title'])}</div>
    <div class="news-desc">{_esc(item['desc'][:120]+'…' if len(item['desc'])>120 else item['desc'])}</div>
    <div class="news-meta">{_esc(item.get('source',''))} · {age}</div>
  </div>
</a>"""

    if not cards_html:
        cards_html = '<div class="news-empty">Could not load news right now. Try again shortly.</div>'

    return HTMLResponse(f"""<!DOCTYPE html><html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>News · Scrum</title>{_FONTS}<style>{_BASE_CSS}
.news-wrap{{max-width:860px;margin:0 auto;padding:1.5rem 2rem}}
.news-card{{display:flex;gap:1rem;background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:.9rem;text-decoration:none;transition:border-color .2s;margin-bottom:.75rem}}
.news-card:hover{{border-color:rgba(77,158,247,.4)}}
.news-thumb{{width:100px;height:68px;object-fit:cover;border-radius:6px;flex-shrink:0;background:var(--surface2)}}
.news-thumb-ph{{display:flex;align-items:center;justify-content:center;color:var(--muted)}}
.news-body{{display:flex;flex-direction:column;gap:.3rem;min-width:0}}
.news-title{{font-family:'Barlow Condensed',sans-serif;font-size:1.05rem;font-weight:700;line-height:1.3;color:var(--text)}}
.news-desc{{font-size:.82rem;color:var(--muted);line-height:1.4;overflow:hidden;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical}}
.news-meta{{font-size:.72rem;color:var(--muted);margin-top:auto}}
.news-empty{{color:var(--muted);text-align:center;padding:3rem 1rem}}
@media(max-width:768px){{.news-wrap{{padding:1rem 1rem 80px}}.news-thumb{{width:80px;height:56px}}}}
</style></head><body>
{_nav(username, is_admin, "news")}
<div class="news-wrap page-body">
  {cards_html}
</div>
{_bnav("news", 0, is_admin)}
</body></html>""")


# ── How to Play ───────────────────────────────────────────────────────────────
@app.get("/how-to-play", response_class=HTMLResponse)
async def how_to_play(request: Request):
    username = _get_session_user(request)
    if not username:
        return RedirectResponse(url="/login", status_code=303)
    user = await _get_user(username)
    is_admin = bool(user and user["is_admin"])
    admin_section = f"""
  <div class="htp-section admin-section">
    <div class="admin-label">Admin Guide</div>
    <h2>Creating a Group</h2>
    <p>Go to <strong>Groups → Create Group</strong>. Give it a name, pick which competitions to follow, and choose which prediction types your group will compete on.</p>
    <p>Every prediction type is <span class="badge-auto">Auto-resolved</span> — the result is fetched automatically from ESPN after the match, so there's nothing to enter by hand.</p>
    <p>You can change leagues and prediction types any time from <strong>Group → Settings</strong>.</p>

    <h2>Inviting People</h2>
    <p>From your group, tap <strong>Invite People</strong>. There are two ways:</p>
    <ul>
      <li><strong>Shareable link</strong> — tap Generate Link and share it on WhatsApp or Telegram. Anyone who clicks it can create an account and join straight away. One link works for up to 50 people.</li>
      <li><strong>Notify existing users</strong> — if someone already has an account, tick their name and send them an in-app notification. They accept or decline from their Me page.</li>
    </ul>
    <p>You don't need to create accounts for people — the shareable link handles registration automatically.</p>

    <h2>Managing Members</h2>
    <p>In <strong>Group → Settings</strong> you can promote any member to group admin (so they can share the load of managing invites and results) or remove members who are no longer playing.</p>

    <h2>User Management</h2>
    <p>The site Admin panel (<strong>Admin</strong> in the nav) shows all registered users. From here you can:</p>
    <ul>
      <li><strong>Login Link</strong> — generates a one-time magic link for any user. If someone says they've forgotten their password, copy this link and send it on WhatsApp. They click it and they're in. Then they can change their password themselves from their Me page.</li>
      <li><strong>Delete</strong> — removes the user's account.</li>
    </ul>
    <p>The Auto-Fetch button triggers an immediate pull from ESPN to apply any results that came in since the last hourly check.</p>
  </div>
""" if is_admin else ""

    return HTMLResponse(f"""<!DOCTYPE html><html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>How to Play · Scrum</title>{_FONTS}<style>{_BASE_CSS}
.htp-wrap{{max-width:680px;margin:0 auto;padding:1.5rem 1rem 5rem}}
.htp-wrap h1{{font-family:'Barlow Condensed',sans-serif;font-size:1.8rem;font-weight:700;letter-spacing:.04em;text-transform:uppercase;margin:0 0 .25rem}}
.htp-subtitle{{color:var(--muted);font-size:.9rem;margin-bottom:2rem}}
.htp-section{{margin-bottom:2rem}}
.htp-section h2{{font-family:'Barlow Condensed',sans-serif;font-size:1.1rem;font-weight:700;letter-spacing:.06em;text-transform:uppercase;color:var(--header);margin:1.1rem 0 .6rem;padding-bottom:.35rem;border-bottom:1px solid var(--border)}}
.htp-section h2:first-of-type{{margin-top:0}}
.htp-section p,.htp-section li{{color:var(--text);font-size:.91rem;line-height:1.7}}
.htp-section ul{{padding-left:1.2rem;margin:.4rem 0 .75rem}}
.htp-section li{{margin-bottom:.4rem}}
.score-table{{width:100%;border-collapse:collapse;margin:.75rem 0;font-size:.88rem;border-radius:10px;overflow:hidden}}
.score-table th{{text-align:left;padding:.5rem .85rem;background:var(--surface2);color:var(--muted);font-family:'Barlow Condensed',sans-serif;font-size:.78rem;font-weight:700;letter-spacing:.07em;text-transform:uppercase}}
.score-table td{{padding:.5rem .85rem;border-top:1px solid var(--border);background:var(--surface)}}
.pts{{font-family:'Barlow Condensed',sans-serif;font-size:1rem;font-weight:700;color:var(--accent3)}}
.badge-exact{{display:inline-block;background:rgba(245,166,35,.15);color:var(--accent3);border:1px solid rgba(245,166,35,.3);border-radius:4px;padding:.1rem .4rem;font-size:.68rem;font-weight:700;letter-spacing:.06em;text-transform:uppercase;vertical-align:middle}}
.badge-closest{{display:inline-block;background:rgba(31,110,58,.1);color:var(--header);border:1px solid rgba(31,110,58,.2);border-radius:4px;padding:.1rem .4rem;font-size:.68rem;font-weight:700;letter-spacing:.06em;text-transform:uppercase;vertical-align:middle}}
.badge-played{{display:inline-block;background:var(--surface2);color:var(--muted);border:1px solid var(--border);border-radius:4px;padding:.1rem .4rem;font-size:.68rem;font-weight:700;letter-spacing:.06em;text-transform:uppercase;vertical-align:middle}}
.badge-auto{{display:inline-block;background:rgba(34,168,74,.12);color:#22a84a;border:1px solid rgba(34,168,74,.25);border-radius:4px;padding:.1rem .4rem;font-size:.68rem;font-weight:700;letter-spacing:.06em;text-transform:uppercase;vertical-align:middle}}
.badge-manual{{display:inline-block;background:rgba(245,166,35,.1);color:var(--accent3);border:1px solid rgba(245,166,35,.25);border-radius:4px;padding:.1rem .4rem;font-size:.68rem;font-weight:700;letter-spacing:.06em;text-transform:uppercase;vertical-align:middle}}
.htp-example{{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:1rem 1.1rem;margin:.75rem 0;font-size:.88rem}}
.ex-label{{font-family:'Barlow Condensed',sans-serif;font-size:.75rem;font-weight:700;letter-spacing:.06em;text-transform:uppercase;color:var(--muted);margin-bottom:.6rem}}
.htp-example table{{width:100%;border-collapse:collapse}}
.htp-example td{{padding:.32rem .4rem;color:var(--text);font-size:.88rem}}
.htp-example td:last-child{{text-align:right;font-family:'Barlow Condensed',sans-serif;font-weight:700}}
.pred-type-table{{width:100%;border-collapse:collapse;margin:.75rem 0;font-size:.87rem}}
.pred-type-table th{{text-align:left;padding:.45rem .75rem;background:var(--surface2);color:var(--muted);font-family:'Barlow Condensed',sans-serif;font-size:.75rem;font-weight:700;letter-spacing:.07em;text-transform:uppercase}}
.pred-type-table td{{padding:.5rem .75rem;border-top:1px solid var(--border);background:var(--surface);vertical-align:top;line-height:1.5}}
.pred-type-table td:first-child{{font-weight:600;white-space:nowrap}}
.pred-type-table td:last-child{{text-align:right;font-family:'Barlow Condensed',sans-serif;font-weight:700;color:var(--accent3);white-space:nowrap}}
.admin-section{{background:var(--surface);border:2px solid var(--header);border-radius:12px;padding:1.25rem 1.25rem 1.5rem;margin-top:2.5rem}}
.admin-label{{font-family:'Barlow Condensed',sans-serif;font-size:.72rem;font-weight:700;letter-spacing:.12em;text-transform:uppercase;color:var(--header);margin-bottom:.75rem;display:flex;align-items:center;gap:.5rem}}
.admin-label::before{{content:'';display:inline-block;width:8px;height:8px;border-radius:50%;background:var(--header)}}
.admin-section h2{{color:var(--header)!important}}
</style></head><body>
{_nav(username, is_admin, "how-to-play")}
<div class="htp-wrap page-body">
  <h1>How to Play</h1>
  <p class="htp-subtitle">Predict rugby matches, earn points, compete in private groups with friends.</p>

  <div class="htp-section">
    <h2>Getting Started</h2>
    <p>You join Scrum through an invite link from a friend or group admin. Clicking the link lets you create a username and password — you're in the group straight away, no waiting for anyone to approve you.</p>
    <p>Once in, you'll land on the <strong>Groups</strong> page. Tap your group to see upcoming fixtures and your group's leaderboard.</p>
  </div>

  <div class="htp-section">
    <h2>Making Predictions</h2>
    <p>Tap any upcoming fixture and tap <strong>Predict</strong>. You'll see the prediction types your group has enabled. Fill in what you know, leave the rest — only the types you submit count toward your score.</p>
    <p><strong>Prediction window:</strong> opens 48 hours before kickoff and closes at kickoff. Once the match starts, no more predictions are accepted. Predictions are locked once submitted and cannot be changed.</p>
    <p>Your prediction applies to every group you're in that follows that competition.</p>
  </div>

  <div class="htp-section">
    <h2>Prediction Types &amp; Points</h2>
    <p>Your group admin chooses which of these are active. Not all groups use all types.</p>
    <table class="pred-type-table">
      <tr><th>Type</th><th>What you predict</th><th>Points</th></tr>
      <tr>
        <td>Score</td>
        <td>Exact final score for each team</td>
        <td>5 / 3 / 1</td>
      </tr>
      <tr>
        <td>Winner</td>
        <td>Home win, away win, or draw</td>
        <td>+2</td>
      </tr>
      <tr>
        <td>Margin Band</td>
        <td>Winning margin: 1–7, 8–14, 15–21, or 22+</td>
        <td>+2</td>
      </tr>
      <tr>
        <td>Both Teams Score</td>
        <td>Will both teams score at least one try?</td>
        <td>+1</td>
      </tr>
      <tr>
        <td>Anytime Try Scorer</td>
        <td>A player who scores a try at any point</td>
        <td>+3</td>
      </tr>
      <tr>
        <td>First Try Scorer</td>
        <td>The player who scores the very first try</td>
        <td>+4</td>
      </tr>
    </table>
    <p>Score points use a diff system — <span class="badge-exact">Exact</span> score = 5 pts, <span class="badge-closest">Closest</span> diff in your group = 3 pts, everyone else who predicted = 1 pt. All other types are right or wrong.</p>
    <p>Maximum possible per match (all types enabled): <strong>17 pts</strong>. With a banker it doubles to <strong>34 pts</strong>.</p>
  </div>

  <div class="htp-section">
    <h2>Banker Pick</h2>
    <p>Once per week you can mark one prediction as your <strong>Banker</strong>. If you earn any points on that match, they're doubled. If you score zero, nothing happens — no penalty.</p>
    <p>The banker toggle appears at the bottom of the prediction form. You can only have one banker active per week — marking a new one automatically clears the previous week's banker if it's still in the same 7-day window.</p>
  </div>

  <div class="htp-section">
    <h2>Example</h2>
    <div class="htp-example">
      <div class="ex-label">All Blacks 35 – 17 Argentina · Ardie Savea scores first try</div>
      <table>
        <tr><td>Score: predicted <strong>35 – 17</strong></td><td>5 pts <span class="badge-exact">Exact</span></td></tr>
        <tr><td>Winner: predicted <strong>Home</strong> ✓</td><td>+2 pts</td></tr>
        <tr><td>Margin: predicted <strong>15–21</strong> ✓ (diff was 18)</td><td>+2 pts</td></tr>
        <tr><td>Both teams score: predicted <strong>Yes</strong> ✓</td><td>+1 pt</td></tr>
        <tr><td>Anytime try scorer: picked <strong>Savea</strong> ✓</td><td>+3 pts</td></tr>
        <tr><td>First try scorer: picked <strong>Retallick</strong> ✗</td><td>0 pts</td></tr>
      </table>
      <div style="border-top:1px solid var(--border);margin-top:.5rem;padding-top:.5rem;text-align:right;font-family:'Barlow Condensed',sans-serif;font-weight:700;font-size:1.05rem;color:var(--accent3)">Total: 13 pts</div>
    </div>
  </div>

  <div class="htp-section">
    <h2>Results &amp; Leaderboard</h2>
    <p>Results are fetched automatically from ESPN, usually within an hour of the final whistle. Try scorer data follows shortly after — typically 30–60 minutes post-match as the match summary is published.</p>
    <p>Your group leaderboard only scores the prediction types your group has enabled.</p>
  </div>

  <div class="htp-section">
    <h2>Your Me Page</h2>
    <p>The <strong>Me</strong> tab is your personal dashboard. It shows your total points, exact score count, win prediction rate, and a bar chart breaking down your hit rate for every prediction type you've used. Below that, your position in each group and your last 10 predictions with results.</p>
    <p>You can also manage your <strong>Friends</strong> list here — add other players so they appear at the top of group invite pickers. And change your password anytime from the bottom of the page.</p>
  </div>

  <div class="htp-section">
    <h2>Groups</h2>
    <p>Groups are private leagues. Each group has its own fixture feed (filtered to the competitions it follows) and its own leaderboard. You can be in multiple groups at once — your prediction is made once but scores separately in each group based on that group's rules.</p>
    <p>Pending group invites appear on your Me page. Tap <strong>Accept</strong> to join or <strong>Decline</strong> to pass.</p>
    <p><strong>Custom Competitions:</strong> Group admins can create mini-tournaments — pick any matches from the feed and give the competition a name (e.g. "NZ Tour of SA"). Custom competitions get their own leaderboard card within the group, separate from the main standings.</p>
  </div>

  <div class="htp-section">
    <h2>Supported Competitions</h2>
    <ul>
      <li>Six Nations</li>
      <li>The Rugby Championship</li>
      <li>Super Rugby Pacific</li>
      <li>United Rugby Championship (URC)</li>
      <li>Gallagher Premiership</li>
      <li>French Top 14</li>
      <li>European Champions Cup</li>
      <li>European Challenge Cup</li>
      <li>Rugby World Cup</li>
      <li>International (Tests &amp; tours)</li>
    </ul>
    <p>Your group admin picks which of these your group follows. You only see and score on the ones your group has enabled.</p>
  </div>

  {admin_section}
</div>
{_bnav("how-to-play", 0, is_admin)}
</body></html>""")


# ── Match History ─────────────────────────────────────────────────────────────
@app.get("/history", response_class=HTMLResponse)
async def history_page(request: Request, t: str = "all"):
    username = _get_session_user(request)
    user = await _get_user(username)
    is_admin = bool(user and user["is_admin"])

    # Fetch all completed matches, optionally filtered by tournament
    if t != "all" and t in ALL_ESPN_LEAGUES:
        async with _db.execute(
            """SELECT * FROM match_results WHERE tournament=?
               ORDER BY kickoff_ts DESC LIMIT 100""", (t,)
        ) as cur:
            matches = [dict(r) for r in await cur.fetchall()]
    else:
        async with _db.execute(
            "SELECT * FROM match_results ORDER BY kickoff_ts DESC LIMIT 100"
        ) as cur:
            matches = [dict(r) for r in await cur.fetchall()]

    # Build league tabs — only show leagues that have results
    async with _db.execute(
        "SELECT DISTINCT tournament FROM match_results ORDER BY tournament"
    ) as cur:
        active_leagues = {r["tournament"] for r in await cur.fetchall() if r["tournament"]}

    tab_active = " active" if t == "all" else ""
    league_tabs = f'<a href="/history?t=all" class="t-tab{tab_active}">All</a>'
    for slug, (_, name) in ALL_ESPN_LEAGUES.items():
        if slug not in active_leagues:
            continue
        active_cls = " active" if slug == t else ""
        league_tabs += f'<a href="/history?t={_esc(slug)}" class="t-tab{active_cls}">{_esc(name)}</a>'

    # Prediction counts for all shown matches in one query (avoids N+1 across the loop)
    pred_counts: dict[str, int] = {}
    if matches:
        ids = [m["match_id"] for m in matches]
        ph = ",".join("?" * len(ids))
        async with _db.execute(
            f"SELECT match_id, COUNT(*) as n FROM predictions WHERE match_id IN ({ph}) GROUP BY match_id",
            ids,
        ) as cur:
            pred_counts = {r["match_id"]: r["n"] for r in await cur.fetchall()}

    rows_html = ""
    for m in matches:
        dt = datetime.fromtimestamp(m["kickoff_ts"], tz=timezone.utc).strftime("%d %b %Y") if m["kickoff_ts"] else "—"
        mid = _esc(m["match_id"])
        league_name = ALL_ESPN_LEAGUES.get(m["tournament"] or "", (None, ""))[1] or m.get("tournament", "")

        # Build match breakdown for inline display
        try:
            scorers = json.loads(m.get("res_try_scorers") or "[]") or []
        except Exception:
            scorers = []
        winner_label = {"home": m["team_home"], "away": m["team_away"], "draw": "Draw"}.get(m.get("res_winner",""), "—")
        btts_label = ("Yes" if m["res_btts"] else "No") if m.get("res_btts") is not None else None
        breakdown_rows = ""
        for label, val in [
            ("Winner",           winner_label if m.get("res_winner") else None),
            ("Margin",           m.get("res_margin")),
            ("Both Teams Scored", btts_label),
            ("First Try Scorer", m.get("res_first_try")),
        ]:
            if val:
                breakdown_rows += (f'<div class="mr-result-row">'
                                   f'<span class="mr-result-label">{label}</span>'
                                   f'<span class="mr-result-val">{_esc(val)}</span>'
                                   f'</div>')
        # Try scorers grouped by team (new format) or flat list (old format)
        if scorers:
            if scorers and isinstance(scorers[0], dict):
                by_team: dict[str, list[str]] = {}
                for s in scorers:
                    team = s.get("team", "")
                    label_val = f'{s["name"]} ({s["clock"]})' if s.get("clock") else s["name"]
                    by_team.setdefault(team, []).append(label_val)
                for team, names in by_team.items():
                    breakdown_rows += (f'<div class="mr-result-row">'
                                       f'<span class="mr-result-label">{_esc(team)}</span>'
                                       f'<span class="mr-result-val">{_esc(", ".join(names))}</span>'
                                       f'</div>')
            else:
                flat = ", ".join(str(s) for s in scorers)
                breakdown_rows += (f'<div class="mr-result-row">'
                                   f'<span class="mr-result-label">Try Scorers</span>'
                                   f'<span class="mr-result-val">{_esc(flat)}</span>'
                                   f'</div>')

        breakdown_html = (f'<div class="mr-result-block" style="border-top:none;padding-top:.5rem">'
                          f'<div class="mr-result-hdr">Match Breakdown</div>'
                          f'{breakdown_rows}</div>') if breakdown_rows else ""

        # Prediction count for secondary section label (precomputed above)
        cnt = pred_counts.get(m["match_id"], 0)
        preds_section = (f'<div class="hist-preds-toggle" onclick="event.stopPropagation();toggleHistPreds(this,\'{mid}\')">'
                         f'{cnt} prediction{"s" if cnt!=1 else ""} <span class="material-symbols-outlined" style="font-size:.85rem;vertical-align:middle">expand_more</span>'
                         f'</div>'
                         f'<div class="hist-preds-body" id="hp-{mid}"></div>') if cnt else ""

        rows_html += (
            f'<div class="hist-match-row" onclick="toggleHistMatch(this)">'
            f'<div class="hmr-main">'
            f'<span class="hmr-date">{dt}</span>'
            f'<span class="hmr-title">{_esc(m["match_title"])}</span>'
            f'<span class="hmr-score">{m["final_home"]}—{m["final_away"]}</span>'
            f'<span class="hmr-league">{_esc(league_name)}</span>'
            f'<span class="hmr-chevron material-symbols-outlined">expand_more</span>'
            f'</div>'
            f'<div class="hmr-body">{breakdown_html}{preds_section}</div>'
            f'</div>'
        )

    if not rows_html:
        rows_html = '<div style="color:var(--muted);font-size:.9rem;padding:1.5rem 0;text-align:center">No completed matches yet.</div>'

    return HTMLResponse(f"""<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>Results · Scrum</title>
{_FONTS}{_PWA_META}<style>{_BASE_CSS}
.page-body{{max-width:700px;margin:0 auto;padding:1.25rem 1rem 5rem}}
.page-title{{font-family:'Barlow Condensed',sans-serif;font-size:1.5rem;font-weight:700;letter-spacing:.04em;text-transform:uppercase;margin-bottom:1rem}}
.t-tabs{{display:flex;gap:.5rem;margin-bottom:1.25rem}}
.t-tab{{font-family:'Barlow Condensed',sans-serif;font-size:.82rem;font-weight:700;letter-spacing:.05em;text-transform:uppercase;padding:.35rem .9rem;border-radius:6px;border:1px solid var(--border);color:var(--muted);white-space:nowrap;transition:all .2s;flex-shrink:0}}
.t-tab:hover{{color:var(--text)}}.t-tab.active{{color:var(--header);border-color:var(--header);background:rgba(31,110,58,.08)}}
@media(max-width:768px){{.t-tabs{{overflow-x:auto;-webkit-overflow-scrolling:touch;scrollbar-width:none}}.t-tabs::-webkit-scrollbar{{display:none}}}}
.hist-match-row{{background:var(--surface);border:1px solid var(--border);border-radius:8px;margin-bottom:.4rem;overflow:hidden}}
.hmr-main{{display:flex;align-items:center;gap:.6rem;padding:.65rem .85rem;cursor:pointer;user-select:none;transition:background .15s}}
.hmr-main:hover{{background:var(--surface2)}}
.hist-match-row.open .hmr-main{{background:var(--surface2)}}
.hmr-date{{color:var(--muted);font-size:.72rem;min-width:58px;flex-shrink:0}}
.hmr-title{{flex:1;font-family:'Barlow Condensed',sans-serif;font-weight:700;font-size:.95rem}}
.hmr-score{{font-family:'Barlow Condensed',sans-serif;font-size:1.1rem;font-weight:700;color:var(--accent);flex-shrink:0}}
.hmr-league{{font-size:.68rem;color:var(--muted);font-family:'Barlow Condensed',sans-serif;letter-spacing:.05em;text-transform:uppercase;flex-shrink:0;display:none}}
@media(min-width:500px){{.hmr-league{{display:block}}}}
.hmr-chevron{{color:var(--muted);font-size:.95rem;transition:transform .2s;flex-shrink:0}}
.hist-match-row.open .hmr-chevron{{transform:rotate(180deg)}}
.hmr-body{{display:none;border-top:1px solid var(--border);padding:.75rem .85rem}}
.hist-match-row.open .hmr-body{{display:block}}
.hist-preds-toggle{{display:inline-flex;align-items:center;gap:.25rem;margin-top:.65rem;font-size:.78rem;color:var(--muted);cursor:pointer;user-select:none;font-family:'Barlow Condensed',sans-serif;font-weight:700;letter-spacing:.04em;text-transform:uppercase}}
.hist-preds-toggle:hover{{color:var(--text)}}
.hist-preds-body{{margin-top:.4rem}}
.mr-bd-row{{display:flex;align-items:center;gap:.75rem;padding:.45rem .5rem;border-bottom:1px solid var(--border);font-size:.83rem}}.mr-bd-row:last-child{{border-bottom:none}}
.mr-bd-row.me{{background:rgba(0,176,255,.04)}}
.mr-bd-name{{font-family:'Barlow Condensed',sans-serif;font-weight:600;flex:1}}
.mr-bd-pred{{font-family:'Barlow Condensed',sans-serif;font-weight:700;color:var(--accent3);min-width:50px;text-align:right}}
.mr-bd-diff{{font-size:.73rem;color:var(--muted);min-width:45px;text-align:right}}
.mr-bd-pts{{font-family:'Barlow Condensed',sans-serif;font-weight:700;font-size:.88rem;min-width:38px;text-align:right}}
.mr-bd-loading{{padding:.5rem;color:var(--muted);font-size:.82rem}}
</style></head><body>
{_nav(username, is_admin, "history")}
<div class="page-body">
  <div class="page-title">Results</div>
  <div class="t-tabs">{league_tabs}</div>
  {rows_html}
</div>
{_bnav("history", 0, is_admin)}
<script>
function toggleHistMatch(row){{
  row.classList.toggle('open');
}}
const _hpCache={{}};
async function toggleHistPreds(btn,mid){{
  const body=document.getElementById('hp-'+mid);
  if(body.style.display==='block'){{body.style.display='none';btn.querySelector('span').textContent='expand_more';return;}}
  body.style.display='block';
  btn.querySelector('span').textContent='expand_less';
  if(_hpCache[mid]){{body.innerHTML=_hpCache[mid];return;}}
  body.innerHTML='<div class="mr-bd-loading">Loading…</div>';
  try{{
    const r=await fetch('/api/match-predictions/'+encodeURIComponent(mid));
    const d=await r.json();
    if(!d.preds||!d.preds.length){{body.innerHTML='<div class="mr-bd-loading">No predictions recorded.</div>';return;}}
    let html='<div style="border:1px solid var(--border);border-radius:6px;overflow:hidden">';
    d.preds.forEach(p=>{{
      const isMe=p.username===d.me;
      const exact=p.exact_score?'<span style="font-size:.62rem;color:var(--accent3);font-weight:700;margin-left:.3rem">EXACT</span>':'';
      html+=`<div class="mr-bd-row${{isMe?' me':''}}"><span class="mr-bd-name">${{p.username.charAt(0).toUpperCase()+p.username.slice(1)}}${{exact}}</span><span class="mr-bd-pred">${{p.score_home}}–${{p.score_away}}</span><span class="mr-bd-diff">${{p.diff!=null?'±'+p.diff:'—'}}</span><span class="mr-bd-pts">${{p.points!=null?p.points+'pts':'—'}}</span></div>`;
    }});
    html+='</div>';
    _hpCache[mid]=html;body.innerHTML=html;
  }}catch(e){{body.innerHTML='<div class="mr-bd-loading">Failed to load.</div>';}}
}}
</script>
</body></html>""")


# ── Leaderboard ───────────────────────────────────────────────────────────────
@app.get("/leaderboard", response_class=HTMLResponse)
async def leaderboard(request: Request, t: str = "all"):
    username = _get_session_user(request)
    if not username:
        return RedirectResponse(url="/login", status_code=303)
    user = await _get_user(username)
    is_admin = user and user["is_admin"]

    if t != "all" and t not in TOURNAMENTS:
        t = "all"

    # Upcoming fixtures from ESPN
    espn_upcoming = []
    try:
        espn_upcoming = await _fetch_espn_upcoming()
    except Exception as exc:
        logger.warning("ESPN upcoming: %s", exc)

    # Collect custom competition match IDs from all of the user's groups
    user_groups = await _get_user_groups(username, include_global=False)
    all_custom_mids: set[str] = set()
    all_custom_match_meta: dict[str, dict] = {}
    for ug in user_groups:
        for cc in await _group_custom_comps(ug["id"]):
            for cm in await _custom_comp_matches(cc["id"]):
                all_custom_mids.add(cm["match_id"])
                all_custom_match_meta[cm["match_id"]] = cm

    now_ts = time.time()
    by_tourn: dict[str, list] = {k: [] for k in TOURNAMENTS}
    seen_slugs: set[str] = set()
    for m in espn_upcoming:
        kts = m.get("kickoff_ts") or 0
        if kts and now_ts >= kts:
            continue
        m_slug = m.get("slug") or re.sub(r"[^a-z0-9]+", "-", f"{m['team_home']}-vs-{m['team_away']}".lower()).strip("-")
        m["slug"] = m_slug
        in_tourn = m["tournament"] in by_tourn
        is_custom = m_slug in all_custom_mids
        if not in_tourn and not is_custom:
            continue
        if kts and kts > now_ts + 14 * 86400 and not is_custom:
            continue
        if in_tourn:
            by_tourn[m["tournament"]].append(m)
        elif is_custom:
            t = m["tournament"] if m["tournament"] in by_tourn else "international"
            by_tourn.setdefault(t, []).append(m)
        seen_slugs.add(m_slug)
    # Add custom comp matches not in ESPN window yet
    for mid, cm in all_custom_match_meta.items():
        if mid not in seen_slugs and cm["kickoff_ts"] > now_ts:
            t = cm["tournament"] if cm["tournament"] in by_tourn else "international"
            by_tourn.setdefault(t, []).append({
                "tournament": cm["tournament"], "tournament_name": ALL_ESPN_LEAGUES.get(cm["tournament"], (None, cm["tournament"]))[1],
                "espn_id": cm["espn_id"] or "", "league_id": cm["league_id"],
                "team_home": cm["team_home"], "team_away": cm["team_away"],
                "kickoff_ts": cm["kickoff_ts"], "in_progress": False,
                "slug": cm["match_id"],
            })

    # Fetch slugs the current user has already predicted (for fixture cards)
    user_pred_slugs: set[str] = set()
    espn_slugs = [
        m.get("slug") or re.sub(r"[^a-z0-9]+", "-", f"{m['team_home']}-vs-{m['team_away']}".lower()).strip("-")
        for m in espn_upcoming
    ]
    if espn_slugs:
        ph = ",".join("?" * len(espn_slugs))
        async with _db.execute(
            f"SELECT match_id FROM predictions WHERE username=? AND match_id IN ({ph})",
            [username] + espn_slugs,
        ) as cur:
            user_pred_slugs = {row["match_id"] for row in await cur.fetchall()}

    def _fix_card(m: dict) -> str:
        kts = m["kickoff_ts"]
        th, ta = m["team_home"], m["team_away"]
        t_slug = m.get("tournament", "")
        slug = m.get("slug") or re.sub(r"[^a-z0-9]+", "-", f"{th}-vs-{ta}".lower()).strip("-")
        pred_url = f"/predict/{_esc(slug)}?th={_esc(th)}&ta={_esc(ta)}&kts={int(kts)}&title={_esc(th+' vs '+ta)}&tournament={_esc(t_slug)}"
        t_str = f'<span class="fix-kick" data-ts="{kts}"></span>'
        window_open = (not kts) or (kts - 172800 <= now_ts < kts)
        already = slug in user_pred_slugs
        if already:
            status = f'<a class="fix-status fix-done" href="{pred_url}">Predicted ✓</a>'
        elif kts and now_ts < kts - 172800:
            status = f'<span class="fix-status fix-soon">Opens in {_time_until(kts - 172800)}</span>'
        elif m.get("in_progress"):
            status = f'<a class="fix-status fix-live" href="{pred_url}">In Progress · Predict</a>'
        elif window_open:
            status = f'<a class="fix-status fix-open" href="{pred_url}">Predict</a>'
        else:
            status = f'<a class="fix-status fix-closed" href="{pred_url}">Closed · View predictions</a>'
        return f'<div class="fix-row"><span class="fix-teams">{_esc(th)} <span class="fix-vs">vs</span> {_esc(ta)}</span><span class="fix-foot">{t_str}{status}</span></div>'

    # Count open unpredicted fixtures per tournament for notification badges
    unpredicted: dict[str, int] = {}
    for key, matches in by_tourn.items():
        count = 0
        for m in matches:
            kts = m["kickoff_ts"]
            slug = m.get("slug") or re.sub(r"[^a-z0-9]+", "-", f"{m['team_home']}-vs-{m['team_away']}".lower()).strip("-")
            window_open = (not kts) or (kts - 172800 <= now_ts < kts)
            if window_open and slug not in user_pred_slugs:
                count += 1
        if count:
            unpredicted[key] = count

    # Build tab bar: "All" first, then tournament tabs
    total_unpredicted = sum(unpredicted.values())
    all_badge = f'<span class="t-badge">{total_unpredicted}</span>' if total_unpredicted else ""
    tabs = f'<a href="/leaderboard?t=all" class="t-tab{"  active" if t == "all" else ""}">All{all_badge}</a>'
    for key, label in TOURNAMENTS.items():
        active_cls = " active" if key == t else ""
        badge = f'<span class="t-badge">{unpredicted[key]}</span>' if key in unpredicted else ""
        tabs += f'<a href="/leaderboard?t={key}" class="t-tab{active_cls}">{label}{badge}</a>'

    # ── "All" tab: show all upcoming fixtures grouped by tournament ──────────
    if t == "all":
        all_fixtures_html = ""
        for t_slug, t_name in TOURNAMENTS.items():
            if not by_tourn[t_slug]:
                continue
            cards = "".join(_fix_card(m) for m in by_tourn[t_slug])
            all_fixtures_html += f'<div class="fix-group"><div class="fix-t-name">{_esc(t_name)}</div>{cards}</div>'

        page_content = f"""<div class="page-section">
  <div class="section-banner sub collapsible" onclick="toggleSection(this)">Upcoming Fixtures<span class="material-symbols-outlined sb-chevron">expand_more</span></div>
  <div class="collapsible-body">
  <p style="color:var(--muted);font-size:.88rem;margin-bottom:1.25rem">Select a tournament tab to see standings and results.</p>
  <div class="fix-section">
    {all_fixtures_html if all_fixtures_html else '<div class="empty-msg" style="padding:1rem">No upcoming fixtures found.</div>'}
  </div>
  </div>
</div>"""

    else:
        # ── Tournament tab ───────────────────────────────────────────────────
        t_name = TOURNAMENTS[t]

        # 1. Upcoming fixtures for this tournament
        fix_cards = "".join(_fix_card(m) for m in by_tourn[t]) if by_tourn[t] else '<div class="empty-msg" style="padding:1rem">No upcoming fixtures found.</div>'
        fixtures_section = f"""<div class="page-section">
  <div class="section-banner sub collapsible" onclick="toggleSection(this)">Upcoming Fixtures<span class="material-symbols-outlined sb-chevron">expand_more</span></div>
  <div class="collapsible-body"><div class="fix-section">{fix_cards}</div></div>
</div>"""

        # 2. Pending predictions for this tournament (awaiting results)
        async with _db.execute(
            """SELECT p.match_id, p.match_title, p.kickoff_ts,
                      p.username, p.score_home, p.score_away
               FROM predictions p
               LEFT JOIN match_results r ON p.match_id = r.match_id
               WHERE r.match_id IS NULL
               AND p.match_id IN (
                   SELECT DISTINCT match_id FROM predictions WHERE tournament = ?
               )
               ORDER BY p.kickoff_ts DESC, p.created_at ASC""",
            (t,),
        ) as cur:
            pending_rows = [dict(r) for r in await cur.fetchall()]

        pending_section_html = ""
        if pending_rows:
            seen_pending: set = set()
            pending_cards = ""
            for row in pending_rows:
                mid = row["match_id"]
                if mid in seen_pending:
                    continue
                seen_pending.add(mid)
                dt = datetime.fromtimestamp(row["kickoff_ts"], tz=timezone.utc).strftime("%d %b %Y") if row["kickoff_ts"] else "—"
                pred_count = sum(1 for r in pending_rows if r["match_id"] == mid)
                mid_esc = _esc(mid)
                pending_cards += (
                    f'<div class="match-result-row" data-mid="{mid_esc}" onclick="togglePreds(this)">'
                    f'<span class="mr-date">{dt}</span>'
                    f'<span class="mr-title">{_esc(row["match_title"])}</span>'
                    f'<span class="mr-score" style="color:var(--muted);font-size:.8rem">Pending</span>'
                    f'<span class="mr-cnt">{pred_count} pick{"s" if pred_count != 1 else ""}</span>'
                    f'<span class="mr-chevron material-symbols-outlined">chevron_right</span>'
                    f'</div>'
                    f'<div class="mr-breakdown" id="bd-{mid_esc}"></div>'
                )
            pending_section_html = f"""<div class="page-section">
  <div class="section-banner sub collapsible collapsed" onclick="toggleSection(this)">Awaiting Results<span class="material-symbols-outlined sb-chevron">expand_more</span></div>
  <div class="collapsible-body collapsed">
  <div class="results-section">{pending_cards}</div>
  </div>
</div>"""

        # 3. Standings for this tournament
        async with _db.execute(
            """SELECT username,
                      SUM(points)           as total_points,
                      COUNT(*)              as played,
                      SUM(exact_score)      as exact_scores,
                      ROUND(AVG(diff),1)    as avg_diff,
                      SUM(pts_score)        as pts_score,
                      SUM(pts_winner)       as pts_winner,
                      SUM(pts_margin)       as pts_margin,
                      SUM(pts_btts)         as pts_btts,
                      SUM(pts_try_any)+SUM(pts_try_first) as pts_try,
                      SUM(pts_motm)         as pts_motm,
                      SUM(pts_banker)       as pts_banker
               FROM leaderboard WHERE group_id=1 AND tournament=?
               GROUP BY username
               ORDER BY total_points DESC, exact_scores DESC, avg_diff ASC""",
            (t,),
        ) as cur:
            standings = [dict(r) for r in await cur.fetchall()]

        if standings:
            # Determine which bonus columns have any data
            has_winner = any(s["pts_winner"] for s in standings)
            has_margin = any(s["pts_margin"] for s in standings)
            has_btts   = any(s["pts_btts"] for s in standings)
            has_try    = any(s["pts_try"] for s in standings)
            has_banker = any(s["pts_banker"] for s in standings)

            simple_s = ""
            detail_s = ""
            for i, s in enumerate(standings):
                medal = _medal(i + 1, 22)
                you_cls = ' class="lb-you"' if s["username"] == username else ""
                avg = s["avg_diff"] or "—"
                name_cell = f'{_avatar_el(s["username"],20)} {_esc(_ucfirst(s["username"]))}'
                simple_s += f"""<tr{you_cls}>
  <td class="lb-rank">{medal}</td>
  <td style="white-space:nowrap">{name_cell}</td>
  <td class="lb-num">{s['played']}</td>
  <td class="lb-num">{s['exact_scores']}</td>
  <td class="lb-num" style="font-size:.78rem">{avg}</td>
  <td class="lb-total">{s['total_points']}</td>
</tr>"""
                opt_tds = ""
                if has_winner: opt_tds += f'<td class="lb-num">{s["pts_winner"] or 0}</td>'
                if has_margin: opt_tds += f'<td class="lb-num">{s["pts_margin"] or 0}</td>'
                if has_btts:   opt_tds += f'<td class="lb-num">{s["pts_btts"] or 0}</td>'
                if has_try:    opt_tds += f'<td class="lb-num">{s["pts_try"] or 0}</td>'
                if has_banker: opt_tds += f'<td class="lb-num" style="color:var(--accent3)">+{s["pts_banker"] or 0}</td>'
                detail_s += f"""<tr{you_cls}>
  <td class="lb-rank">{medal}</td>
  <td style="white-space:nowrap">{name_cell}</td>
  <td class="lb-num">{s['played']}</td>
  <td class="lb-num">{s['exact_scores']}</td>
  <td class="lb-num">{s['pts_score'] or 0}</td>
  {opt_tds}
  <td class="lb-num" style="font-size:.78rem">{avg}</td>
  <td class="lb-total">{s['total_points']}</td>
</tr>"""
            opt_hdr = ""
            if has_winner: opt_hdr += '<th class="lb-num">Win</th>'
            if has_margin: opt_hdr += '<th class="lb-num">Mrg</th>'
            if has_btts:   opt_hdr += '<th class="lb-num">BTS</th>'
            if has_try:    opt_hdr += '<th class="lb-num">Try</th>'
            if has_banker: opt_hdr += '<th class="lb-num">🔒</th>'
            tid = f"lb-{t}"
            table_html = f"""<div style="display:flex;justify-content:flex-end;margin-bottom:.4rem">
  <button class="lb-toggle-btn" onclick="
    const s=document.getElementById('{tid}-s');
    const d=document.getElementById('{tid}-d');
    const open=d.style.display!=='none';
    d.style.display=open?'none':'block';
    s.style.display=open?'block':'none';
    this.textContent=open?'Full breakdown ▾':'Simple view ▴';
  ">Full breakdown ▾</button>
</div>
<div id="{tid}-s">
<table class="lb-table">
  <thead><tr>
    <th class="lb-rank"></th><th style="text-align:left">Player</th>
    <th class="lb-num">P</th><th class="lb-num">Exact</th>
    <th class="lb-num">Avg Diff</th><th class="lb-total">Pts</th>
  </tr></thead><tbody>{simple_s}</tbody>
</table></div>
<div id="{tid}-d" style="display:none"><div class="lb-scroll">
<table class="lb-table">
  <thead><tr>
    <th class="lb-rank"></th><th style="text-align:left">Player</th>
    <th class="lb-num">P</th><th class="lb-num">✓</th>
    <th class="lb-num">Score</th>{opt_hdr}
    <th class="lb-num">Avg</th><th class="lb-total">Pts</th>
  </tr></thead><tbody>{detail_s}</tbody>
</table></div></div>"""
        else:
            table_html = '<div class="empty-msg">No results entered yet — standings will appear here once the first game is resolved.</div>'

        standings_section = f"""<div class="page-section">
  <div class="section-banner sub collapsible collapsed" onclick="toggleSection(this)">Standings<span class="material-symbols-outlined sb-chevron">expand_more</span></div>
  <div class="collapsible-body collapsed">
  {table_html}
  </div>
</div>"""

        # 4. Recent results for this tournament
        async with _db.execute(
            """SELECT r.match_id, r.match_title, r.team_home, r.team_away, r.final_home, r.final_away,
                      r.kickoff_ts, COUNT(p.id) as pred_count
               FROM match_results r
               LEFT JOIN predictions p ON r.match_id = p.match_id
               WHERE r.tournament=?
               GROUP BY r.match_id
               ORDER BY r.kickoff_ts DESC LIMIT 15""",
            (t,),
        ) as cur:
            matches = [dict(r) for r in await cur.fetchall()]

        matches_html = ""
        if matches:
            m_rows = ""
            for m in matches:
                dt = datetime.fromtimestamp(m["kickoff_ts"], tz=timezone.utc).strftime("%d %b %Y") if m["kickoff_ts"] else ""
                mid = _esc(m["match_id"])
                cnt = m["pred_count"]
                m_rows += f"""<div class="match-result-row" data-mid="{mid}" onclick="togglePreds(this)">
  <span class="mr-date">{dt}</span>
  <span class="mr-title">{_esc(m['match_title'])}</span>
  <span class="mr-score">{m['final_home']}—{m['final_away']}</span>
  <span class="mr-cnt">{cnt} pick{"s" if cnt != 1 else ""}</span>
  <span class="mr-chevron material-symbols-outlined">chevron_right</span>
</div>
<div class="mr-breakdown" id="bd-{mid}"></div>"""
            matches_html = f"""<div class="page-section">
  <div class="section-banner sub collapsible collapsed" onclick="toggleSection(this)">Recent Results<span class="material-symbols-outlined sb-chevron">expand_more</span></div>
  <div class="collapsible-body collapsed">
  <div class="results-section">{m_rows}</div>
  </div>
</div>"""

        page_content = f"""{fixtures_section}
{standings_section}
{pending_section_html}
{matches_html}"""

    return HTMLResponse(f"""<!DOCTYPE html><html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Predictions · Scrum</title>{_FONTS}<style>{_BASE_CSS}
.page-main{{max-width:860px;margin:0 auto;padding:1.5rem 2rem}}
.t-tabs{{display:flex;gap:.5rem;flex-wrap:wrap;margin-bottom:1.5rem;padding-bottom:.35rem}}
.t-tab{{font-family:'Barlow Condensed',sans-serif;font-size:.85rem;font-weight:700;letter-spacing:.05em;text-transform:uppercase;padding:.4rem 1rem;border-radius:6px;border:1px solid var(--border);color:var(--muted);transition:all .2s;display:inline-flex;align-items:center;gap:.4rem;white-space:nowrap}}
@media(max-width:768px){{.t-tabs{{flex-wrap:nowrap;overflow-x:auto;-webkit-overflow-scrolling:touch;scrollbar-width:none}}.t-tabs::-webkit-scrollbar{{display:none}}.t-tab{{flex-shrink:0}}}}
.t-tab:hover{{color:var(--text);border-color:var(--muted)}}.t-tab.active{{color:var(--header);border-color:var(--header);background:rgba(31,110,58,.08)}}
.t-badge{{display:inline-flex;align-items:center;justify-content:center;min-width:1.1rem;height:1.1rem;padding:0 .3rem;border-radius:99px;background:var(--accent3);color:#fff;font-size:.6rem;font-weight:800;letter-spacing:0;line-height:1}}
.section-label{{font-family:'Barlow Condensed',sans-serif;font-size:.78rem;font-weight:700;letter-spacing:.08em;text-transform:uppercase;color:var(--muted);margin-bottom:.75rem}}
.lb-scroll{{overflow-x:auto;-webkit-overflow-scrolling:touch;margin:0 -.5rem;padding:0 .5rem}}
.lb-table{{width:100%;min-width:100%;border-collapse:collapse;font-size:.84rem;white-space:nowrap}}
.lb-table thead th{{font-family:'Barlow Condensed',sans-serif;font-size:.72rem;font-weight:700;letter-spacing:.07em;text-transform:uppercase;color:var(--muted);padding:.45rem .5rem;text-align:center;border-bottom:2px solid var(--border);background:var(--surface2)}}
.lb-table td{{padding:.6rem .5rem;border-bottom:1px solid var(--border)}}
.lb-table tbody tr:hover td{{background:var(--surface2)}}
.lb-you td{{background:rgba(31,110,58,.06)!important}}
.lb-rank{{width:2rem;text-align:center}}
.lb-num{{text-align:center;color:var(--muted)}}
.lb-total{{text-align:center;font-family:'Barlow Condensed',sans-serif;font-size:1.05rem;font-weight:700;color:var(--accent3)}}
.empty-msg{{color:var(--muted);font-size:.9rem;padding:2rem;text-align:center;background:var(--surface);border-radius:8px;border:1px solid var(--border);margin-bottom:1rem}}
.my-preds-wrap{{background:var(--surface);border:1px solid var(--border);border-radius:8px;overflow:hidden;margin-bottom:.75rem}}
.my-preds-hdr{{font-family:'Barlow Condensed',sans-serif;font-size:.8rem;font-weight:700;letter-spacing:.07em;text-transform:uppercase;color:var(--muted);padding:.6rem 1rem;border-bottom:1px solid var(--border)}}
.my-pred-row{{display:flex;align-items:center;gap:.75rem;padding:.6rem 1rem;border-bottom:1px solid var(--border);font-size:.88rem}}.my-pred-row:last-child{{border-bottom:none}}
.my-pred-date{{color:var(--muted);font-size:.8rem;min-width:36px}}
.my-pred-match{{flex:1;font-family:'Barlow Condensed',sans-serif;font-size:.95rem;font-weight:600}}
.my-pred-score{{font-family:'Barlow Condensed',sans-serif;font-size:1rem;font-weight:700;color:var(--accent3)}}
.results-section{{margin-top:.5rem}}
.match-result-row{{display:flex;align-items:center;gap:.75rem;padding:.6rem .75rem;background:var(--surface);border-radius:6px;margin-bottom:0;border:1px solid var(--border);font-size:.85rem;cursor:pointer;user-select:none;transition:background .15s,border-color .15s}}
.match-result-row:hover{{background:var(--surface2);border-color:rgba(0,176,255,.3)}}
.match-result-row.open{{border-bottom-left-radius:0;border-bottom-right-radius:0;border-bottom-color:transparent;border-color:rgba(0,176,255,.4);background:var(--surface2)}}
.mr-date{{color:var(--muted);font-size:.78rem;min-width:45px}}
.mr-title{{flex:1;font-family:'Barlow Condensed',sans-serif;font-weight:600}}
.mr-score{{font-family:'Barlow Condensed',sans-serif;font-size:1rem;font-weight:700;color:var(--accent)}}
.mr-cnt{{font-size:.72rem;color:var(--muted);white-space:nowrap}}
.mr-chevron{{color:var(--accent);font-size:1rem;transition:transform .2s;flex-shrink:0;opacity:.7}}
.match-result-row:hover .mr-chevron{{opacity:1}}
.match-result-row.open .mr-chevron{{transform:rotate(90deg);opacity:1}}
.mr-breakdown{{display:none;border:1px solid var(--border);border-top:none;border-radius:0 0 6px 6px;margin-bottom:.35rem;overflow:hidden}}
.mr-breakdown.open{{display:block}}
.mr-bd-row{{display:flex;align-items:center;gap:.75rem;padding:.5rem .75rem;border-bottom:1px solid var(--border);font-size:.84rem}}.mr-bd-row:last-child{{border-bottom:none}}
.mr-bd-row.me{{background:rgba(0,176,255,.05)}}
.mr-bd-name{{font-family:'Barlow Condensed',sans-serif;font-weight:600;font-size:.92rem;flex:1}}
.mr-bd-pred{{font-family:'Barlow Condensed',sans-serif;font-weight:700;color:var(--accent3);min-width:55px;text-align:right}}
.mr-bd-diff{{font-size:.75rem;color:var(--muted);min-width:48px;text-align:right}}
.mr-bd-pts{{font-family:'Barlow Condensed',sans-serif;font-weight:700;font-size:.9rem;min-width:40px;text-align:right}}
.mr-bd-exact{{font-size:.68rem;font-weight:700;letter-spacing:.05em;color:var(--accent3);background:rgba(255,145,0,.12);border:1px solid rgba(255,145,0,.25);border-radius:3px;padding:.05rem .3rem;margin-left:.25rem}}
.mr-bd-loading{{padding:.75rem;color:var(--muted);font-size:.83rem;text-align:center}}
.page-section{{margin-bottom:2rem}}
.section-banner{{font-family:'Barlow Condensed',sans-serif;font-size:1rem;font-weight:800;letter-spacing:.1em;text-transform:uppercase;color:var(--accent3);border-left:3px solid var(--accent3);padding:.1rem 0 .1rem .75rem;margin-bottom:.85rem;width:100%;box-sizing:border-box}}
.section-banner.sub{{color:var(--accent);border-left-color:var(--accent);font-size:.85rem;font-weight:700;margin-bottom:.65rem}}
.section-banner.collapsible{{cursor:pointer;display:flex;align-items:center;justify-content:space-between;user-select:none;padding-right:.5rem}}
.section-banner.collapsible:hover{{color:var(--accent3);opacity:.85}}
.section-banner .sb-chevron{{font-size:.9rem;transition:transform .2s;opacity:.6}}
.section-banner.collapsible.collapsed .sb-chevron{{transform:rotate(-90deg)}}
.collapsible-body{{overflow:hidden;transition:none}}
.collapsible-body.collapsed{{display:none}}
.fix-section{{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:1rem 1.2rem}}
.fix-group{{margin-bottom:.9rem}}.fix-group:last-child{{margin-bottom:0}}
.fix-t-name{{font-family:'Barlow Condensed',sans-serif;font-size:.72rem;font-weight:700;letter-spacing:.08em;text-transform:uppercase;color:var(--accent);margin-bottom:.3rem}}
.fix-row{{display:flex;align-items:center;gap:.5rem;padding:.5rem .6rem;border-radius:5px;background:var(--surface2);margin-bottom:.3rem;font-size:.83rem}}
.fix-teams{{flex:1;font-family:'Barlow Condensed',sans-serif;font-weight:600;text-align:center}}
.fix-vs{{color:var(--muted);font-weight:400;margin:0 .2rem}}
.fix-foot{{display:flex;align-items:center;gap:.4rem;flex-shrink:0}}
@media(max-width:600px){{.fix-row{{flex-direction:column;align-items:stretch;gap:.3rem;padding:.45rem .6rem}}.fix-foot{{justify-content:space-between}}}}
.fix-kick{{font-size:.7rem;color:var(--muted);white-space:nowrap}}
.fix-status{{font-family:'Barlow Condensed',sans-serif;font-size:.65rem;font-weight:700;letter-spacing:.05em;text-transform:uppercase;padding:.1rem .35rem;border-radius:3px}}
.lb-toggle-btn{{background:none;border:1px solid var(--border);color:var(--muted);font-family:'Barlow Condensed',sans-serif;font-size:.7rem;font-weight:700;letter-spacing:.06em;text-transform:uppercase;padding:.2rem .55rem;border-radius:5px;cursor:pointer}}
.lb-toggle-btn:hover{{color:var(--text);border-color:var(--muted)}}
.fix-live{{color:var(--accent2);background:rgba(0,230,118,.12);animation:pulse 2s infinite}}
.fix-open{{color:var(--accent3);background:rgba(232,103,28,.1);border:1px solid rgba(232,103,28,.25);text-decoration:none}}
.fix-closed{{color:var(--muted);background:rgba(141,159,188,.07)}}
.fix-soon{{color:var(--muted);background:rgba(141,159,188,.07)}}
.fix-done{{color:#4caf8a;background:rgba(76,175,138,.1);border:1px solid rgba(76,175,138,.25);text-decoration:none}}
@media(max-width:768px){{.page-main{{padding:1rem 1rem 80px}}}}
</style></head><body>
{_nav(username, is_admin, "leaderboard")}
<div class="page-main page-body">
  <div class="t-tabs">{tabs}</div>
  {page_content}
</div>
{_bnav("leaderboard", 0, is_admin)}
<script>
document.querySelectorAll('.fix-kick[data-ts]').forEach(el=>{{
  const ts=parseFloat(el.dataset.ts);if(!ts)return;
  const d=new Date(ts*1000),now=new Date();
  const t=d.toLocaleTimeString([],{{hour:'2-digit',minute:'2-digit'}});
  const today=new Date(now.getFullYear(),now.getMonth(),now.getDate());
  const tom=new Date(today);tom.setDate(tom.getDate()+1);
  const dDay=new Date(d.getFullYear(),d.getMonth(),d.getDate());
  let day;
  if(dDay.getTime()===today.getTime())day='Today';
  else if(dDay.getTime()===tom.getTime())day='Tomorrow';
  else day=d.toLocaleDateString([],{{weekday:'short',month:'short',day:'numeric'}});
  el.textContent=day+' '+t;
}});
function toggleSection(banner){{
  banner.classList.toggle('collapsed');
  const body=banner.nextElementSibling;
  if(body&&body.classList.contains('collapsible-body')){{
    body.classList.toggle('collapsed');
  }}
}}
const _predCache={{}};
async function togglePreds(row){{
  const mid=row.dataset.mid;
  const bd=document.getElementById('bd-'+mid);
  if(row.classList.contains('open')){{
    row.classList.remove('open');
    bd.classList.remove('open');
    return;
  }}
  row.classList.add('open');
  bd.classList.add('open');
  if(_predCache[mid]){{bd.innerHTML=_predCache[mid];return;}}
  bd.innerHTML='<div class="mr-bd-loading">Loading…</div>';
  try{{
    const r=await fetch('/api/match-predictions/'+encodeURIComponent(mid));
    const d=await r.json();
    if(!d.preds||!d.preds.length){{bd.innerHTML='<div class="mr-bd-loading">No predictions recorded.</div>';return;}}
    let html='';
    d.preds.forEach(p=>{{
      const isMe=p.username===d.me;
      const exact=p.exact_score?'<span class="mr-bd-exact">EXACT</span>':'';
      const diff=p.diff!=null?'diff '+p.diff:'—';
      const pts=p.points!=null?p.points+'pts':'—';
      html+=`<div class="mr-bd-row${{isMe?' me':''}}">`
        +`<span class="mr-bd-name">${{p.username.charAt(0).toUpperCase()+p.username.slice(1)}}${{exact}}</span>`
        +`<span class="mr-bd-pred">${{p.score_home}}–${{p.score_away}}</span>`
        +`<span class="mr-bd-diff">${{diff}}</span>`
        +`<span class="mr-bd-pts">${{pts}}</span>`
        +'</div>';
    }});
    if(d.result&&(d.result.winner||d.result.try_scorers?.length)){{
      const r=d.result;
      const winLabel=r.winner?r.winner.charAt(0).toUpperCase()+r.winner.slice(1):'—';
      const bttsLabel=r.btts!=null?(r.btts?'Yes':'No'):null;
      html+='<div class="mr-result-block">';
      html+='<div class="mr-result-hdr">Match Result</div>';
      const basic=[['Winner',winLabel],['Margin',r.margin||null],['Both Teams Scored',bttsLabel],['First Try Scorer',r.first_try||null]];
      basic.forEach(([l,v])=>{{if(v)html+=`<div class="mr-result-row"><span class="mr-result-label">${{l}}</span><span class="mr-result-val">${{v}}</span></div>`;}});
      if(r.try_details&&r.try_details.length){{
        const byTeam={{}};
        r.try_details.forEach(s=>{{if(!byTeam[s.team])byTeam[s.team]=[];byTeam[s.team].push(s.clock?`${{s.name}} (${{s.clock}})`:s.name);}});
        Object.entries(byTeam).forEach(([team,scorers])=>{{html+=`<div class="mr-result-row"><span class="mr-result-label">${{team}}</span><span class="mr-result-val">${{scorers.join(', ')}}</span></div>`;}});
      }}else if(r.try_scorers&&r.try_scorers.length){{
        html+=`<div class="mr-result-row"><span class="mr-result-label">Try Scorers</span><span class="mr-result-val">${{r.try_scorers.join(', ')}}</span></div>`;
      }}
      html+='</div>';
    }}
    _predCache[mid]=html;
    bd.innerHTML=html;
  }}catch(e){{bd.innerHTML='<div class="mr-bd-loading">Failed to load.</div>';}}
}}
</script>
</body></html>""")


# ── Admin ─────────────────────────────────────────────────────────────────────
async def _require_admin(request: Request):
    username = _get_session_user(request)
    user = await _get_user(username)
    if not user or not user["is_admin"]:
        return None, None
    return username, user


@app.get("/admin", response_class=HTMLResponse)
async def admin_page(request: Request):
    username, user = await _require_admin(request)
    if not username:
        return RedirectResponse(url="/", status_code=303)

    async with _db.execute(
        "SELECT id, username, is_admin, created_at FROM users ORDER BY created_at"
    ) as cur:
        users = [dict(r) for r in await cur.fetchall()]

    async with _db.execute(
        """SELECT p.match_id, p.match_title, p.team_home, p.team_away, p.kickoff_ts,
                  COUNT(p.id) as pred_count
           FROM predictions p
           LEFT JOIN match_results r ON p.match_id = r.match_id
           WHERE r.match_id IS NULL
           GROUP BY p.match_id
           ORDER BY p.kickoff_ts DESC"""
    ) as cur:
        pending = [dict(r) for r in await cur.fetchall()]

    async with _db.execute(
        "SELECT * FROM match_results ORDER BY entered_at DESC LIMIT 20"
    ) as cur:
        resolved = [dict(r) for r in await cur.fetchall()]

    # Auto-fetch status
    laf = _last_auto_fetch
    if laf["ts"]:
        ago = int(time.time() - laf["ts"])
        ago_str = f"{ago // 60}m ago" if ago < 3600 else f"{ago // 3600}h ago"
        auto_status = f"Last fetch {ago_str}: {laf['applied']} applied / {laf['checked']} checked"
    else:
        auto_status = "Never run — runs automatically every hour"

    # Users section
    user_rows = ""
    for u in users:
        admin_badge = ' <span class="tag" style="background:rgba(31,110,58,.12);color:var(--header)">ADMIN</span>' if u["is_admin"] else ""
        del_btn = ""
        login_link_btn = f'<a href="/admin/login-link/{_esc(u["username"])}" class="btn btn-sm btn-ghost" style="margin-right:.35rem">Login Link</a>'
        if u["username"] != username:
            del_btn = f"""<form method="post" action="/admin/users/delete" style="display:inline">
  <input type="hidden" name="del_username" value="{_esc(u['username'])}">
  <button type="submit" class="btn btn-sm btn-danger">Delete</button>
</form>"""
        user_rows += f"""<div class="admin-row">
  <span class="ar-name">{_esc(u['username'])}{admin_badge}</span>
  {login_link_btn}{del_btn}
</div>"""

    # Pending matches section
    pending_html = ""
    if pending:
        t_options = "".join(f'<option value="{k}">{v}</option>' for k, v in TOURNAMENTS.items())
        for m in pending:
            dt = datetime.fromtimestamp(m["kickoff_ts"], tz=timezone.utc).strftime("%d %b %Y %H:%M") if m["kickoff_ts"] else "Unknown"
            pending_html += f"""<details class="pending-match">
  <summary>
    <span class="pm-title">{_esc(m['match_title'])}</span>
    <span class="pm-meta">{dt} UTC · {m['pred_count']} predictions</span>
  </summary>
  <form method="post" action="/admin/results/enter" class="result-form">
    <input type="hidden" name="match_id" value="{_esc(m['match_id'])}">
    <input type="hidden" name="kickoff_ts" value="{m['kickoff_ts']}">
    <div class="rf-row">
      <div class="rf-field">
        <label>Match Title</label>
        <input type="text" name="match_title" value="{_esc(m['match_title'])}" required style="width:100%">
      </div>
    </div>
    <div class="rf-row">
      <div class="rf-field">
        <label>Home Team</label>
        <input type="text" name="team_home" value="{_esc(m['team_home'])}" required>
      </div>
      <div class="rf-field">
        <label>Away Team</label>
        <input type="text" name="team_away" value="{_esc(m['team_away'])}" required>
      </div>
    </div>
    <div class="rf-row">
      <div class="rf-field">
        <label>Tournament</label>
        <select name="tournament" required>{t_options}</select>
      </div>
      <div class="rf-field" style="flex:.5">
        <label>Final: Home</label>
        <input type="number" name="final_home" min="0" max="300" required style="width:80px">
      </div>
      <div class="rf-field" style="flex:.5">
        <label>Final: Away</label>
        <input type="number" name="final_away" min="0" max="300" required style="width:80px">
      </div>
    </div>
    <button type="submit" class="btn btn-sm">Enter Result & Calculate Points</button>
  </form>
</details>"""
    else:
        pending_html = '<div style="color:var(--muted);font-size:.85rem">No pending matches.</div>'

    # Resolved matches
    resolved_html = ""
    for r in resolved:
        t_name = ALL_ESPN_LEAGUES.get(r["tournament"], (None, r["tournament"]))[1] if r["tournament"] else r["tournament"]
        resolved_html += f"""<details class="pending-match">
  <summary>
    <span class="pm-title">{_esc(r['match_title'])}</span>
    <span class="pm-meta">{_esc(t_name or '')}</span>
    <span style="font-family:'Barlow Condensed',sans-serif;font-weight:700;color:var(--header)">{r['final_home']}–{r['final_away']}</span>
  </summary>
  <div class="result-form">
    <div style="font-size:.8rem;color:var(--muted);margin-bottom:.5rem">Correct a wrong result — all group scores will recalculate.</div>
    <form method="post" action="/admin/results/enter">
      <input type="hidden" name="match_id" value="{_esc(r['match_id'])}">
      <input type="hidden" name="match_title" value="{_esc(r['match_title'])}">
      <input type="hidden" name="team_home" value="{_esc(r['team_home'])}">
      <input type="hidden" name="team_away" value="{_esc(r['team_away'])}">
      <input type="hidden" name="tournament" value="{_esc(r['tournament'] or '')}">
      <input type="hidden" name="kickoff_ts" value="{r['kickoff_ts']}">
      <div class="rf-row">
        <div class="rf-field"><label>{_esc(r['team_home'])}</label><input type="number" name="final_home" value="{r['final_home']}" min="0" max="200" required></div>
        <div class="rf-field"><label>{_esc(r['team_away'])}</label><input type="number" name="final_away" value="{r['final_away']}" min="0" max="200" required></div>
      </div>
      <button type="submit" class="btn btn-sm">Update Result</button>
    </form>
    <form method="post" action="/admin/rescore/{_esc(r['match_id'])}" style="margin-top:.5rem">
      <button type="submit" class="btn btn-sm btn-ghost">⟳ Rescore All Groups</button>
    </form>
  </div>
</details>"""
    if not resolved_html:
        resolved_html = '<div style="color:var(--muted);font-size:.85rem">No results entered yet.</div>'

    return HTMLResponse(f"""<!DOCTYPE html><html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Admin · Scrum</title>{_FONTS}<style>{_BASE_CSS}
main{{max-width:780px;margin:2rem auto;padding:0 1.5rem;display:flex;flex-direction:column;gap:2rem}}
.admin-section{{background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:1.25rem}}
.section-title{{font-family:'Barlow Condensed',sans-serif;font-size:1rem;font-weight:700;letter-spacing:.06em;text-transform:uppercase;margin-bottom:1rem;color:var(--text)}}
.admin-row{{display:flex;align-items:center;gap:.75rem;padding:.5rem .75rem;border-radius:6px;background:var(--surface2);margin-bottom:.4rem;flex-wrap:wrap}}
.ar-name{{flex:1;font-family:'Barlow Condensed',sans-serif;font-weight:600;font-size:.95rem}}
.create-form{{display:flex;gap:.6rem;flex-wrap:wrap;align-items:flex-end;margin-top:.75rem;padding-top:.75rem;border-top:1px solid var(--border)}}
.create-form label{{font-size:.75rem;color:var(--muted);display:block;margin-bottom:.25rem}}
.create-form input{{width:160px}}
.admin-check{{display:flex;align-items:center;gap:.4rem;font-size:.82rem;color:var(--muted)}}
.pending-match{{background:var(--surface2);border:1px solid var(--border);border-radius:6px;margin-bottom:.5rem;overflow:hidden}}
.pending-match summary{{padding:.7rem 1rem;cursor:pointer;display:flex;align-items:center;gap:.75rem;flex-wrap:wrap;user-select:none}}
.pending-match summary:hover{{background:var(--surface)}}
.pm-title{{font-family:'Barlow Condensed',sans-serif;font-weight:600;flex:1}}
.pm-meta{{font-size:.75rem;color:var(--muted)}}
.result-form{{padding:.75rem 1rem 1rem;border-top:1px solid var(--border);display:flex;flex-direction:column;gap:.65rem}}
.rf-row{{display:flex;gap:.65rem;flex-wrap:wrap}}
.rf-field{{display:flex;flex-direction:column;gap:.25rem;flex:1}}
.rf-field label{{font-size:.75rem;color:var(--muted)}}
.rf-field input,.rf-field select{{width:100%}}
</style></head><body>
{_nav(username, True, "admin")}
<main>
  <div class="admin-section">
    <div class="section-title">Users</div>
    {user_rows}
    <form method="post" action="/admin/users/create" class="create-form">
      <div>
        <label>Username</label>
        <input type="text" name="new_username" required placeholder="username">
      </div>
      <div>
        <label>Password</label>
        <input type="password" name="new_password" required placeholder="password">
      </div>
      <div class="admin-check">
        <input type="checkbox" name="is_admin" value="1" id="ia">
        <label for="ia">Admin</label>
      </div>
      <button type="submit" class="btn btn-sm">Create User</button>
    </form>
  </div>

  <div class="admin-section">
    <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:1rem">
      <div class="section-title" style="margin-bottom:0">Match Results</div>
      <div style="display:flex;align-items:center;gap:.5rem;flex-wrap:wrap">
        <span style="font-size:.75rem;color:var(--muted)">{auto_status}</span>
        <form method="post" action="/admin/results/auto-fetch" style="display:inline">
          <button type="submit" class="btn btn-sm">⟳ Auto-Fetch Results</button>
        </form>
        <form method="post" action="/admin/squads/pre-fetch" style="display:inline">
          <button type="submit" class="btn btn-sm btn-ghost">⟳ Fetch Squads</button>
        </form>
      </div>
    </div>
    {pending_html}
  </div>

  <div class="admin-section">
    <div class="section-title">Resolved Matches</div>
    {resolved_html}
  </div>
</main></body></html>""")


@app.post("/admin/results/auto-fetch")
async def admin_auto_fetch(request: Request):
    username, _ = await _require_admin(request)
    if not username:
        return RedirectResponse(url="/", status_code=303)
    checked, applied = await _auto_apply_results()
    _last_auto_fetch.update({"ts": time.time(), "checked": checked, "applied": applied})
    return RedirectResponse(url="/admin", status_code=303)


@app.post("/admin/squads/pre-fetch")
async def admin_pre_fetch_squads(request: Request):
    username, _ = await _require_admin(request)
    if not username:
        return RedirectResponse(url="/", status_code=303)
    await _redis.delete("espn:upcoming")  # force fresh fetch with espn_id populated
    await _pre_fetch_squads()
    return RedirectResponse(url="/admin", status_code=303)


@app.post("/admin/users/create")
async def admin_create_user(
    request: Request,
    new_username: str = Form(...),
    new_password: str = Form(...),
    is_admin: str = Form(default=""),
):
    username, _ = await _require_admin(request)
    if not username:
        return RedirectResponse(url="/", status_code=303)
    try:
        await _db.execute(
            "INSERT INTO users (username,password_hash,is_admin,created_at) VALUES(?,?,?,?)",
            (new_username.lower().strip(), pwd_ctx.hash(new_password), 1 if is_admin else 0, time.time()),
        )
        await _db.commit()
    except aiosqlite.IntegrityError:
        pass
    return RedirectResponse(url="/admin", status_code=303)


@app.post("/admin/users/delete")
async def admin_delete_user(request: Request, del_username: str = Form(...)):
    username, _ = await _require_admin(request)
    if not username:
        return RedirectResponse(url="/", status_code=303)
    if del_username != username:
        await _db.execute("DELETE FROM users WHERE username=?", (del_username,))
        await _db.execute("DELETE FROM group_members WHERE username=?", (del_username,))
        await _db.execute("DELETE FROM friends WHERE username=? OR friend_username=?", (del_username, del_username))
        await _db.execute("DELETE FROM predictions WHERE username=?", (del_username,))
        await _db.execute("DELETE FROM leaderboard WHERE username=?", (del_username,))
        await _db.execute("DELETE FROM invite_tokens WHERE username=?", (del_username,))
        await _db.execute("DELETE FROM group_invites WHERE created_by=? OR invited_username=?", (del_username, del_username))
        await _db.execute("DELETE FROM season_leaderboard WHERE username=?", (del_username,))
        await _db.commit()
    return RedirectResponse(url="/admin", status_code=303)


@app.get("/admin/invite-link/{target_username}", response_class=HTMLResponse)
async def admin_invite_link(request: Request, target_username: str):
    username, _ = await _require_admin(request)
    if not username:
        return RedirectResponse(url="/", status_code=303)
    async with _db.execute("SELECT username FROM users WHERE username=?", (target_username,)) as cur:
        if not await cur.fetchone():
            return RedirectResponse(url="/admin", status_code=303)
    # Return existing unused token or create new one
    async with _db.execute(
        "SELECT token FROM invite_tokens WHERE username=? AND used_at IS NULL ORDER BY created_at DESC LIMIT 1",
        (target_username,),
    ) as cur:
        row = await cur.fetchone()
    if row:
        token = row["token"]
    else:
        token = secrets.token_urlsafe(32)
        await _db.execute(
            "INSERT INTO invite_tokens (token, username, created_at) VALUES(?,?,?)",
            (token, target_username, time.time()),
        )
        await _db.commit()
    base = _base_url(request)
    invite_url = f"{base}/join/{token}"
    return HTMLResponse(f"""<!DOCTYPE html><html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Invite Link · Scrum</title>{_FONTS}<style>{_BASE_CSS}
main{{max-width:500px;margin:3rem auto;padding:0 1.5rem;display:flex;flex-direction:column;gap:1.5rem}}
.invite-box{{background:var(--surface);border:1px solid rgba(0,176,255,.3);border-radius:10px;padding:2rem;text-align:center;display:flex;flex-direction:column;gap:1rem}}
.inv-title{{font-family:'Barlow Condensed',sans-serif;font-size:1.2rem;font-weight:700;letter-spacing:.05em;text-transform:uppercase}}
.inv-for{{font-size:.88rem;color:var(--muted)}}
.inv-url{{background:var(--surface2);border:1px solid var(--border);border-radius:6px;padding:.75rem 1rem;font-size:.8rem;word-break:break-all;text-align:left;color:var(--accent);cursor:pointer}}
.inv-note{{font-size:.75rem;color:var(--muted)}}
</style></head><body>
{_nav(username, True, "admin")}
<main>
  <div class="invite-box">
    <div class="inv-title">Invite Link</div>
    <div class="inv-for">For <strong>{_esc(target_username)}</strong></div>
    <div class="inv-url" id="inv-url" onclick="copyLink()" title="Click to copy">{_esc(invite_url)}</div>
    <button class="btn" onclick="copyLink()" id="copy-btn">Copy Link</button>
    <div class="inv-note">Single-use · expires after first tap · creates a permanent session on that device</div>
  </div>
  <div style="text-align:center"><a href="/admin" class="nav-link">← Back to Admin</a></div>
</main>
<script>
function copyLink(){{
  navigator.clipboard.writeText(document.getElementById('inv-url').textContent.trim())
    .then(()=>document.getElementById('copy-btn').textContent='Copied!')
    .catch(()=>document.getElementById('copy-btn').textContent='Select manually');
}}
</script>
</body></html>""")


@app.post("/admin/results/enter")
async def admin_enter_result(
    request: Request,
    match_id: str = Form(...),
    match_title: str = Form(...),
    team_home: str = Form(...),
    team_away: str = Form(...),
    tournament: str = Form(...),
    kickoff_ts: float = Form(default=0),
    final_home: int = Form(...),
    final_away: int = Form(...),
):
    username, _ = await _require_admin(request)
    if not username:
        return RedirectResponse(url="/", status_code=303)

    winner = "home" if final_home > final_away else ("away" if final_away > final_home else "draw")
    margin = _calc_margin_band(abs(final_home - final_away))
    btts = 1 if final_home > 0 and final_away > 0 else 0

    # Preserve existing try scorer data if result already exists
    async with _db.execute(
        "SELECT res_try_scorers, res_first_try FROM match_results WHERE match_id=?", (match_id,)
    ) as cur:
        existing = await cur.fetchone()
    existing = dict(existing) if existing else {}

    await _db.execute(
        """INSERT OR REPLACE INTO match_results
           (match_id,match_title,team_home,team_away,tournament,kickoff_ts,
            final_home,final_away,entered_by,entered_at,
            res_winner,res_margin,res_btts,
            res_try_scorers,res_first_try,motm_pending)
           VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (match_id, match_title.strip(), team_home.strip(), team_away.strip(),
         tournament, kickoff_ts, final_home, final_away, username, time.time(),
         winner, margin, btts,
         existing.get("res_try_scorers"), existing.get("res_first_try"),
         0),
    )
    await _db.commit()

    try_scorers = json.loads(existing.get("res_try_scorers") or "[]") or []
    first_try = existing.get("res_first_try")

    async with _db.execute("SELECT id FROM groups") as cur:
        group_ids = [r["id"] for r in await cur.fetchall()]
    for gid in group_ids:
        try:
            await _score_group(gid, match_id, tournament, final_home, final_away,
                               winner, margin, btts, try_scorers, first_try)
        except Exception as exc:
            logger.warning("Score group %d error for %s: %s", gid, match_id, exc)

    return RedirectResponse(url="/admin", status_code=303)


@app.post("/admin/rescore/{match_id}")
async def admin_rescore_match(request: Request, match_id: str):
    """Re-score all groups for a completed match using stored result data."""
    username, _ = await _require_admin(request)
    if not username:
        return RedirectResponse(url="/", status_code=303)

    async with _db.execute("SELECT * FROM match_results WHERE match_id=?", (match_id,)) as cur:
        r = await cur.fetchone()
    if not r:
        return RedirectResponse(url="/admin", status_code=303)
    r = dict(r)

    try_scorers = json.loads(r.get("res_try_scorers") or "[]") or []
    first_try = r.get("res_first_try")
    winner = r["res_winner"]
    margin = r["res_margin"]
    btts = r["res_btts"]

    async with _db.execute("SELECT id FROM groups") as cur:
        group_ids = [row["id"] for row in await cur.fetchall()]
    for gid in group_ids:
        try:
            await _score_group(gid, match_id, r["tournament"],
                               r["final_home"], r["final_away"],
                               winner, margin, btts, try_scorers, first_try)
        except Exception as exc:
            logger.warning("Rescore group %d for %s: %s", gid, match_id, exc)

    logger.info("Admin rescore: %s — scored %d groups", match_id, len(group_ids))
    return RedirectResponse(url="/admin", status_code=303)


# ── Username change ───────────────────────────────────────────────────────────
@app.post("/me/username")
async def me_change_username(request: Request, new_username: str = Form(...)):
    username = _get_session_user(request)
    new_username = new_username.strip().lower()

    if not new_username:
        return RedirectResponse(url="/me?un=empty", status_code=303)
    if not re.match(r'^[a-z0-9][a-z0-9_.-]*[a-z0-9]$|^[a-z0-9]$', new_username):
        return RedirectResponse(url="/me?un=invalid", status_code=303)
    if new_username == username:
        return RedirectResponse(url="/me", status_code=303)

    async with _db.execute("SELECT id FROM users WHERE username=?", (new_username,)) as cur:
        if await cur.fetchone():
            return RedirectResponse(url="/me?un=taken", status_code=303)

    # Also check for orphaned references to new_username that would cause conflicts
    async with _db.execute(
        "SELECT COUNT(*) as n FROM group_members WHERE username=?", (new_username,)
    ) as cur:
        if (await cur.fetchone())["n"] > 0:
            # Orphaned entry exists — clean it up silently (user was previously deleted)
            await _db.execute("DELETE FROM group_members WHERE username=?", (new_username,))
            await _db.commit()

    # All updates in one transaction — rollback everything if any step fails
    try:
        for sql, params in [
            ("UPDATE users SET username=? WHERE username=?",                         (new_username, username)),
            ("UPDATE predictions SET username=? WHERE username=?",                   (new_username, username)),
            ("UPDATE leaderboard SET username=? WHERE username=?",                   (new_username, username)),
            ("UPDATE group_members SET username=? WHERE username=?",                 (new_username, username)),
            ("UPDATE friends SET username=? WHERE username=?",                       (new_username, username)),
            ("UPDATE friends SET friend_username=? WHERE friend_username=?",         (new_username, username)),
            ("UPDATE group_invites SET created_by=? WHERE created_by=?",             (new_username, username)),
            ("UPDATE group_invites SET used_by=? WHERE used_by=?",                   (new_username, username)),
            ("UPDATE group_invites SET invited_username=? WHERE invited_username=?", (new_username, username)),
            ("UPDATE season_leaderboard SET username=? WHERE username=?",            (new_username, username)),
            ("UPDATE invite_tokens SET username=? WHERE username=?",                 (new_username, username)),
            ("UPDATE groups SET created_by=? WHERE created_by=?",                   (new_username, username)),
        ]:
            await _db.execute(sql, params)
        await _db.commit()
        logger.info("Username changed: %s → %s", username, new_username)
    except Exception as exc:
        await _db.rollback()
        logger.warning("Username change failed %s → %s: %s", username, new_username, exc)
        return RedirectResponse(url="/me?un=error", status_code=303)

    # Issue a new session cookie with the new username
    resp = RedirectResponse(url="/me?un=ok", status_code=303)
    resp.set_cookie(SESSION_COOKIE, _make_token(new_username),
                    max_age=SESSION_MAX_AGE, httponly=True, samesite="lax")
    return resp


# ── Password change ───────────────────────────────────────────────────────────
@app.post("/me/password")
async def me_change_password(
    request: Request,
    current_password: str = Form(...),
    new_password: str = Form(...),
):
    username = _get_session_user(request)
    async with _db.execute("SELECT password_hash FROM users WHERE username=?", (username,)) as cur:
        row = await cur.fetchone()
    if not row or not pwd_ctx.verify(current_password, row["password_hash"]):
        return RedirectResponse(url="/me?pw=wrong", status_code=303)
    if len(new_password) < 6:
        return RedirectResponse(url="/me?pw=short", status_code=303)
    await _db.execute(
        "UPDATE users SET password_hash=? WHERE username=?",
        (pwd_ctx.hash(new_password), username),
    )
    await _db.commit()
    return RedirectResponse(url="/me?pw=ok", status_code=303)


# ── Admin login link (password reset via share) ────────────────────────────────
@app.get("/admin/login-link/{target_username}", response_class=HTMLResponse)
async def admin_login_link(request: Request, target_username: str):
    username, _ = await _require_admin(request)
    if not username:
        return RedirectResponse(url="/", status_code=303)
    async with _db.execute("SELECT username FROM users WHERE username=?", (target_username,)) as cur:
        if not await cur.fetchone():
            return RedirectResponse(url="/admin", status_code=303)
    token = secrets.token_urlsafe(24)
    await _db.execute(
        "INSERT INTO invite_tokens (token,username,created_at) VALUES(?,?,?)",
        (token, target_username, time.time()),
    )
    await _db.commit()
    base = _base_url(request)
    login_url = f"{base}/join/{token}"
    user = await _get_user(username)
    is_admin = user and user["is_admin"]
    return HTMLResponse(f"""<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Login Link · Admin</title>{_FONTS}{_PWA_META}<style>{_BASE_CSS}
.page-body{{max-width:560px;margin:0 auto;padding:1.25rem 1rem}}
.page-title{{font-family:'Barlow Condensed',sans-serif;font-size:1.4rem;font-weight:700;letter-spacing:.04em;text-transform:uppercase;margin-bottom:1.1rem}}
.link-box{{background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:1.1rem}}
.link-url{{background:var(--surface2);border:1px solid var(--border);border-radius:8px;padding:.65rem .9rem;font-size:.82rem;word-break:break-all;color:var(--header);margin:.75rem 0}}
</style></head><body>
{_nav(username, True, "admin")}
<div class="page-body">
  <div class="page-title">Login Link — {_esc(_ucfirst(target_username))}</div>
  <div class="link-box">
    <div style="font-size:.88rem;color:var(--muted)">Share this with {_esc(_ucfirst(target_username))} via WhatsApp or Telegram. It logs them straight in — one time use.</div>
    <div class="link-url">{_esc(login_url)}</div>
    <button class="btn btn-sm" onclick="navigator.clipboard.writeText('{_esc(login_url)}').then(()=>this.textContent='Copied!')">Copy Link</button>
  </div>
  <div style="margin-top:1rem"><a href="/admin" class="btn btn-ghost btn-sm">← Back to Admin</a></div>
</div>
{_bnav("", 0, True)}
</body></html>""")


# ── Avatar serve + upload ─────────────────────────────────────────────────────
@app.get("/avatar/{username}")
async def serve_avatar(username: str):
    path = os.path.join(AVATAR_DIR, f"{username}.jpg")
    if not os.path.exists(path):
        return Response(status_code=404)
    with open(path, "rb") as f:
        return Response(content=f.read(), media_type="image/jpeg",
                        headers={"Cache-Control": "public, max-age=86400"})


@app.post("/me/avatar")
async def upload_avatar(request: Request, file: UploadFile = File(...)):
    username = _get_session_user(request)
    if not username:
        return Response(status_code=401)
    data = await file.read(4 * 1024 * 1024)  # 4MB max
    if len(data) == 0:
        return Response(status_code=400)
    try:
        from PIL import Image, ImageFile
        import io
        ImageFile.LOAD_TRUNCATED_IMAGES = True
        img = Image.open(io.BytesIO(data)).convert("RGB")
        # Resize to 256x256 if not already (canvas sends exact size, originals may vary)
        if img.size != (256, 256):
            side = min(img.size)
            w, h = img.size
            img = img.crop(((w - side) // 2, (h - side) // 2, (w + side) // 2, (h + side) // 2))
            img = img.resize((256, 256), Image.LANCZOS)
        out = io.BytesIO()
        img.save(out, "JPEG", quality=88)
        os.makedirs(AVATAR_DIR, exist_ok=True)
        with open(os.path.join(AVATAR_DIR, f"{username}.jpg"), "wb") as f:
            f.write(out.getvalue())
    except Exception as exc:
        logger.warning("Avatar upload error for %s: %s", username, exc)
        return Response(status_code=500)
    return Response(status_code=200, content=b"ok")


# ── Health / misc ─────────────────────────────────────────────────────────────
@app.get("/favicon.ico")
async def favicon():
    return Response(status_code=204)


@app.get("/health")
async def health():
    return {"status": "ok"}


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    uvicorn.run("server:app", host="0.0.0.0", port=SERVER_PORT,
                log_level="warning", access_log=False)
