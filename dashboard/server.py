"""
server.py — Rugby Streams: dashboard, chat, predictions, leaderboard.
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
from fastapi import FastAPI, Form, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
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

TOURNAMENTS = {
    "six-nations":        "Six Nations",
    "rugby-championship": "The Rugby Championship",
    "super-rugby":        "Super Rugby",
    "urc":                "URC",
    "world-cup":          "Rugby World Cup",
    "international":      "International",
}

# ESPN league IDs for the leaderboard/predictions feature
ESPN_LEAGUES = {
    "six-nations":        180659,
    "rugby-championship": 244293,
    "super-rugby":        242041,
    "urc":                270557,
    "world-cup":          164205,
    "international":      289234,
}

# Standings page: tab order and ESPN league IDs
STANDINGS_TABS = [
    ("world-cup",          "World Cup",          164205),
    ("six-nations",        "Six Nations",         180659),
    ("rugby-championship", "Rugby Championship",  244293),
    ("super-rugby",        "Super Rugby",         242041),
    ("urc",                "URC",                 270557),
    ("premiership",        "Premiership",         267979),
    ("top-14",             "Top 14",              270559),
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
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    match_id     TEXT NOT NULL,
    match_title  TEXT NOT NULL,
    team_home    TEXT NOT NULL,
    team_away    TEXT NOT NULL,
    kickoff_ts   REAL NOT NULL DEFAULT 0,
    tournament   TEXT,
    username     TEXT NOT NULL,
    score_home   INTEGER NOT NULL,
    score_away   INTEGER NOT NULL,
    created_at   REAL NOT NULL,
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
    entered_at   REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS leaderboard (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    match_id     TEXT NOT NULL,
    tournament   TEXT NOT NULL,
    username     TEXT NOT NULL,
    diff         INTEGER NOT NULL,
    exact_score  INTEGER NOT NULL DEFAULT 0,
    points       INTEGER NOT NULL,
    created_at   REAL NOT NULL,
    UNIQUE(match_id, username)
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
        await _db.execute("UPDATE users SET password_hash=? WHERE username=?", (new_hash, admin))
    else:
        await _db.execute(
            "INSERT INTO users (username,password_hash,is_admin,created_at) VALUES(?,?,1,?)",
            (admin, new_hash, time.time()),
        )
    await _db.commit()
    # Migration: add tournament column to predictions if missing
    try:
        await _db.execute("ALTER TABLE predictions ADD COLUMN tournament TEXT")
        await _db.commit()
    except Exception:
        pass
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
    """Fetch completed results from ESPN for all four leaderboard tournaments."""
    results = []
    end = datetime.now(timezone.utc) + timedelta(days=1)  # +1: ESPN end date is exclusive
    start = end - timedelta(days=days_back + 1)
    date_range = f"{start.strftime('%Y%m%d')}-{end.strftime('%Y%m%d')}"
    for slug, league_id in ESPN_LEAGUES.items():
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


async def _auto_apply_results() -> tuple[int, int]:
    """Match ESPN results to pending predictions and apply. Returns (checked, applied)."""
    espn = await _fetch_espn_results()
    if not espn:
        return 0, 0

    # Pending matches: have predictions, no result, kickoff started >90 min ago
    cutoff = time.time() - 5400
    async with _db.execute(
        """SELECT p.match_id, p.match_title, p.team_home, p.team_away, p.kickoff_ts
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
            # Team names must match both sides
            if not (_names_match(pred["team_home"], espn_match["team_home"]) and
                    _names_match(pred["team_away"], espn_match["team_away"])):
                # Try reversed (some sites list home/away differently)
                if not (_names_match(pred["team_home"], espn_match["team_away"]) and
                        _names_match(pred["team_away"], espn_match["team_home"])):
                    continue
                # Swap scores if teams were reversed
                fh, fa = espn_match["score_away"], espn_match["score_home"]
            else:
                fh, fa = espn_match["score_home"], espn_match["score_away"]

            # Date proximity check (within 36 hours if kickoff_ts is known)
            if pred["kickoff_ts"] and espn_match["event_ts"]:
                if abs(pred["kickoff_ts"] - espn_match["event_ts"]) > 129600:  # 36h
                    continue

            # Apply result
            try:
                await _db.execute(
                    """INSERT OR IGNORE INTO match_results
                       (match_id,match_title,team_home,team_away,tournament,kickoff_ts,
                        final_home,final_away,entered_by,entered_at)
                       VALUES(?,?,?,?,?,?,?,?,?,?)""",
                    (pred["match_id"], pred["match_title"],
                     pred["team_home"], pred["team_away"],
                     espn_match["tournament"], pred["kickoff_ts"],
                     fh, fa, "auto-fetch", time.time()),
                )
                await _db.commit()

                # Calculate leaderboard points
                async with _db.execute(
                    "SELECT username, score_home, score_away FROM predictions WHERE match_id=?",
                    (pred["match_id"],),
                ) as cur2:
                    preds = [dict(r) for r in await cur2.fetchall()]
                if preds:
                    for p in preds:
                        p["diff"] = abs(p["score_home"] - fh) + abs(p["score_away"] - fa)
                    min_diff = min(p["diff"] for p in preds)
                    for p in preds:
                        pts = 5 if p["diff"] == 0 else (3 if p["diff"] == min_diff else 1)
                        await _db.execute(
                            """INSERT OR REPLACE INTO leaderboard
                               (match_id,tournament,username,diff,exact_score,points,created_at)
                               VALUES(?,?,?,?,?,?,?)""",
                            (pred["match_id"], espn_match["tournament"], p["username"],
                             p["diff"], 1 if p["diff"] == 0 else 0, pts, time.time()),
                        )
                    await _db.commit()
                applied += 1
                logger.info("Auto-applied result: %s %d-%d %s (match_id=%s)",
                            espn_match["team_home"], fh, fa, espn_match["team_away"], pred["match_id"])
            except Exception as exc:
                logger.warning("Auto-apply error for %s: %s", pred["match_id"], exc)
            break  # matched — move to next pending

    return len(pending), applied


async def _auto_fetch_loop():
    """Background task: auto-fetch results every 60 minutes."""
    await asyncio.sleep(120)  # 2-min grace period on startup
    while True:
        try:
            checked, applied = await _auto_apply_results()
            _last_auto_fetch.update({"ts": time.time(), "checked": checked, "applied": applied})
            if applied:
                logger.info("Auto-fetch: applied %d/%d results", applied, checked)
        except Exception as exc:
            logger.warning("Auto-fetch loop error: %s", exc)
        await asyncio.sleep(3600)  # every hour


async def _fetch_espn_upcoming() -> list[dict]:
    """Fetch upcoming matches from ESPN for all four tournaments. Cached 2h in Redis."""
    cached = await _redis.get("espn:upcoming")
    if cached:
        try:
            return json.loads(cached)
        except Exception:
            pass
    matches = []
    start = datetime.now(timezone.utc)
    end = start + timedelta(days=60)
    date_range = f"{start.strftime('%Y%m%d')}-{end.strftime('%Y%m%d')}"
    for slug, league_id in ESPN_LEAGUES.items():
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
                    "tournament_name": TOURNAMENTS[slug],
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
    for slug, league_id in ESPN_LEAGUES.items():
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
                    "tournament":  TOURNAMENTS.get(slug, slug),
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
    """Capitalise first letter of a username for display."""
    return s[0].upper() + s[1:] if s else s


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
_OPEN_PATHS = {"/login", "/health", "/manifest.json", "/sw.js", "/icon.svg"}

@app.middleware("http")
async def auth_mw(request: Request, call_next):
    path = request.url.path
    if path in _OPEN_PATHS or path.startswith("/join/"):
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
             '<meta name="theme-color" content="#171d28">'
             '<meta name="apple-mobile-web-app-capable" content="yes">'
             '<meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">'
             '<meta name="apple-mobile-web-app-title" content="Scrum">'
             '<link rel="apple-touch-icon" href="/icon.svg">')

_BASE_CSS = """:root{--bg:#16171d;--surface:#1e2028;--surface2:#262932;--accent:#4d9ef7;--accent2:#22c55e;--accent3:#e8671c;--text:#f0f1f5;--muted:#8896b0;--live:#22c55e;--border:#2c2f3e}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--text);font-family:'Barlow',sans-serif;min-height:100vh}
a{color:inherit;text-decoration:none}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.4}}
.logo{font-family:'Barlow Condensed',sans-serif;font-size:1.55rem;font-weight:700;letter-spacing:.06em;text-transform:uppercase}.logo span{color:var(--accent3)}
header{display:grid;grid-template-columns:1fr auto 1fr;align-items:center;padding:0 2rem;height:64px;border-bottom:1px solid var(--border);background:rgba(22,23,29,.97);backdrop-filter:blur(10px);position:sticky;top:0;z-index:10}
.nav-left{display:flex;align-items:center}
.nav-center{display:flex;align-items:center;gap:2.5rem}
.nav-right{display:flex;align-items:center;justify-content:flex-end;gap:1.25rem}
.nav-link{font-family:'Barlow Condensed',sans-serif;font-size:.9rem;font-weight:700;letter-spacing:.08em;text-transform:uppercase;color:var(--muted);padding:.18rem 0;border-bottom:2px solid transparent;transition:color .2s,border-color .2s}
.nav-link:hover{color:var(--text);border-color:var(--muted)}
.nav-link.active{color:var(--accent3);border-color:var(--accent3)}
.admin-link{font-size:.75rem;opacity:.5;letter-spacing:.05em;border-bottom:none}
.admin-link:hover{opacity:.9;color:var(--accent)}
.ubadge{font-size:.75rem;color:var(--muted);font-family:'Barlow Condensed',sans-serif;letter-spacing:.05em}
.logout-link{font-size:.78rem;border-bottom:none;color:var(--muted)}.logout-link:hover{color:var(--text)}
input[type=text],input[type=password],input[type=number],select{background:var(--surface2);border:1px solid var(--border);border-radius:8px;color:var(--text);font-family:'Barlow',sans-serif;font-size:.9rem;padding:.55rem .85rem;outline:none;transition:border-color .2s}
input:focus,select:focus{border-color:var(--accent)}
.btn{display:inline-block;background:var(--accent3);color:#fff;border:none;border-radius:8px;font-family:'Barlow Condensed',sans-serif;font-size:.9rem;font-weight:700;letter-spacing:.06em;text-transform:uppercase;padding:.55rem 1.2rem;cursor:pointer;transition:background .2s}
.btn:hover{background:#cc5a17}
.btn-sm{padding:.32rem .8rem;font-size:.78rem;border-radius:6px}
.btn-ghost{background:transparent;color:var(--muted);border:1px solid var(--border)}.btn-ghost:hover{color:var(--text);border-color:var(--muted)}
.btn-danger{background:rgba(239,68,68,.1);color:#ef4444;border:1px solid rgba(239,68,68,.25)}.btn-danger:hover{background:rgba(239,68,68,.2)}
.card{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:1rem 1.2rem}
.tag{font-family:'Barlow Condensed',sans-serif;font-size:.68rem;font-weight:700;letter-spacing:.08em;text-transform:uppercase;padding:.15rem .45rem;border-radius:4px}
.material-symbols-outlined{font-size:1.1rem;vertical-align:middle;font-variation-settings:'FILL' 0,'wght' 400,'GRAD' 0,'opsz' 24}
.bnav{display:none;position:fixed;bottom:0;left:0;right:0;height:56px;background:var(--surface);border-top:1px solid var(--border);z-index:20;justify-content:space-around;align-items:center}
.bnav-item{display:flex;flex-direction:column;align-items:center;gap:2px;flex:1;color:var(--muted);font-family:'Barlow Condensed',sans-serif;font-size:.62rem;font-weight:700;letter-spacing:.05em;text-transform:uppercase;padding:.5rem 0;transition:color .2s;text-decoration:none}
.bnav-item:hover,.bnav-item.active{color:var(--accent3)}
.bnav-item .material-symbols-outlined{font-size:1.35rem}
@media(max-width:768px){.bnav{display:flex}.page-body{padding-bottom:72px}.nav-center{display:none}}
"""


def _nav(username: str, is_admin: bool, active: str = "") -> str:
    def _ac(name):
        return " active" if active == name else ""
    adm = f'<a href="/admin" class="nav-link admin-link{_ac("admin")}">Admin</a>' if is_admin else ""
    return f"""<header>
  <div class="nav-left"><a href="/"><div class="logo">Scrum</div></a></div>
  <div class="nav-center">
    <a href="/leaderboard" class="nav-link{_ac('leaderboard')}">Predictions</a>
    <a href="/standings" class="nav-link{_ac('standings')}">Standings</a>
    <a href="/news" class="nav-link{_ac('news')}">News</a>
    <a href="/how-to-play" class="nav-link{_ac('how-to-play')}">How to Play</a>
  </div>
  <div class="nav-right">
    {adm}
    <a href="/logout" class="nav-link logout-link">Logout</a>
  </div>
</header>"""


def _bnav(active: str = "") -> str:
    def _ac(name):
        return " active" if active == name else ""
    return f"""<nav class="bnav">
  <a href="/leaderboard" class="bnav-item{_ac('leaderboard')}">
    <span class="material-symbols-outlined">emoji_events</span>
    <span>Predictions</span>
  </a>
  <a href="/standings" class="bnav-item{_ac('standings')}">
    <span class="material-symbols-outlined">table_chart</span>
    <span>Standings</span>
  </a>
  <a href="/news" class="bnav-item{_ac('news')}">
    <span class="material-symbols-outlined">newspaper</span>
    <span>News</span>
  </a>
  <a href="/how-to-play" class="bnav-item{_ac('how-to-play')}">
    <span class="material-symbols-outlined">help</span>
    <span>How to Play</span>
  </a>
</nav>"""


# ── PWA endpoints ─────────────────────────────────────────────────────────────
@app.get("/manifest.json")
async def pwa_manifest():
    data = {
        "name": "Scrum",
        "short_name": "Scrum",
        "description": "Self-hosted rugby prediction league",
        "start_url": "/",
        "display": "standalone",
        "background_color": "#171d28",
        "theme_color": "#171d28",
        "orientation": "portrait",
        "icons": [{"src": "/icon.svg", "sizes": "any", "type": "image/svg+xml", "purpose": "any maskable"}],
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
    <div class="join-logo">Rugby<span>.</span>Streams</div>
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
<title>Invalid Link</title></head><body style="background:#171d28;color:#eef2f9;display:flex;
align-items:center;justify-content:center;min-height:100vh;font-family:sans-serif">
<div style="text-align:center"><h2>Link Not Found</h2>
<p style="color:#8d9fbc;margin-top:.5rem">This invite link is invalid.</p>
</div></body></html>""", status_code=404)
    if row["used_at"]:
        current = _get_session_user(request)
        if current == row["username"]:
            return RedirectResponse(url="/", status_code=303)
        return HTMLResponse("""<!DOCTYPE html><html><head><meta charset="utf-8">
<title>Link Used</title></head><body style="background:#171d28;color:#eef2f9;display:flex;
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
<title>Link Used</title></head><body style="background:#171d28;color:#eef2f9;display:flex;
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
  <div class="box-title">Rugby<span>.</span>Streams</div>
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
    resp.delete_cookie(SESSION_COOKIE)
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
           LEFT JOIN leaderboard l ON l.match_id = p.match_id AND l.username = p.username
           WHERE p.match_id = ?
           ORDER BY l.diff ASC NULLS LAST, p.created_at ASC""",
        (match_id,),
    ) as cur:
        rows = [dict(r) for r in await cur.fetchall()]
    return JSONResponse({"me": username, "preds": rows})


# ── Live scores API ───────────────────────────────────────────────────────────
@app.get("/api/live-scores")
async def api_live_scores(request: Request):
    if not _get_session_user(request):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    try:
        return JSONResponse({"matches": await _fetch_espn_live()})
    except Exception as exc:
        return JSONResponse({"matches": [], "error": str(exc)})


# ── Streams page ──────────────────────────────────────────────────────────────
@app.get("/", response_class=HTMLResponse)
async def root_redirect():
    return RedirectResponse(url="/leaderboard", status_code=302)

# ── Predictions ───────────────────────────────────────────────────────────────
@app.get("/predict/{slug}", response_class=HTMLResponse)
async def predict_page(request: Request, slug: str, th: str = "", ta: str = "", kts: float = 0, title: str = "", tournament: str = ""):
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
        # Fallback: match info supplied via query params (from ESPN fixture cards)
        team_home, team_away = th.strip(), ta.strip()
        title = title.strip() or f"{team_home} vs {team_away}"
        kickoff_ts = float(kts)
    else:
        return HTMLResponse(f"""<!DOCTYPE html><html><head><meta charset="utf-8">{_FONTS}
<style>{_BASE_CSS}</style></head><body>{_nav(username, is_admin)}
<div style="padding:3rem 2rem;text-align:center;color:var(--muted)">Match not found or no predictions yet.</div>
</body></html>""")

    now = time.time()
    window_open = (kickoff_ts == 0) or (kickoff_ts - 86400 <= now < kickoff_ts + 1200)

    # My prediction
    async with _db.execute(
        "SELECT score_home, score_away FROM predictions WHERE match_id=? AND username=?",
        (slug, username),
    ) as cur:
        my_pred = await cur.fetchone()
    my_pred = dict(my_pred) if my_pred else None

    # Result (if entered)
    async with _db.execute("SELECT * FROM match_results WHERE match_id=?", (slug,)) as cur:
        result_row = await cur.fetchone()
    result = dict(result_row) if result_row else None

    # All predictions — always visible so everyone can see each other's picks
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
            p["winner"] = p["exact"] or (p["diff"] == min_diff)
        all_preds.sort(key=lambda p: p["diff"])

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
  <div style="font-size:.8rem;color:var(--muted);margin-top:.5rem">Locked — predictions cannot be changed.</div>
</div>"""
    elif window_open:
        mins_left = ""
        if kickoff_ts > 0:
            secs = max(0, int(kickoff_ts + 1200 - now))
            if secs < 3600:
                mins_left = f" · {secs // 60}m left"
            else:
                mins_left = " · Open"
        form_html = f"""<div class="pred-box" id="pred-form-wrap">
  <div class="pb-label">Make Your Prediction{mins_left}</div>
  <form id="pred-form">
    <input type="hidden" name="match_title" value="{_esc(title)}">
    <input type="hidden" name="kickoff_ts" value="{kickoff_ts}">
    <input type="hidden" name="team_home" value="{_esc(team_home)}">
    <input type="hidden" name="team_away" value="{_esc(team_away)}">
    <input type="hidden" name="tournament" value="{_esc(tournament)}">
    <div class="pb-score" style="margin:.75rem 0">
      <div class="pb-team">{_esc(team_home)}</div>
      <input type="number" name="score_home" min="0" max="200" placeholder="0" required
             style="width:60px;text-align:center;font-size:1.2rem">
      <div class="pb-dash">—</div>
      <input type="number" name="score_away" min="0" max="200" placeholder="0" required
             style="width:60px;text-align:center;font-size:1.2rem">
      <div class="pb-team">{_esc(team_away)}</div>
    </div>
    <button type="submit" class="btn" style="width:100%">Lock In Prediction</button>
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
):
    username = _get_session_user(request)
    if not username:
        return JSONResponse({"error": "not authenticated"}, status_code=401)

    now = time.time()
    if kickoff_ts > 0 and not (kickoff_ts - 86400 <= now < kickoff_ts + 1200):
        return JSONResponse({"error": "prediction window closed"}, status_code=400)

    try:
        await _db.execute(
            "INSERT INTO predictions "
            "(match_id,match_title,team_home,team_away,kickoff_ts,tournament,username,score_home,score_away,created_at) "
            "VALUES(?,?,?,?,?,?,?,?,?,?)",
            (slug, match_title.strip(), team_home.strip(), team_away.strip(),
             kickoff_ts, tournament.strip(), username, score_home, score_away, time.time()),
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
            # Update tournament if it's now known and wasn't before
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
        # 2023 RWC knockout results (hardcoded)
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

        # 2027 RWC pool draw (hardcoded)
        pools_2027 = {
            "Pool A": ["South Africa", "Scotland", "Fiji", "Portugal", "Chile"],
            "Pool B": ["New Zealand", "England", "Australia", "Georgia", "Tonga"],
            "Pool C": ["France", "Ireland", "Argentina", "Japan", "Samoa"],
            "Pool D": ["Wales", "Italy", "Namibia", "Uruguay", "Qualifying 1"],
        }
        pool_rows = ""
        for pool_name, teams in pools_2027.items():
            pool_rows += f'<div class="wc-pool-hdr">{pool_name}</div><ul class="wc-pool-list">'
            for team in teams:
                pool_rows += f'<li>{_esc(team)}</li>'
            pool_rows += '</ul>'

        table_html = f"""
<div class="section-banner">2023 Knockout Results</div>
<div class="wc-ko-wrap">{ko_rows}</div>
<div class="section-banner" style="margin-top:2rem">2027 Pool Draw</div>
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
.s-tab:hover{{color:var(--text);border-color:var(--muted)}}
.s-tab-active{{color:var(--accent);border-color:rgba(0,176,255,.4);background:rgba(0,176,255,.07)}}
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
{_bnav("standings")}
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
{_bnav("news")}
</body></html>""")


# ── How to Play ───────────────────────────────────────────────────────────────
@app.get("/how-to-play", response_class=HTMLResponse)
async def how_to_play(request: Request):
    username = _get_session_user(request)
    if not username:
        return RedirectResponse(url="/login", status_code=303)
    user = await _get_user(username)
    is_admin = bool(user and user["is_admin"])
    return HTMLResponse(f"""<!DOCTYPE html><html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>How to Play · Scrum</title>{_FONTS}<style>{_BASE_CSS}
.htp-wrap{{max-width:680px;margin:0 auto;padding:2rem 2rem 4rem}}
.htp-wrap h1{{font-family:'Barlow Condensed',sans-serif;font-size:1.8rem;font-weight:700;letter-spacing:.04em;text-transform:uppercase;margin:0 0 .25rem}}
.htp-subtitle{{color:var(--muted);font-size:.9rem;margin-bottom:2rem}}
.htp-section{{margin-bottom:2rem}}
.htp-section h2{{font-family:'Barlow Condensed',sans-serif;font-size:1.15rem;font-weight:700;letter-spacing:.06em;text-transform:uppercase;color:var(--accent3);margin:0 0 .75rem;padding-bottom:.4rem;border-bottom:1px solid var(--border)}}
.htp-section p{{color:var(--text);font-size:.92rem;line-height:1.65;margin:.5rem 0}}
.htp-section ul{{color:var(--text);font-size:.92rem;line-height:1.65;padding-left:1.25rem;margin:.5rem 0}}
.htp-section li{{margin-bottom:.35rem}}
.score-table{{width:100%;border-collapse:collapse;margin:.75rem 0;font-size:.88rem}}
.score-table th{{text-align:left;padding:.45rem .75rem;background:var(--surface2);color:var(--muted);font-family:'Barlow Condensed',sans-serif;font-size:.8rem;font-weight:700;letter-spacing:.06em;text-transform:uppercase}}
.score-table td{{padding:.45rem .75rem;border-top:1px solid var(--border);color:var(--text)}}
.pts{{font-family:'Barlow Condensed',sans-serif;font-size:1rem;font-weight:700;color:var(--accent3)}}
.badge-exact{{display:inline-block;background:rgba(255,145,0,.15);color:var(--accent3);border:1px solid rgba(255,145,0,.3);border-radius:4px;padding:.1rem .4rem;font-size:.72rem;font-weight:700;letter-spacing:.05em;text-transform:uppercase;vertical-align:middle}}
.badge-closest{{display:inline-block;background:rgba(77,158,247,.12);color:#4d9ef7;border:1px solid rgba(77,158,247,.25);border-radius:4px;padding:.1rem .4rem;font-size:.72rem;font-weight:700;letter-spacing:.05em;text-transform:uppercase;vertical-align:middle}}
.badge-played{{display:inline-block;background:rgba(141,159,188,.1);color:var(--muted);border:1px solid var(--border);border-radius:4px;padding:.1rem .4rem;font-size:.72rem;font-weight:700;letter-spacing:.05em;text-transform:uppercase;vertical-align:middle}}
.htp-example{{background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:1rem 1.1rem;margin:.75rem 0;font-size:.88rem}}
.htp-example .ex-label{{font-family:'Barlow Condensed',sans-serif;font-size:.75rem;font-weight:700;letter-spacing:.06em;text-transform:uppercase;color:var(--muted);margin-bottom:.5rem}}
.htp-example table{{width:100%;border-collapse:collapse}}
.htp-example td{{padding:.3rem .5rem;color:var(--text)}}
.htp-example td:last-child{{text-align:right;font-family:'Barlow Condensed',sans-serif;font-weight:700}}
@media(max-width:768px){{.htp-wrap{{padding:1.25rem 1rem 80px}}}}
</style></head><body>
{_nav(username, is_admin, "how-to-play")}
<div class="htp-wrap page-body">
  <h1>How to Play</h1>
  <p class="htp-subtitle">Predict rugby scores, earn points, climb the leaderboard.</p>

  <div class="htp-section">
    <h2>Making a Prediction</h2>
    <ul>
      <li>Go to the <strong>Predictions</strong> tab and find an upcoming fixture.</li>
      <li>Click <strong>Predict</strong> and enter the score you think each team will finish with.</li>
      <li>Submit — your prediction is locked in. You can only predict once per match and it cannot be changed.</li>
    </ul>
    <p><strong>Prediction window:</strong> opens 24 hours before kickoff and closes 20 minutes after kickoff. Once the window closes, no new predictions are accepted.</p>
  </div>

  <div class="htp-section">
    <h2>How Points Are Scored</h2>
    <p>After the final whistle, everyone's prediction is compared to the real score.</p>
    <table class="score-table">
      <tr><th>Result</th><th>Points</th></tr>
      <tr><td>Exact score <span class="badge-exact">Exact</span></td><td class="pts">5 pts</td></tr>
      <tr><td>Closest combined score difference <span class="badge-closest">Closest</span></td><td class="pts">3 pts</td></tr>
      <tr><td>Everyone else who predicted <span class="badge-played">Played</span></td><td class="pts">1 pt</td></tr>
    </table>
    <p>The <strong>difference</strong> is calculated as: |your home score − real home| + |your away score − real away|. The lower your diff, the better.</p>
    <p>Multiple players can share the 3-point spot if they have the same lowest diff.</p>
  </div>

  <div class="htp-section">
    <h2>Example</h2>
    <div class="htp-example">
      <div class="ex-label">Final score: All Blacks 32 – 18 Argentina</div>
      <table>
        <tr><td>Johan predicted <strong>32 – 18</strong></td><td class="pts">5 pts <span class="badge-exact">Exact</span></td></tr>
        <tr><td>Sarah predicted <strong>30 – 20</strong> (diff: 4)</td><td class="pts">3 pts <span class="badge-closest">Closest</span></td></tr>
        <tr><td>Mike predicted <strong>28 – 14</strong> (diff: 8)</td><td class="pts">1 pt</td></tr>
        <tr><td>Anna predicted <strong>25 – 25</strong> (diff: 25)</td><td class="pts">1 pt</td></tr>
      </table>
    </div>
  </div>

  <div class="htp-section">
    <h2>Leaderboard</h2>
    <p>Points accumulate across all tournaments throughout the season. The leaderboard updates automatically once results come in — usually within an hour of the final whistle.</p>
  </div>

  <div class="htp-section">
    <h2>Tournaments</h2>
    <ul>
      <li>Six Nations</li>
      <li>The Rugby Championship</li>
      <li>Super Rugby Pacific</li>
      <li>United Rugby Championship (URC)</li>
      <li>Rugby World Cup</li>
      <li>International (Tests &amp; tours)</li>
    </ul>
  </div>
</div>
{_bnav("how-to-play")}
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

    now_ts = time.time()
    by_tourn: dict[str, list] = {k: [] for k in TOURNAMENTS}
    for m in espn_upcoming:
        if m["tournament"] in by_tourn:
            kts = m.get("kickoff_ts") or 0
            # Skip games where the prediction window has already closed (kickoff + 20min ago)
            if kts and now_ts >= kts + 1200:
                continue
            # Only show fixtures within the next 14 days
            if kts and kts > now_ts + 14 * 86400:
                continue
            by_tourn[m["tournament"]].append(m)

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
        window_open = (not kts) or (kts - 86400 <= now_ts < kts + 1200)
        already = slug in user_pred_slugs
        if already:
            status = f'<a class="fix-status fix-done" href="{pred_url}">Predicted ✓</a>'
        elif kts and now_ts < kts - 86400:
            status = f'<span class="fix-status fix-soon">Opens in {_time_until(kts - 86400)}</span>'
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
            window_open = (not kts) or (kts - 86400 <= now_ts < kts + 1200)
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
  <div class="section-banner">Upcoming Fixtures</div>
  <p style="color:var(--muted);font-size:.88rem;margin-bottom:1.25rem">Select a tournament tab to see standings and results.</p>
  <div class="fix-section">
    {all_fixtures_html if all_fixtures_html else '<div class="empty-msg" style="padding:1rem">No upcoming fixtures found.</div>'}
  </div>
</div>"""

    else:
        # ── Tournament tab ───────────────────────────────────────────────────
        t_name = TOURNAMENTS[t]

        # 1. Upcoming fixtures for this tournament
        fix_cards = "".join(_fix_card(m) for m in by_tourn[t]) if by_tourn[t] else '<div class="empty-msg" style="padding:1rem">No upcoming fixtures found.</div>'
        fixtures_section = f"""<div class="page-section">
  <div class="section-banner">Upcoming Fixtures</div>
  <div class="fix-section">{fix_cards}</div>
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
            from collections import OrderedDict as _OD
            pending_by_match: dict = _OD()
            for row in pending_rows:
                mid = row["match_id"]
                if mid not in pending_by_match:
                    pending_by_match[mid] = {"title": row["match_title"], "kickoff_ts": row["kickoff_ts"], "preds": []}
                pending_by_match[mid]["preds"].append({"username": row["username"], "home": row["score_home"], "away": row["score_away"]})
            pending_cards = ""
            for mid, info in pending_by_match.items():
                dt = datetime.fromtimestamp(info["kickoff_ts"], tz=timezone.utc).strftime("%d %b") if info["kickoff_ts"] else "—"
                pred_rows_html = "".join(
                    f'<div class="my-pred-row">'
                    f'<span class="my-pred-date"></span>'
                    f'<span class="my-pred-match">{_esc(_ucfirst(p["username"]))}</span>'
                    f'<span class="my-pred-score">{p["home"]}–{p["away"]}</span>'
                    f'</div>'
                    for p in info["preds"]
                )
                pending_cards += f"""<div class="my-preds-wrap" style="margin-bottom:1rem">
  <div class="my-preds-hdr">{_esc(info['title'])} · <span style="font-weight:400;text-transform:none;letter-spacing:0">{dt} · {len(info['preds'])} prediction{"s" if len(info['preds'])!=1 else ""}</span></div>
  {pred_rows_html}
</div>"""
            pending_section_html = f"""<div class="page-section">
  <div class="section-banner collapsible collapsed" onclick="toggleSection(this)">Awaiting Results<span class="material-symbols-outlined sb-chevron">expand_more</span></div>
  <div class="collapsible-body collapsed">
  {pending_cards}
  </div>
</div>"""

        # 3. Standings for this tournament
        async with _db.execute(
            """SELECT username,
                      SUM(points)      as total_points,
                      COUNT(*)         as pred_count,
                      SUM(exact_score) as exact_scores,
                      ROUND(AVG(diff),1) as avg_diff
               FROM leaderboard WHERE tournament=?
               GROUP BY username
               ORDER BY total_points DESC, exact_scores DESC, avg_diff ASC""",
            (t,),
        ) as cur:
            standings = [dict(r) for r in await cur.fetchall()]

        if standings:
            s_rows = ""
            for i, s in enumerate(standings):
                medal = ["🥇", "🥈", "🥉"][i] if i < 3 else str(i + 1)
                is_me = " me-row" if s["username"] == username else ""
                s_rows += f"""<tr class="{is_me}">
  <td class="rank-cell">{medal}</td>
  <td class="name-cell">{_esc(_ucfirst(s['username']))}</td>
  <td class="pts-cell">{s['total_points']}</td>
  <td class="num-cell">{s['pred_count']}</td>
  <td class="num-cell">{s['exact_scores']}</td>
  <td class="num-cell">{s['avg_diff'] or '—'}</td>
</tr>"""
            table_html = f"""<table class="lb-table">
  <thead><tr>
    <th>#</th><th>Player</th><th>Points</th><th>Played</th><th>Exact</th><th>Avg Diff</th>
  </tr></thead>
  <tbody>{s_rows}</tbody>
</table>"""
        else:
            table_html = '<div class="empty-msg">No results entered yet — standings will appear here once the first game is resolved.</div>'

        standings_section = f"""<div class="page-section">
  <div class="section-banner collapsible collapsed" onclick="toggleSection(this)">Standings<span class="material-symbols-outlined sb-chevron">expand_more</span></div>
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
                dt = datetime.fromtimestamp(m["kickoff_ts"], tz=timezone.utc).strftime("%d %b") if m["kickoff_ts"] else ""
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
  <div class="section-banner collapsible collapsed" onclick="toggleSection(this)">Recent Results<span class="material-symbols-outlined sb-chevron">expand_more</span></div>
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
.t-tabs{{display:flex;gap:.5rem;flex-wrap:wrap;margin-bottom:1.5rem}}
.t-tab{{font-family:'Barlow Condensed',sans-serif;font-size:.85rem;font-weight:700;letter-spacing:.05em;text-transform:uppercase;padding:.4rem 1rem;border-radius:6px;border:1px solid var(--border);color:var(--muted);transition:all .2s;display:inline-flex;align-items:center;gap:.4rem}}
.t-tab:hover{{color:var(--text);border-color:var(--muted)}}.t-tab.active{{color:var(--accent);border-color:rgba(0,176,255,.4);background:rgba(0,176,255,.07)}}
.t-badge{{display:inline-flex;align-items:center;justify-content:center;min-width:1.1rem;height:1.1rem;padding:0 .3rem;border-radius:99px;background:var(--accent3);color:#fff;font-size:.6rem;font-weight:800;letter-spacing:0;line-height:1}}
.section-label{{font-family:'Barlow Condensed',sans-serif;font-size:.78rem;font-weight:700;letter-spacing:.08em;text-transform:uppercase;color:var(--muted);margin-bottom:.75rem}}
.lb-table{{width:100%;border-collapse:collapse;margin-bottom:1rem}}
.lb-table thead tr{{border-bottom:2px solid var(--border)}}
.lb-table th{{font-family:'Barlow Condensed',sans-serif;font-size:.78rem;font-weight:700;letter-spacing:.07em;text-transform:uppercase;color:var(--muted);padding:.5rem .75rem;text-align:left}}
.lb-table td{{padding:.65rem .75rem;border-bottom:1px solid var(--border);font-size:.9rem}}
.lb-table tbody tr:hover{{background:var(--surface)}}
.me-row{{background:rgba(0,176,255,.05)!important}}
.rank-cell{{font-size:1.1rem;width:40px}}
.name-cell{{font-family:'Barlow Condensed',sans-serif;font-size:1rem;font-weight:600}}
.pts-cell{{font-family:'Barlow Condensed',sans-serif;font-size:1.1rem;font-weight:700;color:var(--accent2)}}
.num-cell{{color:var(--muted);font-size:.85rem}}
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
.section-banner{{font-family:'Barlow Condensed',sans-serif;font-size:1rem;font-weight:800;letter-spacing:.1em;text-transform:uppercase;color:var(--accent3);border-left:3px solid var(--accent3);padding:.1rem 0 .1rem .75rem;margin-bottom:.85rem}}
.section-banner.collapsible{{cursor:pointer;display:flex;align-items:center;justify-content:space-between;user-select:none;padding-right:.25rem}}
.section-banner.collapsible:hover{{color:var(--accent3);opacity:.85}}
.section-banner .sb-chevron{{font-size:.9rem;transition:transform .2s;opacity:.6}}
.section-banner.collapsible.collapsed .sb-chevron{{transform:rotate(-90deg)}}
.collapsible-body{{overflow:hidden;transition:none}}
.collapsible-body.collapsed{{display:none}}
.fix-section{{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:1rem 1.2rem}}
.fix-group{{margin-bottom:.9rem}}.fix-group:last-child{{margin-bottom:0}}
.fix-t-name{{font-family:'Barlow Condensed',sans-serif;font-size:.72rem;font-weight:700;letter-spacing:.08em;text-transform:uppercase;color:var(--accent);margin-bottom:.3rem}}
.fix-row{{display:flex;align-items:center;gap:.5rem;padding:.35rem .5rem;border-radius:5px;background:var(--surface2);margin-bottom:.22rem;font-size:.83rem}}
.fix-teams{{flex:1;font-family:'Barlow Condensed',sans-serif;font-weight:600;text-align:center}}
.fix-vs{{color:var(--muted);font-weight:400;margin:0 .2rem}}
.fix-foot{{display:flex;align-items:center;gap:.4rem;flex-shrink:0}}
@media(max-width:600px){{.fix-row{{flex-direction:column;align-items:stretch;gap:.3rem;padding:.45rem .6rem}}.fix-foot{{justify-content:space-between}}}}
.fix-kick{{font-size:.7rem;color:var(--muted);white-space:nowrap}}
.fix-status{{font-family:'Barlow Condensed',sans-serif;font-size:.65rem;font-weight:700;letter-spacing:.05em;text-transform:uppercase;padding:.1rem .35rem;border-radius:3px}}
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
{_bnav("leaderboard")}
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
        admin_badge = ' <span class="tag" style="background:rgba(0,176,255,.15);color:var(--accent)">ADMIN</span>' if u["is_admin"] else ""
        del_btn = ""
        link_btn = f'<a href="/admin/invite-link/{_esc(u["username"])}" class="btn btn-sm btn-ghost">Get Link</a>'
        if u["username"] != username:
            del_btn = f"""<form method="post" action="/admin/users/delete" style="display:inline">
  <input type="hidden" name="del_username" value="{_esc(u['username'])}">
  <button type="submit" class="btn btn-sm btn-danger">Delete</button>
</form>"""
        user_rows += f"""<div class="admin-row">
  <span class="ar-name">{_esc(u['username'])}{admin_badge}</span>
  {link_btn}{del_btn}
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
        t_name = TOURNAMENTS.get(r["tournament"], r["tournament"])
        resolved_html += f"""<div class="admin-row">
  <span class="ar-name">{_esc(r['match_title'])}</span>
  <span style="font-size:.78rem;color:var(--muted)">{t_name}</span>
  <span style="font-family:'Barlow Condensed',sans-serif;color:var(--accent2)">{r['final_home']}—{r['final_away']}</span>
</div>"""
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
      <form method="post" action="/admin/results/auto-fetch" style="display:flex;align-items:center;gap:.75rem">
        <span style="font-size:.75rem;color:var(--muted)">{auto_status}</span>
        <button type="submit" class="btn btn-sm">⟳ Auto-Fetch from ESPN</button>
      </form>
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

    await _db.execute(
        """INSERT OR REPLACE INTO match_results
           (match_id,match_title,team_home,team_away,tournament,kickoff_ts,final_home,final_away,entered_by,entered_at)
           VALUES(?,?,?,?,?,?,?,?,?,?)""",
        (match_id, match_title.strip(), team_home.strip(), team_away.strip(),
         tournament, kickoff_ts, final_home, final_away, username, time.time()),
    )
    await _db.commit()

    # Calculate leaderboard entries
    async with _db.execute(
        "SELECT username, score_home, score_away FROM predictions WHERE match_id=?", (match_id,)
    ) as cur:
        preds = [dict(r) for r in await cur.fetchall()]

    if preds:
        for p in preds:
            p["diff"] = abs(p["score_home"] - final_home) + abs(p["score_away"] - final_away)
        min_diff = min(p["diff"] for p in preds)
        for p in preds:
            if p["diff"] == 0:
                pts = 5
            elif p["diff"] == min_diff:
                pts = 3
            else:
                pts = 1
            await _db.execute(
                """INSERT OR REPLACE INTO leaderboard
                   (match_id,tournament,username,diff,exact_score,points,created_at)
                   VALUES(?,?,?,?,?,?,?)""",
                (match_id, tournament, p["username"], p["diff"],
                 1 if p["diff"] == 0 else 0, pts, time.time()),
            )
        await _db.commit()

    return RedirectResponse(url="/admin", status_code=303)


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
