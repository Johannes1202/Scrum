# Scrum 🏉

**A self-hosted rugby companion app with a prediction league built in.**

Scrum is genuinely useful as a standalone rugby dashboard — live fixtures, match results with full breakdowns, league standings, squad data, and news across 10 major competitions. No account required to browse. The prediction game is the social layer that makes it worth sharing with your mates.

> Built for the guy with an old laptop and a group of rugby-mad friends. If it runs on that, it runs anywhere.

---

## What you get

### As a rugby fan
- Fixtures across 10 competitions (Super Rugby Pacific, URC, Six Nations, Rugby Championship, Premiership, Top 14, Champions Cup, Challenge Cup, World Cup, International)
- Match results with full breakdowns — try scorers grouped by team with minute stamps, first try scorer, Man of the Match
- League standings
- Pre-match squad data (updated 48–72h before kickoff)
- Rugby news feed

### As a prediction league
- Predict on 7 types per match: score, winner, margin band, both teams scored, anytime try scorer, first try scorer, Man of the Match
- Banker pick — double your points once per week
- Private groups with custom league subscriptions and prediction types
- Auto-resolution via ESPN (scores, try scorers, margin) + RSS scraping (MOTM)
- Full match breakdown showing correct answers after the whistle
- Group leaderboards, personal stats, head-to-head, hot streaks

### As an admin
- Fully managed via in-app admin panel
- No server access needed after initial setup
- Magic link password resets (no email server required)
- Manual MOTM entry fallback for when auto-scrape doesn't find it

---

## Quick Start

### Prerequisites
- Docker + Docker Compose
- ~200MB RAM, ~500MB disk
- A port (default: 8888)

### 1. Clone

```bash
git clone https://github.com/Johannes1202/scrum.git
cd scrum
```

### 2. Configure

```bash
cp .env.example .env
```

Edit `.env`:

```env
ADMIN_USERNAME=admin
ADMIN_PASSWORD=your_secure_password
SESSION_SECRET=replace_with_64_char_hex   # openssl rand -hex 32
REDIS_URL=redis://redis:6379
DB_PATH=/data/scrum.db
PORT=8888
```

### 3. Run

```bash
docker compose up --build -d
```

That's it. Open `http://localhost:8888` and log in with your admin credentials.

> ⚠️ **Important:** Always use `docker compose up --build -d` when updating. `docker compose restart` uses the old image and won't pick up code changes.

---

## First run checklist

1. **Log in** as admin
2. **Go to Admin panel** — create accounts for your players (or share the signup link)
3. **Create a Group** — choose which leagues to follow and which prediction types to enable
4. **Invite your players** — generate an invite link from the group page, share it on WhatsApp
5. **Wait for a match** — results auto-resolve within an hour of final whistle

---

## How predictions work

Each match has a 48-hour prediction window that opens before kickoff. Players predict on any combination of:

| Type | Points | How it resolves |
|------|--------|-----------------|
| Score Prediction | 5 exact / 3 closest in group / 1 otherwise | Auto (ESPN) |
| Winner | 2 | Auto (ESPN) |
| Winning Margin Band | 2 | Auto (ESPN) |
| Both Teams Scored | 1 | Auto (ESPN) |
| Anytime Try Scorer | 3 | Auto (ESPN) |
| First Try Scorer | 4 | Auto (ESPN) |
| Man of the Match | 3 | Auto (RSS scraper) + admin fallback |

**Banker pick:** Mark one prediction per week as your banker — correct picks earn double points.

---

## Auto-resolution pipeline

Scrum runs a background process every 60 minutes that:

1. **Checks ESPN** for completed matches and applies scores
2. **Fetches try scorer data** from ESPN match summaries (with retry logic for slow data)
3. **Scrapes MOTM** from 4 public RSS feeds (RugbyPass, BBC, The Roar, ESPN) using article pattern matching
4. **Scores all groups** with the full result data

MOTM scraping retries hourly for 48 hours. If it still can't find it after that, the admin can enter it manually in the admin panel — one field, one button.

---

## Configuration reference

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `ADMIN_USERNAME` | Yes | — | Admin account username |
| `ADMIN_PASSWORD` | Yes | — | Admin account password |
| `SESSION_SECRET` | Yes | — | 64-char hex string for session signing |
| `REDIS_URL` | No | `redis://redis:6379` | Redis connection URL |
| `DB_PATH` | No | `/data/scrum.db` | SQLite database path |
| `PORT` | No | `8888` | Port to expose |

---

## Updating

```bash
git pull
docker compose up --build -d
```

The app runs database migrations automatically on startup — no manual schema changes needed.

---

## Exposing publicly

Scrum works great behind a reverse proxy or Cloudflare Tunnel.

### Nginx example

```nginx
server {
    listen 80;
    server_name scrum.yourdomain.com;
    location / {
        proxy_pass http://localhost:8888;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    }
}
```

### Cloudflare Tunnel

```bash
cloudflared tunnel --url http://localhost:8888
```

---

## Competitions covered

| Competition | Leagues |
|-------------|---------|
| Super Rugby Pacific | Australia, New Zealand, Pacific Islands |
| United Rugby Championship | Ireland, Scotland, Wales, South Africa, Italy |
| Six Nations | England, France, Ireland, Scotland, Wales, Italy |
| Rugby Championship | South Africa, New Zealand, Australia, Argentina |
| Premiership Rugby | England |
| Top 14 | France |
| Champions Cup | Europe |
| Challenge Cup | Europe |
| Rugby World Cup | International |
| International Rugby | Test matches |

Fixtures, results, standings and squad data are pulled automatically from ESPN. No API keys required.

---

## Data & Privacy

- All data stays on your server. Nothing is sent anywhere except outbound requests to ESPN (results/fixtures) and public RSS feeds (MOTM news)
- No email server required — password resets use admin-generated magic links
- No analytics, no tracking, no third-party scripts

---

## Tech stack

- **Backend:** Python 3.12 / FastAPI
- **Database:** SQLite (via aiosqlite)
- **Cache:** Redis 7
- **Container:** Docker + Docker Compose
- **Data sources:** ESPN API (free, no key) + RugbyPass/BBC/The Roar RSS feeds (free, no key)
- **Frontend:** Server-rendered HTML, vanilla JS, no framework

Single binary deployment. One `docker compose up` and it's running.

---

## Architecture notes

- Single-file FastAPI app — all routes, HTML rendering, background tasks in `server.py`
- All HTML rendered server-side as f-strings — no template engine, no build step
- Predictions are global per user per match — groups are just scoring lenses on the same prediction data
- Global group (id=1) is a system group — not shown in UI, powers the global Predictions page
- Custom competitions let group admins create mini-tournaments from any mix of matches

---

## License

MIT — do whatever you want with it.

---

*Built with too much coffee and a deep love of rugby. Tested against live Super Rugby, URC, Six Nations, and Champions Cup data.*
