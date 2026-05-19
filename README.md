# Sport Streams v2

Multi-sport streaming hub using Docker Compose.

## Architecture
- **Redis** — shared stream registry with TTL-based token management
- **Dashboard** — FastAPI UI + HLS reverse proxy
- **14x Scrapers** — one per sport, independent Playwright scrapers

## Deployment

### 1. Prerequisites
Fresh Ubuntu 24.04 VM with Docker installed:
```bash
apt-get update -qq && apt-get install -y ca-certificates curl && \
install -m 0755 -d /etc/apt/keyrings && \
curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc && \
chmod a+r /etc/apt/keyrings/docker.asc && \
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null && \
apt-get update -qq && \
apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin && \
systemctl enable docker && systemctl start docker
```

### 2. Deploy via Portainer
- Copy this entire folder to the VM (e.g. `/opt/sportstreams-v2/`)
- In Portainer → Stacks → Add Stack → Upload → select `docker-compose.yml`
- Or via CLI: `docker compose up -d --build`

### 3. Cloudflare Tunnel
Point `sportstreams.homebrewjoe.com` → `http://localhost:8888`

## Configuration
All config is in `docker-compose.yml` environment variables.
To tune a specific sport, edit its scraper service and restart just that container.

## Adding a new sport
1. Add a new service block to `docker-compose.yml` (copy any existing scraper)
2. Set SPORT, DOMAIN, CATEGORIES
3. `docker compose up -d scraper-newsport`

## Password
Default: `dstvsepoes` — change via `STREAM_PASSWORD` env var in dashboard service.
