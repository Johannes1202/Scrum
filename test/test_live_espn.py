"""Live ESPN check — asserts the real feed splits the way we expect.

Network-dependent by design: the unit tests prove the filter logic, this proves the
filter still matches what ESPN is actually publishing. Skips cleanly (exit 0) if
ESPN is unreachable so a flaky network never blocks a deploy.

    docker exec scrum_dashboard python3 /app/test/test_live_espn.py
"""
import os
import sys

os.environ.setdefault("AVATAR_DIR", "/tmp/avatars-test")
sys.path.insert(0, "/app")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dashboard"))

import httpx  # noqa: E402
import server  # noqa: E402

SOURCE = 289234
FAILURES = []

# Known ESPN event ids -> the league they must be filed under.
EXPECTED = {
    "603247": "greatest-rivalry",   # 1st Test, Ellis Park
    "603248": "greatest-rivalry",   # 2nd Test, DHL Stadium
    "603249": "greatest-rivalry",   # 3rd Test, FNB Stadium
    "603250": "greatest-rivalry",   # 4th Test, M&T Bank Stadium, Baltimore
    "603460": "puma-trophy",
    "603461": "puma-trophy",
    "603462": "mandela-plate",
    "603463": "bledisloe-cup",
    "603464": "bledisloe-cup",
    "603498": "international",       # standalone warm-up, deliberately unclaimed
}

url = (f"https://site.api.espn.com/apis/site/v2/sports/rugby/{SOURCE}"
       f"/scoreboard?limit=100&dates=20260801-20261231")
try:
    resp = httpx.get(url, timeout=15)
    resp.raise_for_status()
    events = resp.json().get("events", [])
except Exception as exc:
    print(f"live-espn: SKIPPED (ESPN unreachable: {exc})")
    sys.exit(0)

if not events:
    print("live-espn: SKIPPED (ESPN returned no events for the window)")
    sys.exit(0)

seen = {}
for event in events:
    comp = event.get("competitions", [{}])[0]
    teams = comp.get("competitors", [])
    if len(teams) != 2:
        continue
    home = next((t for t in teams if t.get("homeAway") == "home"), teams[0])
    away = next((t for t in teams if t.get("homeAway") == "away"), teams[1])
    slug, _ = server._derive_tournament(
        SOURCE,
        home["team"]["displayName"], away["team"]["displayName"],
        server._iso_to_ts(event.get("date", "")),
        "international", "International",
    )
    seen[event.get("id", "")] = (slug, event.get("name", ""), event.get("date", "")[:10])

for event_id, want in EXPECTED.items():
    if event_id not in seen:
        FAILURES.append(f"event {event_id} missing from ESPN feed (expected {want}) "
                        f"— fixture may have been rescheduled or re-id'd")
        continue
    got, name, date = seen[event_id]
    if got != want:
        FAILURES.append(f"event {event_id} ({name}, {date}) filed as {got}, expected {want}")

# The four Tests must be exactly the four we know about — no more, no less.
rivalry = sorted(e for e, (s, _, _) in seen.items() if s == "greatest-rivalry")
if rivalry != ["603247", "603248", "603249", "603250"]:
    FAILURES.append(f"greatest-rivalry claimed {rivalry}, expected the four known Tests")

print(f"live-espn: {len(events)} events from league {SOURCE}, "
      f"{len(EXPECTED)} assertions, {len(FAILURES)} failed")
for event_id, (slug, name, date) in sorted(seen.items(), key=lambda kv: kv[1][2]):
    print(f"   {date}  {slug:<18} {name}")
for f in FAILURES:
    print(f"  FAIL {f}")
sys.exit(1 if FAILURES else 0)
