"""Derived-league filter tests.

Run inside the app container (it has the deps):
    docker exec scrum_dashboard python3 /app/test/test_derived_leagues.py

Covers the mechanism that splits trophy series out of ESPN's generic
"International" league (289234), which carries no competition metadata of its own.
"""
import os
import sys
from datetime import datetime, timezone

os.environ.setdefault("AVATAR_DIR", "/tmp/avatars-test")
sys.path.insert(0, "/app")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dashboard"))

import server  # noqa: E402

FAILURES = []
CHECKS = [0]


def check(label, got, want):
    CHECKS[0] += 1
    if got != want:
        FAILURES.append(f"{label}\n     got:  {got!r}\n     want: {want!r}")


def ts(date_str):
    return datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()


def derive(home, away, date_str, league_id=289234):
    """Return just the slug the event would be filed under."""
    return server._derive_tournament(
        league_id, home, away, ts(date_str), "international", "International")[0]


# ── The four Tests of Rugby's Greatest Rivalry ────────────────────────────────
# Dates and venues verified against Wikipedia + ESPN event data (Aug 2026).
check("1st Test 22 Aug -> greatest-rivalry",
      derive("South Africa", "New Zealand", "2026-08-22"), "greatest-rivalry")
check("2nd Test 29 Aug -> greatest-rivalry",
      derive("South Africa", "New Zealand", "2026-08-29"), "greatest-rivalry")
check("3rd Test 5 Sep -> greatest-rivalry",
      derive("South Africa", "New Zealand", "2026-09-05"), "greatest-rivalry")
check("4th Test 12 Sep (Baltimore) -> greatest-rivalry",
      derive("South Africa", "New Zealand", "2026-09-12"), "greatest-rivalry")

# Reversed home/away must match too — the Baltimore Test is at a neutral venue.
check("reversed teams still match",
      derive("New Zealand", "South Africa", "2026-08-22"), "greatest-rivalry")

# Nicknames must normalise via _TEAM_ALIASES.
check("Springboks/All Blacks aliases resolve",
      derive("Springboks", "All Blacks", "2026-08-22"), "greatest-rivalry")

# ── Other trophy series ───────────────────────────────────────────────────────
check("Puma Trophy 1st Test", derive("Argentina", "Australia", "2026-08-29"), "puma-trophy")
check("Puma Trophy 2nd Test", derive("Argentina", "Australia", "2026-09-05"), "puma-trophy")
check("Mandela Challenge Plate", derive("Australia", "South Africa", "2026-09-27"), "mandela-plate")
check("Bledisloe 1st Test", derive("New Zealand", "Australia", "2026-10-10"), "bledisloe-cup")
check("Bledisloe 2nd Test", derive("Australia", "New Zealand", "2026-10-17"), "bledisloe-cup")

# ── Must NOT be claimed ───────────────────────────────────────────────────────
# The 8 Aug Argentina v South Africa is a standalone warm-up, not part of any series.
check("Arg v SA warm-up stays International",
      derive("Argentina", "South Africa", "2026-08-08"), "international")
check("Japan v Australia stays International",
      derive("Japan", "Australia", "2026-08-08"), "international")
check("Belgium v Hong Kong stays International",
      derive("Belgium", "Hong Kong", "2026-10-31"), "international")
check("Paraguay v Brazil stays International",
      derive("Paraguay", "Brazil", "2026-11-14"), "international")

# A third nation against one of the pair must never be swept in.
check("SA v Argentina not greatest-rivalry",
      derive("South Africa", "Argentina", "2026-08-22"), "international")

# Out-of-window fixtures fall back rather than being misfiled.
check("SA v NZ outside window falls back",
      derive("South Africa", "New Zealand", "2027-08-22"), "international")
check("NZ v Aus before Bledisloe window falls back",
      derive("New Zealand", "Australia", "2026-07-01"), "international")

# Same pairing, different windows, must not collide.
check("NZ v Aus in Oct is Bledisloe not Puma",
      derive("New Zealand", "Australia", "2026-10-10"), "bledisloe-cup")

# A different source league must never be claimed.
check("URC fixture unaffected by derived rules",
      derive("Lions", "Leinster", "2026-09-26", league_id=270557), "international")

# ── Registration invariants ───────────────────────────────────────────────────
for d in server.DERIVED_LEAGUES:
    check(f"{d['slug']} registered in ALL_ESPN_LEAGUES",
          d["slug"] in server.ALL_ESPN_LEAGUES, True)
    check(f"{d['slug']} has a display name",
          server.TOURNAMENTS.get(d["slug"]), d["name"])
    check(f"{d['slug']} window is ordered", d["from_ts"] < d["to_ts"], True)

# Slugs must be unique and must not shadow a real ESPN league.
slugs = [d["slug"] for d in server.DERIVED_LEAGUES]
check("derived slugs unique", len(slugs), len(set(slugs)))

# Every derived league's source must itself be a real, fetched league.
real_sources = {lid for s, (lid, _) in server.ALL_ESPN_LEAGUES.items()
                if s not in server.DERIVED_BY_SLUG}
for d in server.DERIVED_LEAGUES:
    check(f"{d['slug']} source {d['source']} is a fetched league",
          d["source"] in real_sources, True)

# ── Squad fetching must survive the reassignment ──────────────────────────────
# Squad rosters and try-scorer resolution both call ESPN as
# /rugby/{league_id}/summary?event={espn_id}. A derived league has no ESPN id of
# its own, so it must keep its SOURCE league id — remapping that to something
# derived would silently empty every try-scorer dropdown on the tour.
for d in server.DERIVED_LEAGUES:
    check(f"{d['slug']} resolves to its source ESPN league id",
          server.ALL_ESPN_LEAGUES[d["slug"]][0], d["source"])
    check(f"{d['slug']} source is a real ESPN league, not a slug",
          isinstance(d["source"], int), True)

# _derive_tournament reassigns the label only — it must never be a route by which
# a league id changes.
_sig = server._derive_tournament(289234, "South Africa", "New Zealand",
                                 ts("2026-08-22"), "international", "International")
check("_derive_tournament returns (slug, name) only", len(_sig), 2)

# ── Manually declared fixtures ────────────────────────────────────────────────
# ESPN carries the tour's four Tests but none of the four midweek provincial games
# (checked across all 25 of its rugby leagues), so those are declared in config.
rows = server._manual_fixture_rows()
check("four provincial fixtures declared", len(rows), 4)
for r in rows:
    check(f"{r['slug']} is on the tour", r["tournament"], "greatest-rivalry")
    check(f"{r['slug']} has a kickoff", r["kickoff_ts"] > 0, True)
    check(f"{r['slug']} carries no ESPN id", r["espn_id"], "")
    check(f"{r['slug']} id cannot collide with an ESPN event id",
          r["slug"].isdigit(), False)

check("Stormers game is Fri 7 Aug 17:10 UTC (19:10 SAST)",
      server._manual_fixture_rows()[0]["kickoff_ts"], ts("2026-08-07") + 17 * 3600 + 10 * 60)

# Merged into the feed and ordered by kickoff alongside the ESPN Tests.
fake_espn = [{"slug": "603247", "espn_id": "603247", "team_home": "South Africa",
              "team_away": "New Zealand", "kickoff_ts": ts("2026-08-22") + 15 * 3600 + 10 * 60,
              "tournament": "greatest-rivalry"}]
merged = server._with_manual_fixtures(fake_espn)
check("merge yields all eight tour matches", len(merged), 5)  # 1 fake ESPN + 4 manual
check("merged feed is ordered by kickoff",
      [m["kickoff_ts"] for m in merged], sorted(m["kickoff_ts"] for m in merged))
check("the ESPN Test survives the merge",
      any(m.get("espn_id") == "603247" for m in merged), True)

# If ESPN ever publishes one of these, the real event must win — no duplicate row.
espn_added = fake_espn + [{"slug": "999001", "espn_id": "999001", "team_home": "Stormers",
                           "team_away": "New Zealand",
                           "kickoff_ts": ts("2026-08-07") + 17 * 3600 + 10 * 60,
                           "tournament": "greatest-rivalry"}]
merged2 = server._with_manual_fixtures(espn_added)
check("ESPN supersedes the manual Stormers fixture",
      sum(1 for m in merged2 if m.get("team_home") == "Stormers"), 1)
check("and it is the ESPN one that survives",
      next(m["espn_id"] for m in merged2 if m.get("team_home") == "Stormers"), "999001")

# Reversed home/away on the same day must still be recognised as the same fixture.
check("fixture identity ignores home/away order",
      server._fixture_key({"team_home": "Stormers", "team_away": "New Zealand",
                           "kickoff_ts": ts("2026-08-07")}),
      server._fixture_key({"team_home": "New Zealand", "team_away": "Stormers",
                           "kickoff_ts": ts("2026-08-07")}))

# ── Config-rot guard ──────────────────────────────────────────────────────────
# Windows are per-edition. Once one fully elapses its fixtures silently revert to
# the source league, so this fails loudly and asks for the next edition's dates.
expired = server._derived_windows_expired()
check(f"no derived league windows have expired (expired: {expired})", expired, [])

# ── Report ────────────────────────────────────────────────────────────────────
print(f"derived-leagues: {CHECKS[0]} checks, {len(FAILURES)} failed")
for f in FAILURES:
    print(f"  FAIL {f}")
sys.exit(1 if FAILURES else 0)
