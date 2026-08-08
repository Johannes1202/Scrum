"""Scoring engine tests — runs _score_group against a throwaway SQLite DB.

Three of the six bugs found in the first live round (May 2026) were type-coercion
in this function: TEXT '0' being truthy, TEXT '1' never equalling INTEGER 1. These
tests pin every prediction type, the banker doubling and the closest-score bonus so
that class of bug cannot come back silently.

    docker exec scrum_dashboard python3 /app/test/test_scoring.py
"""
import asyncio
import os
import sys
import tempfile
import time
from datetime import datetime, timezone

_tmp = tempfile.mkdtemp(prefix="scrum-test-")
os.environ["DB_PATH"] = os.path.join(_tmp, "test.db")
os.environ.setdefault("AVATAR_DIR", os.path.join(_tmp, "avatars"))
os.environ.setdefault("SESSION_SECRET", "test-secret")
sys.path.insert(0, "/app")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dashboard"))

import server  # noqa: E402

FAILURES = []
CHECKS = [0]

ALL_TYPES = ("score", "winner", "margin", "btts", "try_anytime", "try_first", "banker")
NO_BANKER = tuple(t for t in ALL_TYPES if t != "banker")
KICKOFF = 1787410800.0  # 22 Aug 2026, the 1st Test


def check(label, got, want):
    CHECKS[0] += 1
    if got != want:
        FAILURES.append(f"{label}\n     got:  {got!r}\n     want: {want!r}")


async def setup_group(group_id, members, types=ALL_TYPES, leagues=("greatest-rivalry",)):
    db = server._db
    await db.execute("INSERT OR REPLACE INTO groups (id,name,slug,created_by,created_at) "
                     "VALUES(?,?,?,?,?)",
                     (group_id, f"G{group_id}", f"g{group_id}", "johan", time.time()))
    for u in members:
        await db.execute("INSERT OR IGNORE INTO users (username,password_hash,is_admin,created_at) "
                         "VALUES(?,?,0,?)", (u, "x", time.time()))
        await db.execute("INSERT OR IGNORE INTO group_members (group_id,username,role,joined_at) "
                         "VALUES(?,?,?,?)", (group_id, u, "member", time.time()))
    for t in types:
        await db.execute("INSERT OR IGNORE INTO group_prediction_types (group_id,prediction_type) "
                         "VALUES(?,?)", (group_id, t))
    for lg in leagues:
        await db.execute("INSERT OR IGNORE INTO group_leagues (group_id,league_slug) VALUES(?,?)",
                         (group_id, lg))
    await db.commit()


async def predict(match_id, username, sh, sa, winner=None, margin=None, btts=None,
                  try_any=None, try_first=None, banker=0, tournament="greatest-rivalry"):
    await server._db.execute(
        """INSERT OR REPLACE INTO predictions
           (match_id,match_title,team_home,team_away,kickoff_ts,tournament,username,
            score_home,score_away,pred_winner,pred_margin,pred_btts,pred_try_any,
            pred_try_first,is_banker,created_at)
           VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (match_id, "SA v NZ", "South Africa", "New Zealand", KICKOFF, tournament, username,
         sh, sa, winner, margin, btts, try_any, try_first, banker, time.time()))
    await server._db.commit()


async def points(group_id, match_id, username):
    async with server._db.execute(
        "SELECT points,pts_score,pts_winner,pts_margin,pts_btts,pts_try_any,"
        "pts_try_first,pts_banker,exact_score FROM leaderboard "
        "WHERE group_id=? AND match_id=? AND username=?",
        (group_id, match_id, username)) as cur:
        row = await cur.fetchone()
    return dict(row) if row else None


async def main():
    await server._init_db()
    await setup_group(10, ["alice", "bob", "carol"])

    # ── Exact score, closest score, and the participation point ───────────────
    # Result 27-24: SA win by 3 (band 1-7), both teams scored.
    m = "m-basic"
    await predict(m, "alice", 27, 24)   # exact
    await predict(m, "bob",   26, 24)   # diff 1  -> closest of the rest
    await predict(m, "carol", 10, 40)   # diff 33 -> participation only
    await server._score_group(10, m, "greatest-rivalry", 27, 24, "South Africa", "1-7", 1)

    check("exact score = 5", (await points(10, m, "alice"))["pts_score"], 5)
    check("exact flagged", (await points(10, m, "alice"))["exact_score"], 1)
    check("closest non-exact = 1 (exact holder owns min_diff)",
          (await points(10, m, "bob"))["pts_score"], 1)
    check("furthest still scores participation point",
          (await points(10, m, "carol"))["pts_score"], 1)

    # With no exact prediction the closest gets 3.
    m = "m-closest"
    await predict(m, "alice", 25, 24)   # diff 2 -> closest
    await predict(m, "bob",   10, 40)   # diff 33
    await server._score_group(10, m, "greatest-rivalry", 27, 24, "South Africa", "1-7", 1)
    check("closest (no exact) = 3", (await points(10, m, "alice"))["pts_score"], 3)
    check("not closest = 1", (await points(10, m, "bob"))["pts_score"], 1)

    # ── Each prediction type in isolation ─────────────────────────────────────
    m = "m-types"
    await predict(m, "alice", 27, 24, winner="South Africa", margin="1-7", btts=1,
                  try_any="Cheslin Kolbe", try_first="Cheslin Kolbe")
    await predict(m, "bob", 27, 24, winner="New Zealand", margin="8-14", btts=0,
                  try_any="Nobody Here", try_first="Nobody Here")
    scorers = [{"name": "Cheslin Kolbe", "team": "South Africa", "clock": "5'"},
               {"name": "Will Jordan", "team": "New Zealand", "clock": "20'"}]
    await server._score_group(10, m, "greatest-rivalry", 27, 24, "South Africa", "1-7", 1,
                              scorers, "Cheslin Kolbe")
    a = await points(10, m, "alice")
    b = await points(10, m, "bob")
    check("winner correct = 2", a["pts_winner"], 2)
    check("winner wrong = 0", b["pts_winner"], 0)
    check("margin correct = 2", a["pts_margin"], 2)
    check("margin wrong = 0", b["pts_margin"], 0)
    check("btts correct = 1", a["pts_btts"], 1)
    check("btts wrong = 0", b["pts_btts"], 0)
    check("anytime try correct = 3", a["pts_try_any"], 3)
    check("anytime try wrong = 0", b["pts_try_any"], 0)
    check("first try correct = 4", a["pts_try_first"], 4)
    check("first try wrong = 0", b["pts_try_first"], 0)
    check("max points one match = 5+2+2+1+3+4", a["points"], 17)

    # ── Type-coercion regressions (the May 2026 bugs) ─────────────────────────
    # btts stored as TEXT must still compare equal to an INTEGER result.
    m = "m-coerce"
    await predict(m, "alice", 27, 24, btts="1")
    await predict(m, "bob", 27, 24, btts="0")
    await server._score_group(10, m, "greatest-rivalry", 27, 24, "South Africa", "1-7", 1)
    check("btts TEXT '1' matches INTEGER 1", (await points(10, m, "alice"))["pts_btts"], 1)
    check("btts TEXT '0' does not match INTEGER 1", (await points(10, m, "bob"))["pts_btts"], 0)

    # is_banker stored as TEXT '0' must not be treated as truthy.
    m = "m-banker-text"
    await predict(m, "alice", 27, 24, banker="0")
    await predict(m, "bob", 27, 24, banker="1")
    await server._score_group(10, m, "greatest-rivalry", 27, 24, "South Africa", "1-7", 1)
    check("is_banker TEXT '0' is not truthy", (await points(10, m, "alice"))["pts_banker"], 0)
    check("is_banker TEXT '1' doubles", (await points(10, m, "bob"))["pts_banker"] > 0, True)

    # ── Banker doubling ───────────────────────────────────────────────────────
    m = "m-banker"
    await predict(m, "alice", 27, 24, winner="South Africa", margin="1-7", btts=1, banker=1)
    await predict(m, "bob", 27, 24, winner="South Africa", margin="1-7", btts=1, banker=0)
    await server._score_group(10, m, "greatest-rivalry", 27, 24, "South Africa", "1-7", 1)
    a = await points(10, m, "alice")
    b = await points(10, m, "bob")
    check("banker doubles the total", a["points"], b["points"] * 2)
    check("pts_banker records only the bonus half", a["pts_banker"], b["points"])
    check("component columns are not doubled", a["pts_score"], b["pts_score"])

    # ── Group scoping ─────────────────────────────────────────────────────────
    # A group that does not follow the league must not score the match at all.
    await setup_group(11, ["alice"], leagues=("urc",))
    m = "m-scope"
    await predict(m, "alice", 27, 24)
    await server._score_group(11, m, "greatest-rivalry", 27, 24, "South Africa", "1-7", 1)
    check("unsubscribed league does not score", await points(11, m, "alice"), None)

    # A group with a prediction type disabled must award nothing for it.
    await setup_group(12, ["alice"], types=("score",))
    m = "m-types-off"
    await predict(m, "alice", 27, 24, winner="South Africa", margin="1-7", btts=1)
    await server._score_group(12, m, "greatest-rivalry", 27, 24, "South Africa", "1-7", 1)
    p = await points(12, m, "alice")
    check("disabled winner awards 0", p["pts_winner"], 0)
    check("disabled margin awards 0", p["pts_margin"], 0)
    check("disabled btts awards 0", p["pts_btts"], 0)
    check("enabled score still awards", p["pts_score"], 5)

    # ── Rescoring is idempotent ───────────────────────────────────────────────
    m = "m-idem"
    await predict(m, "alice", 27, 24, winner="South Africa", banker=1)
    await server._score_group(10, m, "greatest-rivalry", 27, 24, "South Africa", "1-7", 1)
    first = await points(10, m, "alice")
    for _ in range(3):
        await server._score_group(10, m, "greatest-rivalry", 27, 24, "South Africa", "1-7", 1)
    check("rescoring does not compound points", (await points(10, m, "alice"))["points"],
          first["points"])

    # Two-pass scoring: try scorers arrive late and must top up, not double-count.
    m = "m-twopass"
    await predict(m, "alice", 27, 24, winner="South Africa", try_any="Cheslin Kolbe")
    await server._score_group(10, m, "greatest-rivalry", 27, 24, "South Africa", "1-7", 1)
    check("pass 1 has no try points", (await points(10, m, "alice"))["pts_try_any"], 0)
    await server._score_group(10, m, "greatest-rivalry", 27, 24, "South Africa", "1-7", 1,
                              scorers, "Cheslin Kolbe")
    p = await points(10, m, "alice")
    check("pass 2 adds try points", p["pts_try_any"], 3)
    check("pass 2 total is not compounded", p["points"], 5 + 2 + 3)

    # ── Streaks ───────────────────────────────────────────────────────────────
    # Both /me and /me/card call these helpers, so they cannot report different
    # numbers for the same stat the way the two old inline versions did.
    async def resolve(mid, kickoff, fh, fa, winner):
        await server._db.execute(
            """INSERT OR REPLACE INTO match_results
               (match_id,match_title,team_home,team_away,tournament,kickoff_ts,
                final_home,final_away,entered_by,entered_at,res_winner,res_margin,res_btts)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (mid, "T", "South Africa", "New Zealand", "greatest-rivalry", kickoff,
             fh, fa, "test", time.time(), winner, "1-7", 1))
        await server._db.commit()

    await setup_group(20, ["dave", "erin"])
    base = 1780000000.0
    # dave tops m1 and m2, is beaten on m3 (oldest). Ordered by kickoff, so the
    # streak should be 2 and must stop at m3 rather than running to the end.
    for i, (mid, dave_sc, erin_sc) in enumerate(
            [("s3", (27, 24), (10, 40)), ("s2", (27, 24), (10, 40)), ("s1", (10, 40), (27, 24))]):
        kickoff = base - i * 604800
        await resolve(mid, kickoff, 27, 24, "South Africa")
        await server._db.execute("UPDATE predictions SET kickoff_ts=? WHERE match_id=?", (kickoff, mid))
        await predict(mid, "dave", *dave_sc)
        await predict(mid, "erin", *erin_sc)
        await server._db.execute("UPDATE predictions SET kickoff_ts=? WHERE match_id=?", (kickoff, mid))
        await server._db.commit()
        await server._score_group(20, mid, "greatest-rivalry", 27, 24, "South Africa", "1-7", 1)

    check("top-score streak stops at the first loss", await server._top_score_streak("dave"), 2)
    check("beaten player has no top-score streak", await server._top_score_streak("erin"), 0)
    check("streak is 0 for a user with no rows", await server._top_score_streak("nobody"), 0)

    # Winner streak reads predictions directly, so it works for a user with no
    # leaderboard rows at all — the case that left timo's profile blank.
    for i, (mid, pick) in enumerate(
            [("w3", "South Africa"), ("w2", "South Africa"), ("w1", "New Zealand")]):
        kickoff = base - i * 604800
        await resolve(mid, kickoff, 27, 24, "South Africa")
        await predict(mid, "ghost", 27, 24, winner=pick)
        await server._db.execute("UPDATE predictions SET kickoff_ts=? WHERE match_id=?", (kickoff, mid))
        await server._db.commit()

    # Best-ever run survives a bad result, unlike the current streak.
    check("best run remembers a broken streak",
          await server._best_top_score_streak("dave"), 2)
    # erin topped the oldest match (dave was furthest out there), so her current streak
    # is 0 but her best is 1 — exactly the case the stat exists to show.
    check("current streak is 0 for erin", await server._top_score_streak("erin"), 0)
    check("but her best run remembers the one she won",
          await server._best_top_score_streak("erin"), 1)
    check("best run is 0 for a player with no rows at all",
          await server._best_top_score_streak("nobody"), 0)

    check("winner streak counts consecutive correct picks",
          await server._winner_streak("ghost"), 2)
    check("winner streak works with zero leaderboard rows",
          (await points(20, "w3", "ghost")) is None and await server._winner_streak("ghost") == 2, True)

    # ── Global opt-in ─────────────────────────────────────────────────────────
    # A player's profile is built only from groups they actually compete in, so
    # opting out of Global must remove it from their numbers — and opting back in
    # must restore them, since leaving keeps the rows and only stops counting them.
    await setup_group(server.GLOBAL_GROUP_ID, ["opter"], leagues=("greatest-rivalry",))
    await setup_group(30, ["opter"], leagues=("greatest-rivalry",))

    m = "m-optin"
    await resolve(m, base, 27, 24, "South Africa")
    await predict(m, "opter", 27, 24, winner="South Africa")
    await server._db.execute("UPDATE predictions SET kickoff_ts=? WHERE match_id=?", (base, m))
    await server._db.commit()
    await server._score_group(server.GLOBAL_GROUP_ID, m, "greatest-rivalry",
                              27, 24, "South Africa", "1-7", 1)
    await server._score_group(30, m, "greatest-rivalry", 27, 24, "South Africa", "1-7", 1)

    check("opted in to Global", await server._in_global("opter"), True)
    both = await server._profile_totals("opter")
    check("one prediction scored in 2 groups counts as 1 match", both["matches"], 1)
    check("points are not doubled by group count", both["total_pts"], 7)

    # Leave Global — the row survives but must stop counting.
    await server._db.execute("DELETE FROM group_members WHERE group_id=? AND username=?",
                             (server.GLOBAL_GROUP_ID, "opter"))
    await server._db.commit()
    check("opted out of Global", await server._in_global("opter"), False)
    check("Global row survives the opt-out",
          (await points(server.GLOBAL_GROUP_ID, m, "opter")) is not None, True)
    out = await server._profile_totals("opter")
    check("still counts the remaining group", out["matches"], 1)
    check("points unchanged — other group still counts", out["total_pts"], 7)

    # A player in no groups at all has an empty profile rather than a crash.
    await server._db.execute("DELETE FROM group_members WHERE username=?", ("opter",))
    await server._db.commit()
    none = await server._profile_totals("opter")
    check("no groups means no points", none["total_pts"], 0)
    check("no groups means no matches", none["matches"], 0)
    check("no groups means no streak", await server._top_score_streak("opter"), 0)

    # ── Backfill on join ──────────────────────────────────────────────────────
    # Someone joining after matches resolved previously arrived with a blank record,
    # because _score_group only scores members present at scoring time.
    await setup_group(40, ["early"], leagues=("greatest-rivalry",))
    m = "m-backfill"
    await resolve(m, base, 27, 24, "South Africa")
    await predict(m, "early", 20, 20, winner="South Africa")
    await predict(m, "late", 27, 24, winner="South Africa")   # predicted, not yet a member
    for u in ("early", "late"):
        await server._db.execute(
            "UPDATE predictions SET kickoff_ts=? WHERE match_id=? AND username=?", (base, m, u))
    await server._db.commit()
    await server._score_group(40, m, "greatest-rivalry", 27, 24, "South Africa", "1-7", 1)

    check("non-member is not scored", await points(40, m, "late"), None)
    check("member is scored", (await points(40, m, "early")) is not None, True)

    # Join late, then backfill.
    await server._db.execute(
        "INSERT OR IGNORE INTO users (username,password_hash,is_admin,created_at) VALUES(?,?,0,?)",
        ("late", "x", time.time()))
    await server._db.execute(
        "INSERT OR IGNORE INTO group_members (group_id,username,role,joined_at) VALUES(?,?,?,?)",
        (40, "late", "member", time.time()))
    await server._db.commit()
    done = await server._backfill_member(40, "late")

    check("backfill scores the joiner's resolved matches", done, 1)
    check("joiner now has a row", (await points(40, m, "late")) is not None, True)
    check("joiner's exact score is recognised", (await points(40, m, "late"))["pts_score"], 5)

    # Running it again must be a no-op — it only targets matches with no row yet.
    check("backfill is not repeated for already-scored matches",
          await server._backfill_member(40, "late"), 0)

    # Only leagues the group follows are backfilled.
    await setup_group(41, ["late"], leagues=("urc",))
    check("backfill respects the group's leagues",
          await server._backfill_member(41, "late"), 0)

    # ── Manually entered try scorers ──────────────────────────────────────────
    # ESPN carries no data for the tour's provincial games, so their try predictions
    # can only resolve from what an admin types in. Same shape ESPN produces, so the
    # scoring path cannot tell the difference.
    typed = [n.strip() for n in "Cheslin Kolbe, Will Jordan, Damian Willemse".split(",") if n.strip()]
    manual_scorers = [{"name": n, "team": "", "clock": ""} for n in typed]

    await setup_group(50, ["hit", "miss"])
    m = "m-manualtry"
    await resolve(m, base, 27, 24, "South Africa")
    await predict(m, "hit", 27, 24, try_any="Will Jordan", try_first="Cheslin Kolbe")
    await predict(m, "miss", 27, 24, try_any="Nobody", try_first="Will Jordan")
    for u in ("hit", "miss"):
        await server._db.execute(
            "UPDATE predictions SET kickoff_ts=? WHERE match_id=? AND username=?", (base, m, u))
    await server._db.commit()
    await server._score_group(50, m, "greatest-rivalry", 27, 24, "South Africa", "1-7", 1,
                              manual_scorers, typed[0])

    check("typed try scorer resolves anytime try",
          (await points(50, m, "hit"))["pts_try_any"], 3)
    check("typed first scorer resolves first try",
          (await points(50, m, "hit"))["pts_try_first"], 4)
    check("wrong anytime pick scores nothing",
          (await points(50, m, "miss"))["pts_try_any"], 0)
    check("right scorer but not first scores no first-try points",
          (await points(50, m, "miss"))["pts_try_first"], 0)
    check("manual scorers parse to the same shape ESPN yields",
          server._scorer_names(manual_scorers), typed)

    # ── Player-name matching ──────────────────────────────────────────────────
    # Sources disagree on capitalisation and spacing. Awarding try points by raw string
    # comparison meant a correct pick could silently score nothing.
    np = server._norm_player
    check("case differences collapse", np("Ethan De Groot"), np("Ethan de Groot"))
    check("extra spacing collapses", np("Will  Jordan "), np("Will Jordan"))
    check("different players stay different", np("Will Jordan") == np("Rieko Ioane"), False)
    check("None is safe", np(None), "")

    await setup_group(60, ["caps"])
    m = "m-caps"
    await resolve(m, base, 27, 24, "South Africa")
    # Pick stored the way the squad sheet writes it; result the way ESPN writes it.
    await predict(m, "caps", 27, 24, try_any="Ethan De Groot", try_first="Ethan De Groot")
    await server._db.execute("UPDATE predictions SET kickoff_ts=? WHERE match_id=?", (base, m))
    await server._db.commit()
    await server._score_group(60, m, "greatest-rivalry", 27, 24, "South Africa", "1-7", 1,
                              [{"name": "Ethan de Groot", "team": "", "clock": ""}],
                              "Ethan de Groot")
    check("anytime try scores despite capitalisation mismatch",
          (await points(60, m, "caps"))["pts_try_any"], 3)
    check("first try scores despite capitalisation mismatch",
          (await points(60, m, "caps"))["pts_try_first"], 4)

    # ── Season labelling ──────────────────────────────────────────────────────
    # The point of this is that next season's fixtures never pile onto this
    # season's under the same league slug.
    sf = server._season_for
    ts_ = lambda s: datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()

    check("URC Sep 2026 is season 2026-27", sf("urc", ts_("2026-09-25")), "2026-27")
    check("URC Jun 2027 is still 2026-27", sf("urc", ts_("2027-06-15")), "2026-27")
    check("URC Sep 2027 rolls to 2027-28", sf("urc", ts_("2027-09-20")), "2027-28")
    check("same league, consecutive seasons differ",
          sf("urc", ts_("2026-09-25")) == sf("urc", ts_("2027-09-25")), False)
    check("Premiership splits the same way", sf("premiership", ts_("2027-01-10")), "2026-27")
    check("Top 14 splits the same way", sf("top-14", ts_("2026-11-01")), "2026-27")

    # Calendar-year competitions must NOT be split.
    check("the tour is a calendar year", sf("greatest-rivalry", ts_("2026-08-22")), "2026")
    check("Bledisloe is a calendar year", sf("bledisloe-cup", ts_("2026-10-10")), "2026")
    check("Super Rugby is a calendar year", sf("super-rugby", ts_("2026-04-10")), "2026")
    check("Nations Championship is a calendar year",
          sf("nations-championship", ts_("2026-11-07")), "2026")
    check("next year's Bledisloe is a new season",
          sf("bledisloe-cup", ts_("2027-10-09")), "2027")
    check("no kickoff yields no season", sf("urc", 0), "")

    # Every league the app knows about must produce a season label.
    for lg in server.ALL_ESPN_LEAGUES:
        check(f"{lg} yields a season label", bool(sf(lg, ts_("2026-10-01"))), True)

    # Scoring must stamp the season and kickoff onto the leaderboard row.
    m = "m-season"
    await resolve(m, ts_("2026-08-22"), 27, 24, "South Africa")
    await predict(m, "alice", 27, 24)
    await server._db.execute("UPDATE predictions SET kickoff_ts=? WHERE match_id=?",
                             (ts_("2026-08-22"), m))
    await server._db.commit()
    await server._score_group(10, m, "greatest-rivalry", 27, 24, "South Africa", "1-7", 1)
    async with server._db.execute(
        "SELECT season, kickoff_ts FROM leaderboard WHERE match_id=? AND username=?",
        (m, "alice")) as cur:
        row = await cur.fetchone()
    check("leaderboard row carries the season", row["season"], "2026")
    check("leaderboard row carries the kickoff", row["kickoff_ts"], ts_("2026-08-22"))

    # ── Season floor ──────────────────────────────────────────────────────────
    # The companion backfill stores every ESPN result it can see, reaching 45 days
    # back. Without a floor it refilled the table with matches the cutover had just
    # removed — caught in production minutes after the 5 Aug cutover.
    floor = server.RESULTS_FLOOR_TS
    check("floor is the Nations Championship cutoff", floor, ts_("2026-07-04"))
    check("a June match is below the floor", ts_("2026-06-27") < floor, True)
    check("a Nations Championship match is above it", ts_("2026-07-11") > floor, True)
    check("the tour is above it", ts_("2026-08-07") > floor, True)

    # ── Custom competitions ───────────────────────────────────────────────────
    # A one-off event group follows no leagues at all; its fixtures come from a custom
    # competition. Scoring used to gate purely on group_leagues, so such a group could
    # never score: fixtures rendered, predictions saved, standings stayed empty.
    await setup_group(60, ["cc1", "cc2"], leagues=())
    await server._db.execute(
        "INSERT OR REPLACE INTO custom_competitions (id,group_id,name,slug,created_at) "
        "VALUES(?,?,?,?,?)", (60, 60, "One-off", "one-off", time.time()))
    await server._db.execute(
        "INSERT OR REPLACE INTO custom_competition_matches "
        "(comp_id,match_id,espn_id,league_id,team_home,team_away,kickoff_ts,tournament) "
        "VALUES(?,?,?,?,?,?,?,?)",
        (60, "CC-1", "CC-1", 289234, "Argentina", "South Africa", KICKOFF, "international"))
    await server._db.commit()

    await predict("CC-1", "cc1", 20, 20, winner="draw", tournament="international")
    await server._score_group(60, "CC-1", "international", 20, 20, "draw", "1-7", 1)
    got = await points(60, "CC-1", "cc1")
    check("a league-less group scores its custom-competition match", bool(got), True)
    check("and awards the winner points", (got or {}).get("pts_winner"), 2)

    # A match nobody picked into the competition must still be ignored by that group.
    await predict("CC-2", "cc1", 20, 20, winner="draw", tournament="international")
    await server._score_group(60, "CC-2", "international", 20, 20, "draw", "1-7", 1)
    check("a match outside both leagues and the competition is not scored",
          await points(60, "CC-2", "cc1"), None)

    # ── Predict Now is scoped to the player's own groups ──────────────────────
    # Global follows all fifteen competitions, so counting it advertised every fixture in
    # the app no matter what a player had joined.
    await setup_group(95, ["scoped"], leagues=("urc",))
    scope = await server._player_league_scope("scoped")
    check("scope is the player's own group's leagues", scope, {"urc"})
    check("Global's other leagues are not pulled in", "six-nations" in scope, False)

    # A player whose only group is Global keeps the full list — Global is then their
    # entire scoring context and everything really is relevant.
    await server._db.execute(
        "INSERT OR IGNORE INTO users (username,password_hash,is_admin,created_at) VALUES(?,?,0,?)",
        ("lonely", "x", time.time()))
    await server._db.commit()
    lonely = await server._player_league_scope("lonely")
    check("a player with no groups of their own sees everything",
          lonely == set(server.ALL_ESPN_LEAGUES), True)

    # ── The banker backfill runs once, not on every boot ──────────────────────
    # It was written as a plain INSERT OR IGNORE at startup, so every restart re-armed
    # the banker for every group and silently undid deliberate opt-outs.
    await setup_group(90, ["mig"], types=NO_BANKER)
    await server._db.execute(
        "DELETE FROM group_prediction_types WHERE group_id=90 AND prediction_type='banker'")
    await server._db.commit()
    # Close first: _init_db opens a fresh aiosqlite connection, and leaving the old one
    # dangling leaks its non-daemon thread and hangs the interpreter at exit — the very
    # fault the per-suite timeout in all.sh was added to catch, which is how this was
    # caught within a minute of writing it.
    await server._db.close()
    await server._init_db()          # simulate a restart
    async with server._db.execute(
        "SELECT 1 FROM group_prediction_types WHERE group_id=90 AND prediction_type='banker'"
    ) as cur:
        rearmed = await cur.fetchone()
    check("a restart does not re-arm banker on a group that switched it off",
          rearmed, None)

    # Global has its own startup loop that re-asserts its defaults every boot, keyed on
    # group_id 1. Banker must not be in it, or Global is re-armed on every deploy no
    # matter what the admin set — which is exactly what happened twice.
    await server._db.execute(
        "DELETE FROM group_prediction_types WHERE group_id=1 AND prediction_type='banker'")
    await server._db.commit()
    await server._db.close()
    await server._init_db()
    async with server._db.execute(
        "SELECT 1 FROM group_prediction_types WHERE group_id=1 AND prediction_type='banker'"
    ) as cur:
        global_rearmed = await cur.fetchone()
    check("a restart does not re-arm banker on Global either", global_rearmed, None)

    # ── Banker is a per-group rule ────────────────────────────────────────────
    # Same prediction, same banker flag, two groups: one honours it, one does not.
    # Banker used to be applied unconditionally, so a group with a single fixture a
    # month had its table decided by a doubling that made no sense for its format.
    await setup_group(80, ["bk"], types=ALL_TYPES)           # banker on, as by default
    await setup_group(81, ["bk"], types=NO_BANKER)           # deliberately switched off
    await predict("BK-G", "bk", 27, 24, winner="South Africa", banker=1)
    await server._score_group(80, "BK-G", "greatest-rivalry", 27, 24, "South Africa", "1-7", 1)
    await server._score_group(81, "BK-G", "greatest-rivalry", 27, 24, "South Africa", "1-7", 1)
    on, off = await points(80, "BK-G", "bk"), await points(81, "BK-G", "bk")
    check("banker doubles where the group allows it", on["pts_banker"] > 0, True)
    check("banker is ignored where the group does not", off["pts_banker"], 0)
    check("and the doubled total is exactly twice the plain one", on["points"], off["points"] * 2)

    # ── Banker cannot be spent twice in a week ────────────────────────────────
    # Bank Friday, let it pay, then bank Saturday: the flag used to move while the
    # already-scored points stayed banked, so the doubling accumulated all week.
    await setup_group(70, ["bank1"], leagues=("international",))
    wk = server._banker_week_start(KICKOFF)
    now = KICKOFF + 3600            # Friday's match has kicked off
    await predict("BK-FRI", "bank1", 10, 10, banker=1, tournament="international")
    await server._db.execute("UPDATE predictions SET kickoff_ts=? WHERE match_id=?",
                             (KICKOFF, "BK-FRI"))
    await server._db.commit()
    check("a started banker this week is reported as spent",
          await server._banker_spent_on("bank1", wk, now) is not None, True)

    # Before it kicks off it is still movable, which is the intended behaviour.
    check("an unstarted banker is not spent",
          await server._banker_spent_on("bank1", wk, KICKOFF - 3600), None)

    # And a banker in a different week never blocks this one.
    check("a banker in another week does not count",
          await server._banker_spent_on("bank1", wk + server.BANKER_WEEK,
                                        now + server.BANKER_WEEK), None)

    # ── Banker week bucketing ─────────────────────────────────────────────────
    wk = server._banker_week_start
    check("same round shares a bucket", wk(KICKOFF), wk(KICKOFF + 3600 * 24))
    check("a week later is a new bucket", wk(KICKOFF) == wk(KICKOFF + 604800), False)
    check("bucket is 7 days wide", wk(KICKOFF + 604800) - wk(KICKOFF), 604800.0)

    # aiosqlite's worker thread is non-daemon, so leaving the connection open kept the
    # interpreter alive in threading._shutdown long after sys.exit(0). all.sh then blocked
    # on this suite forever and test_auth + test_live_espn never ran at all — while every
    # line it printed said "0 failed". Closing it is what lets the process exit.
    await server._db.close()


asyncio.run(main())

print(f"scoring: {CHECKS[0]} checks, {len(FAILURES)} failed")
for f in FAILURES:
    print(f"  FAIL {f}")
sys.exit(1 if FAILURES else 0)
