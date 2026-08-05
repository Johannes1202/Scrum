"""Login throttling and session tests.

The live site is public and the accounts use short passwords by choice, so the
throttle is the control that actually stands between the login form and a guess.
It has to hold, and it has to fail open rather than lock everyone out.

    docker exec scrum_dashboard python3 /app/test/test_auth.py
"""
import asyncio
import os
import sys
import pathlib
import tempfile
import time

_tmp = tempfile.mkdtemp(prefix="scrum-auth-")
os.environ["DB_PATH"] = os.path.join(_tmp, "test.db")
os.environ.setdefault("AVATAR_DIR", os.path.join(_tmp, "avatars"))
os.environ.setdefault("SESSION_SECRET", "test-secret-for-auth-suite")
sys.path.insert(0, "/app")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dashboard"))

import server  # noqa: E402

FAILURES = []
CHECKS = [0]


def check(label, got, want):
    CHECKS[0] += 1
    if got != want:
        FAILURES.append(f"{label}\n     got:  {got!r}\n     want: {want!r}")


class FakeRedis:
    """Minimal INCR/EXPIRE/TTL/GET/DELETE, enough to exercise the throttle."""

    def __init__(self):
        self.store, self.ttls, self.fail = {}, {}, False

    async def get(self, k):
        if self.fail:
            raise RuntimeError("redis down")
        return self.store.get(k)

    async def incr(self, k):
        if self.fail:
            raise RuntimeError("redis down")
        self.store[k] = int(self.store.get(k, 0)) + 1
        return self.store[k]

    async def expire(self, k, ttl):
        self.ttls[k] = ttl

    async def ttl(self, k):
        return self.ttls.get(k, -1)

    async def delete(self, *keys):
        for k in keys:
            self.store.pop(k, None)
            self.ttls.pop(k, None)


class FakeRequest:
    def __init__(self, headers=None, host="10.0.0.1"):
        self.headers = headers or {}
        self.client = type("C", (), {"host": host})()


async def main():
    # ── Client IP resolution ──────────────────────────────────────────────────
    # Behind the tunnel every request appears to come from the tunnel container, so
    # without the forwarded header one shared limit would cover all players at once.
    ip = server._client_ip
    check("cf-connecting-ip wins",
          ip(FakeRequest({"cf-connecting-ip": "203.0.113.7"})), "203.0.113.7")
    check("x-forwarded-for is used next",
          ip(FakeRequest({"x-forwarded-for": "203.0.113.8, 10.0.0.5"})), "203.0.113.8")
    check("falls back to the socket address", ip(FakeRequest()), "10.0.0.1")
    check("a spoofed header cannot inject an oversized key",
          len(ip(FakeRequest({"cf-connecting-ip": "x" * 500}))) <= 45, True)

    # ── Per-username lock ─────────────────────────────────────────────────────
    server._redis = FakeRedis()
    for i in range(server.LOGIN_MAX_USER):
        check(f"attempt {i+1} still allowed",
              await server._login_retry_after("darcy", "1.1.1.1"), 0)
        await server._login_record_failure("darcy", "1.1.1.1")
    blocked = await server._login_retry_after("darcy", "1.1.1.1")
    check("locked once the limit is reached", blocked > 0, True)
    check("lock lasts the full window", blocked, server.LOGIN_WINDOW)

    # A different account from the same address is unaffected until the IP limit.
    check("another account is not collateral damage",
          await server._login_retry_after("botes", "1.1.1.1"), 0)

    # A correct password clears the lock.
    await server._login_clear("darcy", "1.1.1.1")
    check("successful login clears the lock",
          await server._login_retry_after("darcy", "1.1.1.1"), 0)

    # ── Per-address lock ──────────────────────────────────────────────────────
    server._redis = FakeRedis()
    for i in range(server.LOGIN_MAX_IP):
        await server._login_record_failure(f"user{i}", "9.9.9.9")
    check("spraying many accounts trips the address limit",
          await server._login_retry_after("someoneelse", "9.9.9.9") > 0, True)
    check("a different address is unaffected",
          await server._login_retry_after("someoneelse", "9.9.9.10"), 0)

    # The address limit must be loose enough that a shared household is not locked
    # out by one person fumbling their password.
    check("address limit is well above the per-user limit",
          server.LOGIN_MAX_IP > server.LOGIN_MAX_USER * 3, True)

    # ── Fails open ────────────────────────────────────────────────────────────
    # Locking every player out of a live prediction game because Redis blipped would
    # be worse than the attack the throttle exists to stop.
    server._redis = FakeRedis()
    server._redis.fail = True
    check("a broken Redis does not lock anyone out",
          await server._login_retry_after("darcy", "1.1.1.1"), 0)
    await server._login_record_failure("darcy", "1.1.1.1")  # must not raise

    server._redis = None
    check("no Redis at all does not lock anyone out",
          await server._login_retry_after("darcy", "1.1.1.1"), 0)
    await server._login_record_failure("darcy", "1.1.1.1")
    await server._login_clear("darcy", "1.1.1.1")

    # ── Messaging ─────────────────────────────────────────────────────────────
    check("retry message rounds up to whole minutes",
          server._retry_msg(61), "Too many failed attempts. Try again in 2 minutes.")
    check("one minute is not pluralised",
          server._retry_msg(30), "Too many failed attempts. Try again in 1 minute.")

    # ── Session tokens ────────────────────────────────────────────────────────
    tok = server._make_token("darcy")
    check("a valid token round-trips", server._token_to_user(tok), "darcy")
    check("a tampered token is rejected", server._token_to_user(tok[:-2] + "xy"), None)
    check("an empty token is rejected", server._token_to_user(""), None)
    check("junk is rejected", server._token_to_user("not-a-token"), None)

    # A token signed with a different secret must not be accepted.
    real = server.SESSION_SECRET
    server.SESSION_SECRET = "a-different-secret"
    forged = server._make_token("darcy")
    server.SESSION_SECRET = real
    check("a token signed with another secret is rejected",
          server._token_to_user(forged), None)

    # Expiry is enforced.
    old_ts = str(int(time.time()) - server.SESSION_MAX_AGE - 60)
    import base64
    import hashlib
    import hmac
    sig = hmac.new(real.encode(), f"{old_ts}:darcy".encode(), hashlib.sha256).hexdigest()
    expired = base64.urlsafe_b64encode(f"{old_ts}:darcy:{sig}".encode()).decode()
    check("an expired token is rejected", server._token_to_user(expired), None)

    # ── Cookie hardening ──────────────────────────────────────────────────────
    check("COOKIE_SECURE is configurable", isinstance(server.COOKIE_SECURE, bool), True)

    # ── Invite links ──────────────────────────────────────────────────────────
    # A shareable link creates an account, so it must expire by default and the
    # expiry has to be enforced on the POST that acts, not only the GET that renders.
    check("shareable links expire by default", server.INVITE_DEFAULT_DAYS > 0, True)

    src = pathlib.Path("/app/server.py")
    if not src.exists():
        src = pathlib.Path(__file__).parent.parent / "dashboard" / "server.py"
    text = src.read_text()
    post_handler = text.split("async def group_join_link_post")[1].split("async def ")[0]
    check("the POST join path enforces expiry", "expires_at" in post_handler, True)
    check("the POST join path enforces use count", "max_uses" in post_handler, True)


asyncio.run(main())

print(f"auth: {CHECKS[0]} checks, {len(FAILURES)} failed")
for f in FAILURES:
    print(f"  FAIL {f}")
sys.exit(1 if FAILURES else 0)
