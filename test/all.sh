#!/usr/bin/env bash
# Scrum test suite.
#
# Runs inside the app container, which is the only place the deps live:
#     ./test/all.sh                  # against the local dev container
#     CONTAINER=scrum_dashboard ./test/all.sh
#
# Exits non-zero if any suite fails, so it can gate a deploy.
set -uo pipefail

CONTAINER="${CONTAINER:-scrum_dashboard}"
FAILED=0
declare -a RESULTS=()

if ! docker ps --format '{{.Names}}' | grep -qx "$CONTAINER"; then
    echo "error: container '$CONTAINER' is not running"
    echo "hint:  docker compose up --build -d"
    exit 2
fi

# Tests live outside the image, so copy them in each run — no rebuild needed to
# iterate on a test, and the image stays free of test code. Clear the target first:
# docker cp into an existing directory nests instead of replacing.
docker exec "$CONTAINER" rm -rf /app/test
docker cp "$(dirname "$0")" "$CONTAINER:/app/test" >/dev/null || {
    echo "error: could not copy test/ into $CONTAINER"; exit 2; }

for suite in test_derived_leagues test_scoring test_live_espn; do
    echo "── $suite ──────────────────────────────────────────"
    if docker exec "$CONTAINER" python3 "/app/test/$suite.py"; then
        RESULTS+=("  PASS  $suite")
    else
        RESULTS+=("  FAIL  $suite")
        FAILED=1
    fi
    echo
done

echo "════════════════════════════════════════════════════"
printf '%s\n' "${RESULTS[@]}"
echo "════════════════════════════════════════════════════"

if [ "$FAILED" -ne 0 ]; then
    echo "SUITE FAILED"
    exit 1
fi
echo "ALL GREEN"
