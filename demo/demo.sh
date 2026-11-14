#!/usr/bin/env bash
# End-to-end demo: full sync, slow-page responsiveness, concurrent submissions
# (409 without orphans), disconnect/resume, cursor expiry, incremental sync,
# retraction stickiness, and collector restart resumption.
#
# Usage:
#   docker compose up -d --build
#   ./demo/demo.sh
set -euo pipefail

REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
SOURCE=${SOURCE_URL:-http://localhost:8000}
COLLECTOR=${COLLECTOR_URL:-http://localhost:8001}
CUTOFF_1="2026-11-08T23:59:59Z"
CUTOFF_2="2026-11-10T23:59:59Z"

hr()    { printf '\n==================== %s ====================\n' "$1"; }
say()   { printf '\n--- %s ---\n' "$1"; }
req()   { curl -fsS "$@"; }
wait_for() {
  local url=$1
  for _ in $(seq 1 60); do
    curl -fsS "$url" >/dev/null 2>&1 && return 0
    sleep 1
  done
  echo "timeout waiting for $url" >&2
  exit 1
}
job_status() { req "$COLLECTOR/sync/jobs/$1" | jq -r .status; }
job_count()  { req "$COLLECTOR/sync/jobs" | jq 'length'; }
wait_status() {
  # wait_status <job_id> <expected_status...>
  local id=$1; shift
  local status
  for _ in $(seq 1 60); do
    status=$(job_status "$id")
    for want in "$@"; do
      [ "$status" = "$want" ] && return 0
    done
    sleep 1
  done
  return 1
}

wait_for "$SOURCE/health"
wait_for "$COLLECTOR/health"

hr "0. Reset demo state"
req -X POST "$SOURCE/control/reset" | jq .
req -X POST "$SOURCE/control/page-delay" -H 'Content-Type: application/json' \
  -d '{"seconds":0}' >/dev/null
req "$SOURCE/control/state" | jq .

# ---------------------------------------------------------------------------
hr "1. First full sync (closed interval, paged, shuffled/duplicated events)"
# ---------------------------------------------------------------------------
say "Slow the source down: 3s per page, so status/paper queries can be observed mid-fetch"
req -X POST "$SOURCE/control/page-delay" -H 'Content-Type: application/json' \
  -d '{"seconds":3}' | jq .

JOB=$(req -X POST "$COLLECTOR/sync/jobs" \
        -H 'Content-Type: application/json' \
        -d "{\"end\": \"$CUTOFF_1\"}" | jq .)
echo "$JOB" | jq '{id, interval_start, interval_end, status}'
JOB_ID=$(echo "$JOB" | jq -r .id)

sleep 1
say "While a slow page is being fetched: status and paper queries stay responsive"
curl -s -o /dev/null -w "  GET /sync/jobs/$JOB_ID -> HTTP %{http_code} in %{time_total}s\n" \
  "$COLLECTOR/sync/jobs/$JOB_ID"
curl -s -o /dev/null -w "  GET /papers             -> HTTP %{http_code} in %{time_total}s\n" \
  "$COLLECTOR/papers"
curl -s -o /dev/null -w "  GET /papers-count       -> HTTP %{http_code} in %{time_total}s\n" \
  "$COLLECTOR/papers-count"

say "Concurrent submissions while one job owns the execution slot"
BEFORE=$(job_count)
rm -f /tmp/conflict-*.json
for i in 1 2 3; do
  curl -sS -o "/tmp/conflict-$i.json" -w "%{http_code}\n" \
    -X POST "$COLLECTOR/sync/jobs" -H 'Content-Type: application/json' \
    -d "{\"end\": \"$CUTOFF_2\"}" >"/tmp/conflict-$i.code" &
done
wait
CODES=""
for i in 1 2 3; do CODES="$CODES $(cat "/tmp/conflict-$i.code")"; done
echo "  HTTP codes:$CODES"
cat /tmp/conflict-1.json | jq -r '"  rejected body: " + .detail' 2>/dev/null || true
AFTER=$(job_count)
JOBS_NOW=$(req "$COLLECTOR/sync/jobs")
echo "  jobs before=$BEFORE after=$AFTER (must be equal: 409 adds NO job row)"
if [ "$BEFORE" -ne "$AFTER" ]; then
  echo "FAIL: a rejected request created an orphan job" >&2
  echo "$JOBS_NOW" | jq . >&2
  exit 1
fi
echo "$JOBS_NOW" | jq -r '.[] | "  job \(.id): \(.status)"'
if echo "$JOBS_NOW" | jq -e '.[] | select(.status=="collecting")' >/dev/null; then
  echo "  (the one legitimate in-flight job is still collecting; rejected ones added no row)"
fi
rm -f /tmp/conflict-*.json /tmp/conflict-*.code

say "Polling job $JOB_ID until completed"
for _ in $(seq 1 40); do
  JOB=$(req "$COLLECTOR/sync/jobs/$JOB_ID")
  STATUS=$(echo "$JOB" | jq -r .status)
  echo "  status=$STATUS pages=$(echo "$JOB" | jq -r .pages_fetched) staged=$(echo "$JOB" | jq -r .staged_count)"
  [ "$STATUS" = "completed" ] && break
  [ "$STATUS" = "failed" ] && { echo "$JOB" | jq .; exit 1; }
  sleep 1
done
echo "$JOB" | jq '{id, status, pages_fetched, staged_count, cursor_resets}'
req -X POST "$SOURCE/control/page-delay" -H 'Content-Type: application/json' \
  -d '{"seconds":0}' >/dev/null

say "Immediately after completion: competition may 409 (slot hand-off), but retry is accepted"
COUNT_BEFORE=$(job_count)
RESUBMIT_ID=""
for attempt in $(seq 1 20); do
  CODE=$(curl -sS -o /tmp/resubmit.json -w '%{http_code}' \
    -X POST "$COLLECTOR/sync/jobs" -H 'Content-Type: application/json' \
    -d "{\"end\": \"$CUTOFF_1\"}")
  if [ "$CODE" = "201" ]; then
    RESUBMIT_ID=$(jq -r .id /tmp/resubmit.json)
    echo "  attempt $attempt: HTTP 201 -> job $RESUBMIT_ID accepted"
    break
  fi
  echo "  attempt $attempt: HTTP $CODE ($(jq -r .detail /tmp/resubmit.json))"
  [ "$(job_count)" -eq "$COUNT_BEFORE" ] \
    || { echo "FAIL: a rejected submit added a job row" >&2; exit 1; }
  sleep 0.3
done
[ -n "$RESUBMIT_ID" ] || { echo "submit never accepted" >&2; exit 1; }
wait_status "$RESUBMIT_ID" completed || { echo "resubmit did not complete" >&2; exit 1; }
echo "  job $RESUBMIT_ID completed; jobs=$(job_count)"
rm -f /tmp/resubmit.json

say "Completed time is now recorded (incremental baseline)"
req "$COLLECTOR/sync/state" | jq .

say "Library: 4 visible papers; P0001 and P0003 are retracted (hidden)"
req "$COLLECTOR/papers" | jq -r '.[] | "\(.paper_id) rev=\(.rev) retracted=\(.is_retracted)  \(.title)"'
req "$COLLECTOR/papers-count?include_retracted=true" | jq .

say "Retracted paper lookup -> HTTP 410 Gone; with include_retracted -> visible"
curl -s -o /dev/null -w "  GET /papers/P0001 -> HTTP %{http_code}\n" "$COLLECTOR/papers/P0001"
req "$COLLECTOR/papers/P0001?include_retracted=true" \
  | jq -r '"  with flag: \(.paper_id) rev=\(.rev) retracted=\(.is_retracted)"'

say "Highest revision wins (P0002 -> rev 3) despite stale duplicate delivery"
req "$COLLECTOR/papers/P0002" | jq -r '"  P0002 rev=\(.rev): \(.title)"'

# ---------------------------------------------------------------------------
hr "2. Disconnect mid-sync: inject source fault, then resume"
# ---------------------------------------------------------------------------
say "Append the incremental batch to the repository"
req -X POST "$SOURCE/control/append-batch" | jq .

say "Make the next /changes request fail with HTTP 503 (one-shot)"
req -X POST "$SOURCE/control/fail" -H 'Content-Type: application/json' -d '{"times":1}' | jq .

say "Start incremental sync (start defaults to last completed time)"
JOB2=$(req -X POST "$COLLECTOR/sync/jobs" \
         -H 'Content-Type: application/json' \
         -d "{\"end\": \"$CUTOFF_2\"}")
JOB2_ID=$(echo "$JOB2" | jq -r .id)
echo "$JOB2" | jq '{id, interval_start, interval_end, status}'

for _ in $(seq 1 30); do
  JOB2=$(req "$COLLECTOR/sync/jobs/$JOB2_ID")
  STATUS=$(echo "$JOB2" | jq -r .status)
  echo "  status=$STATUS pages=$(echo "$JOB2" | jq -r .pages_fetched) error=$(echo "$JOB2" | jq -r .error)"
  [ "$STATUS" = "failed" ] && break
  sleep 1
done
[ "$(echo "$JOB2" | jq -r .status)" = "failed" ] || { echo "expected failure" >&2; exit 1; }

say "Old library stays queryable; staged changes stay invisible (P0007 not found)"
curl -s -o /dev/null -w "  GET /papers/P0007 -> HTTP %{http_code} (404 expected)\n" "$COLLECTOR/papers/P0007"
req "$COLLECTOR/papers-count" | jq -r '"  visible papers while job failed: \(.count)"'

say "Ensure the injected fault is exhausted, then resume the SAME job"
req -X POST "$SOURCE/control/fail" -H 'Content-Type: application/json' -d '{"times":0}' >/dev/null
req -X POST "$COLLECTOR/sync/jobs/$JOB2_ID/resume" >/dev/null
for _ in $(seq 1 30); do
  JOB2=$(req "$COLLECTOR/sync/jobs/$JOB2_ID")
  STATUS=$(echo "$JOB2" | jq -r .status)
  echo "  status=$STATUS pages=$(echo "$JOB2" | jq -r .pages_fetched) resets=$(echo "$JOB2" | jq -r .cursor_resets)"
  [ "$STATUS" = "completed" ] && break
  sleep 1
done
echo "$JOB2" | jq '{id, status, pages_fetched, staged_count, cursor_resets}'

say "Incremental results: P0007 added, P0005 at rev 3"
req "$COLLECTOR/papers" | jq -r '.[] | "  \(.paper_id) rev=\(.rev) retracted=\(.is_retracted)"'
req "$COLLECTOR/papers/P0005" | jq -r '"  P0005 -> rev=\(.rev), authors=\(.authors)"'

say "Retraction cannot be overwritten by an old revision (P0002 retracted at rev 4)"
curl -s -o /dev/null -w "  GET /papers/P0002 -> HTTP %{http_code} (410 expected)\n" "$COLLECTOR/papers/P0002"
req "$COLLECTOR/papers/P0002?include_retracted=true" | jq -r '"  with flag: rev=\(.rev) retracted=\(.is_retracted)"'

# ---------------------------------------------------------------------------
hr "3. Cursor expiry: expired cursor re-fetches the ORIGINAL interval"
# ---------------------------------------------------------------------------
say "Add FOUR events so the window spans multiple pages (page_size=3)"
req -X POST "$SOURCE/control/append" -H 'Content-Type: application/json' \
  -d '{"events":[
    {"paper_id":"P0008","rev":1,"title":"Knowledge Graph Alignment for Libraries","authors":"M. Park","is_retracted":0,"changed_at":"2026-11-11T10:00:00Z"},
    {"paper_id":"P0010","rev":1,"title":"Discovery Layer Usability","authors":"O. Reed","is_retracted":0,"changed_at":"2026-11-11T11:00:00Z"},
    {"paper_id":"P0011","rev":1,"title":"Linked Data in Catalogs","authors":"P. Quinn","is_retracted":0,"changed_at":"2026-11-11T12:00:00Z"},
    {"paper_id":"P0012","rev":1,"title":"Retention Schedules for ETDs","authors":"Q. Park","is_retracted":0,"changed_at":"2026-11-11T13:00:00Z"}
  ]}' >/dev/null

say "Deterministic hook: invalidate cursors right after the FIRST /changes page"
req -X POST "$SOURCE/control/expire-after" \
  -H 'Content-Type: application/json' -d '{"requests":1}' | jq -c .

JOB3=$(req -X POST "$COLLECTOR/sync/jobs" \
         -H 'Content-Type: application/json' \
         -d '{"end":"2026-11-11T23:59:59Z"}')
JOB3_ID=$(echo "$JOB3" | jq -r .id)
ORIGINAL_END=$(echo "$JOB3" | jq -r .interval_end)

for _ in $(seq 1 30); do
  JOB3=$(req "$COLLECTOR/sync/jobs/$JOB3_ID")
  STATUS=$(echo "$JOB3" | jq -r .status)
  echo "  status=$STATUS pages=$(echo "$JOB3" | jq -r .pages_fetched) resets=$(echo "$JOB3" | jq -r .cursor_resets)"
  [ "$STATUS" = "failed" ] && break
  [ "$STATUS" = "completed" ] && break
  sleep 1
done

if [ "$(echo "$JOB3" | jq -r .status)" = "failed" ]; then
  echo "$JOB3" | jq '{id, status, error, cursor_resets, staged_count, interval_start, interval_end}'
  say "Resume: staging restarts from offset 0 but keeps the SAME interval/cutoff"
  req -X POST "$COLLECTOR/sync/jobs/$JOB3_ID/resume" >/dev/null
  for _ in $(seq 1 30); do
    JOB3=$(req "$COLLECTOR/sync/jobs/$JOB3_ID")
    STATUS=$(echo "$JOB3" | jq -r .status)
    echo "  status=$STATUS pages=$(echo "$JOB3" | jq -r .pages_fetched) resets=$(echo "$JOB3" | jq -r .cursor_resets)"
    [ "$STATUS" = "completed" ] && break
    sleep 1
  done
fi
echo "$JOB3" | jq '{id, status, pages_fetched, cursor_resets, interval_start, interval_end}'
say "The interval end never moved (no jump to a newer cutoff)"
[ "$(echo "$JOB3" | jq -r .interval_end)" = "$ORIGINAL_END" ] \
  && echo "  OK: interval_end still $ORIGINAL_END"
say "All four papers from the re-fetched original interval are published"
for p in P0008 P0010 P0011 P0012; do
  req "$COLLECTOR/papers/$p" | jq -r --arg p "$p" '"  " + $p + " rev=\(.rev): \(.title)"'
done

# ---------------------------------------------------------------------------
hr "4. Restart resilience: kill the collector WHILE a job is collecting"
# ---------------------------------------------------------------------------
say "Append an event and make each page take 5s, so the job is still collecting"
req -X POST "$SOURCE/control/append" -H 'Content-Type: application/json' \
  -d '{"events":[{"paper_id":"P0009","rev":1,"title":"Federated Search Latency Budgets",
                  "authors":"N. Ahmed","is_retracted":0,"changed_at":"2026-11-12T09:00:00Z"}]}' >/dev/null
req -X POST "$SOURCE/control/page-delay" -H 'Content-Type: application/json' \
  -d '{"seconds":5}' >/dev/null

JOB4=$(req -X POST "$COLLECTOR/sync/jobs" -H 'Content-Type: application/json' \
         -d '{"end":"2026-11-12T23:59:59Z"}')
LATEST=$(echo "$JOB4" | jq -r .id)
ORIGINAL_START=$(echo "$JOB4" | jq -r .interval_start)
ORIGINAL_END=$(echo "$JOB4" | jq -r .interval_end)
echo "$JOB4" | jq '{id, status, interval_start, interval_end}'

# Wait until the worker has actually begun collecting (status flips promptly;
# the page itself is still in flight for ~2s).
sleep 1
BEFORE_RESTART=$(req "$COLLECTOR/sync/jobs/$LATEST" | jq '{status, pages_fetched}')
echo "  before restart: $BEFORE_RESTART"

say "Restarting the collector container (SQLite lives on a named volume)"
(cd "$REPO_ROOT" && docker compose restart collector >/dev/null)
wait_for "$COLLECTOR/health"

say "After restart the in-flight job is explicitly interrupted, same id/interval"
RESTARTED=$(req "$COLLECTOR/sync/jobs/$LATEST")
echo "$RESTARTED" | jq '{id, status, error, interval_start, interval_end, pages_fetched}'
[ "$(echo "$RESTARTED" | jq -r .status)" = "interrupted" ] \
  || { echo "expected interrupted after restart" >&2; exit 1; }
[ "$(echo "$RESTARTED" | jq -r .interval_start)" = "$ORIGINAL_START" ] \
  && [ "$(echo "$RESTARTED" | jq -r .interval_end)" = "$ORIGINAL_END" ] \
  || { echo "interval changed across restart" >&2; exit 1; }
echo "  OK: job $LATEST is resumable with [start,end] unchanged"

say "Clear the slow page and resume by the SAME job id"
req -X POST "$SOURCE/control/page-delay" -H 'Content-Type: application/json' \
  -d '{"seconds":0}' >/dev/null
req -X POST "$COLLECTOR/sync/jobs/$LATEST/resume" >/dev/null
wait_status "$LATEST" completed failed || true
JOB4=$(req "$COLLECTOR/sync/jobs/$LATEST")
echo "$JOB4" | jq '{id, status, pages_fetched, interval_start, interval_end}'
[ "$(echo "$JOB4" | jq -r .status)" = "completed" ] \
  || { echo "resume after restart did not complete" >&2; exit 1; }
req "$COLLECTOR/papers/P0009" | jq -r '"  P0009 published: rev=\(.rev) \(.title)"'

# ---------------------------------------------------------------------------
hr "5. Final library state"
# ---------------------------------------------------------------------------
req "$COLLECTOR/sync/state" | jq .
say "Visible (non-retracted) papers:"
req "$COLLECTOR/papers" | jq -r '.[] | "  \(.paper_id) rev=\(.rev)  \(.title)"'
say "All papers including retracted:"
req "$COLLECTOR/papers?include_retracted=true" \
  | jq -r '.[] | "  \(.paper_id) rev=\(.rev) retracted=\(.is_retracted)  \(.title)"'
say "Job history:"
req "$COLLECTOR/sync/jobs" | jq -r '.[] | "  job \(.id): \(.status) pages=\(.pages_fetched) resets=\(.cursor_resets) [\(.interval_start), \(.interval_end)]"'

hr "Demo complete"
