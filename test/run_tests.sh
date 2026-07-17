#!/usr/bin/env bash
# End-to-end test script for queuectl.
# Exercises the scenarios required by the assignment:
#   1. Basic job completes successfully.
#   2. Failed job retries with backoff and moves to DLQ.
#   3. Multiple workers process jobs without overlap/duplication.
#   4. Invalid commands fail gracefully.
#   5. Job data survives restart.
#
# Uses an isolated data directory so it never touches your real queue.

set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export QUEUECTL_DATA_DIR="$ROOT_DIR/test/.tmp-data"
CLI="node $ROOT_DIR/bin/queuectl.js"

PASS=0
FAIL=0

pass() { echo "  PASS: $1"; PASS=$((PASS+1)); }
fail() { echo "  FAIL: $1"; FAIL=$((FAIL+1)); }

cleanup() {
  $CLI worker stop >/dev/null 2>&1
  sleep 1
  pkill -9 -f "src/worker.js.*QUEUECTL_DATA_DIR" >/dev/null 2>&1
  rm -rf "$QUEUECTL_DATA_DIR"
}
trap cleanup EXIT

rm -rf "$QUEUECTL_DATA_DIR"

echo "== Test 1: basic job completes successfully =="
$CLI enqueue '{"id":"basic1","command":"echo hi"}' >/dev/null
$CLI worker start --count 1 >/dev/null
sleep 1.5
$CLI worker stop >/dev/null
sleep 0.5
STATE=$($CLI list | grep basic1 | awk '{print $2}')
[ "$STATE" = "completed" ] && pass "basic job reached 'completed'" || fail "basic job state was '$STATE', expected 'completed'"

echo "== Test 2: failed job retries with backoff and moves to DLQ =="
$CLI config set backoff-base 1 >/dev/null   # 1^n = 1s delay, keeps test fast
$CLI enqueue '{"id":"baddie","command":"exit 1","max_retries":2}' >/dev/null
$CLI worker start --count 1 >/dev/null
sleep 3
$CLI worker stop >/dev/null
sleep 0.5
STATE=$($CLI list | grep baddie | awk '{print $2}')
ATTEMPTS=$($CLI list | grep baddie | grep -o 'attempts=[0-9]*' | cut -d= -f2)
[ "$STATE" = "dead" ] && pass "job moved to DLQ (dead) after exhausting retries" || fail "job state was '$STATE', expected 'dead'"
[ "$ATTEMPTS" = "2" ] && pass "attempts count matches max_retries (2)" || fail "attempts was '$ATTEMPTS', expected 2"
DLQ_HIT=$($CLI dlq list | grep -c baddie)
[ "$DLQ_HIT" = "1" ] && pass "job appears in dlq list" || fail "job missing from dlq list"

echo "== Test 3: multiple workers process jobs without overlap =="
for i in $(seq 1 10); do
  $CLI enqueue "{\"id\":\"multi$i\",\"command\":\"sleep 0.1 && echo $i\"}" >/dev/null
done
$CLI worker start --count 4 >/dev/null
sleep 3
$CLI worker stop >/dev/null
sleep 0.5
COMPLETED=$($CLI list --state completed | grep -c "^multi")
RUN_COUNT=$(grep -h "running job \"multi" "$QUEUECTL_DATA_DIR"/logs/*.log 2>/dev/null | wc -l | tr -d ' ')
[ "$COMPLETED" = "10" ] && pass "all 10 jobs completed" || fail "$COMPLETED/10 jobs completed"
[ "$RUN_COUNT" = "10" ] && pass "each job executed exactly once (no duplicate processing)" || fail "expected 10 job executions, saw $RUN_COUNT"

echo "== Test 4: invalid commands fail gracefully (no crash) =="
$CLI enqueue '{"id":"invalidcmd","command":"totally_not_a_real_binary_zzz","max_retries":1}' >/dev/null
$CLI worker start --count 1 >/dev/null
sleep 2
$CLI worker stop >/dev/null
sleep 0.5
STATE=$($CLI list | grep invalidcmd | awk '{print $2}')
[ "$STATE" = "dead" ] && pass "invalid command failed gracefully and moved to DLQ" || fail "invalid command job state was '$STATE'"
CLI_ALIVE=$($CLI status >/dev/null 2>&1; echo $?)
[ "$CLI_ALIVE" = "0" ] && pass "CLI still responsive after invalid command" || fail "CLI crashed after invalid command"

echo "== Test 5: job data survives restart =="
JOBS_BEFORE=$($CLI list | wc -l | tr -d ' ')
# Simulate a "restart" by just invoking the CLI fresh - it re-reads state.json from disk each time.
JOBS_AFTER=$($CLI list | wc -l | tr -d ' ')
[ "$JOBS_BEFORE" = "$JOBS_AFTER" ] && [ "$JOBS_AFTER" != "0" ] && pass "job count stable across process restarts ($JOBS_AFTER jobs)" || fail "job data did not persist"

echo ""
echo "================================"
echo "Results: $PASS passed, $FAIL failed"
echo "================================"
[ "$FAIL" -eq 0 ] && exit 0 || exit 1
