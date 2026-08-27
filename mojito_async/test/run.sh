#!/bin/sh
# mojito-async A1.x — acceptance harness (A1.1 runtime issue #33; A1.3 channel
# issue #35; later lanes add tNN_* drivers that this glob picks up).
# Runs every tNN_*.mojo driver under test/unit/ and prints a PASS/RED matrix.
# Verdict: PASS=exit0+prints PASS; RED=prints RED; FAIL otherwise.
set -u
SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)
MOJO=${MOJO:-mojo}
TESTS=$(ls "$SCRIPT_DIR"/unit/t[0-9][0-9]_*.mojo 2>/dev/null || true)
[ -z "$TESTS" ] && echo "ERROR: no acceptance tests under $SCRIPT_DIR/unit" && exit 2
command -v "$MOJO" >/dev/null 2>&1 || { echo "ERROR: mojo not found"; exit 2; }
failures=0; matrix=""
for t in $TESTS; do
  name=$(basename "$t" .mojo)
  out=$("$MOJO" run -I "$REPO_ROOT" "$t" 2>&1); st=$?
  if [ "$st" -eq 0 ] && printf '%s' "$out" | grep -q "PASS"; then row="$name PASS"
  else row="$name FAIL (exit $st)"; failures=$((failures+1)); fi
  matrix="$matrix$row"
  echo "== $name"; printf '%s\n' "$out" | tail -n 2 | sed 's/^/   | /'
done
echo ""
echo "mojito-async acceptance test matrix (A1.1 runtime + A1.3 channel)"
printf '%b' "$matrix" | sed 's/^/  /'
echo ""
[ "$failures" -ne 0 ] && { echo "RESULT: $failures FAILURE(S)"; exit 1; }
echo "RESULT: all green"; exit 0