#!/bin/sh
# mojito-async A1.1+A1.5 acceptance harness (issues #33, #37).
#
# Runs:
#   - the A1.1 runtime unit drivers in test/unit/ (t11_*.mojo..t18_*.mojo);
#   - the A1.5 stress suites in test/stress/ (t*_stress.mojo, issue #37);
#   - the AOT stress driver test/stress/t*_aot.mojo (`mojo build` + execute:
#     the 100k-lifecycle suite needs local libc externs — getrusage/malloc —
#     kept in the *_aot driver per modular/modular#6971).
#
# Verdicts per driver: PASS = exit 0 + "PASS"; RED = exit 1 + "RED"
# (intentional TDD-red; allow-listed at gate level via precommit/known-red.tsv
# row `suite`); everything else FAIL.  Exits nonzero while any driver is not
# green, so the pre-commit gate sees the suite as not-yet-green.
set -u
SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)
BUILD_DIR="$REPO_ROOT/build"
MOJO=${MOJO:-mojo}
command -v "$MOJO" >/dev/null 2>&1 || { echo "ERROR: mojo not found"; exit 2; }

UNIT_TESTS=$(ls "$SCRIPT_DIR"/unit/t1[1-8]_*.mojo 2>/dev/null || true)
# _aot.mojo drivers are excluded here; they are built+run in the AOT loop.
STRESS_TESTS=$(ls "$SCRIPT_DIR"/stress/t*_*.mojo 2>/dev/null | grep -v "_aot\.mojo$" | sort || true)
AOT_TESTS=$(ls "$SCRIPT_DIR"/stress/t*_aot.mojo 2>/dev/null || true)
if [ -z "$UNIT_TESTS" ] && [ -z "$STRESS_TESTS" ]; then
    echo "ERROR: no tests under $SCRIPT_DIR/unit or $SCRIPT_DIR/stress"
    exit 2
fi

failures=0; reds=0; matrix=""

run_one() { # <name> <out> <exit>
    name=$1; out=$2; st=$3
    if [ "$st" -eq 0 ] && printf '%s' "$out" | grep -q "PASS"; then
        row="$name PASS"
    elif printf '%s' "$out" | grep -q "RED"; then
        if [ "$st" -eq 1 ]; then row="$name RED (known-red, TDD)"; reds=$((reds+1))
        else row="$name FAIL (RED text but exit $st)"; failures=$((failures+1)); fi
    else
        row="$name FAIL (exit $st; no PASS/RED verdict)"
        failures=$((failures+1))
    fi
    matrix="$matrix$row"
    echo "== $name"; printf '%s\n' "$out" | tail -n 2 | sed 's/^/   | /'
}

# --- A1.1 unit drivers -------------------------------------------------------
for t in $UNIT_TESTS; do
    name=$(basename "$t" .mojo)
    out=$("$MOJO" run -I "$REPO_ROOT" "$t" 2>&1); st=$?
    run_one "$name" "$out" "$st"
done

# --- A1.5 stress drivers (JIT) -----------------------------------------------
for t in $STRESS_TESTS; do
    name=$(basename "$t" .mojo)
    out=$("$MOJO" run -I "$REPO_ROOT" "$t" 2>&1); st=$?
    run_one "$name" "$out" "$st"
done

# --- A1.5 stress AOT driver (getrusage/malloc externs stay in-driver) --------
mkdir -p "$BUILD_DIR" || true
for t in $AOT_TESTS; do
    name=$(basename "$t" .mojo)
    bin="$BUILD_DIR/$name"
    if ! "$MOJO" build "$t" -o "$bin" -I "$REPO_ROOT" \
            >"$BUILD_DIR/$name.build.log" 2>&1; then
        row="$name FAIL (AOT build error)"
        failures=$((failures+1))
        matrix="$matrix$row"
        echo "== $name"; tail -n 3 "$BUILD_DIR/$name.build.log" | sed 's/^/   | /'
    else
        out=$("$bin" 2>&1); st=$?
        run_one "$name" "$out" "$st"
    fi
done

echo ""
echo "mojito-async A1.1 runtime + A1.5 stress test matrix (issues #33, #37)"
printf '%b' "$matrix" | sed 's/^/  /'
echo ""
[ "$failures" -ne 0 ] && { echo "RESULT: $failures FAILURE(S)"; exit 1; }
[ "$reds" -ne 0 ] && { echo "RESULT: $reds RED (intentional TDD-red)"; exit 1; }
echo "RESULT: all green"; exit 0