#!/bin/sh
# mojito-async A0 spike — colorless-runtime semantic test harness (A0.2, #11).
#
# Runs every test driver in spike/colorless_runtime/tests/ and prints a
# PASS/FAIL/RED matrix.  Drivers are:
#   - tN_*.mojo entry points, compiled/run via `mojo run -Xlinker <dylib>
#     -I spike/colorless_runtime -I spike/colorless_runtime/vendor/mojito-sys`.
#   - standalone C tests (tN_main.c-style entry, if a later lane adds one),
#     compiled against the same dylib and run.
#
# Verdict classes:
#   PASS  - driver printed PASS
#   RED   - driver printed RED (intentional TDD-red, allow-listed via the
#           precommit/known-red.tsv `suite` row); not an unexpected failure
#   FAIL  - driver printed FAIL, crashed, or produced no verdict
#   ERROR - harness-level environment problem (missing toolchain / dylib),
#           reported and NOT attributed to individual tests.
#
# Exits nonzero while any test is not green (FAIL or still-red), so the
# pre-commit gate sees the suite as not-yet-green and allow-lists it via the
# known-red `suite` row until A0.4/A0.6 land. Exits 2 for environment trouble.
#
# A missing dylib is an environment problem, not a test FAIL: it means the
# vendor substrate (A0.1) / dylib build has not landed yet.
#
# Usage: spike/colorless_runtime/tests/run.sh   (or `make test` at repo root)
#   MOJO=/path/to/mojo overrides the compiler.
#   CC=<compiler> defaults to cc.

set -u

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/../../.." && pwd)
SPIKE_DIR="$REPO_ROOT/spike/colorless_runtime"
BINDING_DIR="$SPIKE_DIR/vendor/mojito-sys"
DYLIB="$REPO_ROOT/libmojito_spike.dylib"
BUILD_DIR="$REPO_ROOT/build"

MOJO=${MOJO:-mojo}
CC=${CC:-cc}

MOJO_TESTS=""
if [ -d "$SCRIPT_DIR" ]; then
    MOJO_TESTS=$(ls "$SCRIPT_DIR"/t*_*.mojo 2>/dev/null | sort)
fi
C_TESTS=$(ls "$SCRIPT_DIR"/t*_*_main.c 2>/dev/null || true)

if [ -z "$MOJO_TESTS" ] && [ -z "$C_TESTS" ]; then
    echo "ERROR: no test drivers under $SCRIPT_DIR (no t*_*.mojo, no tN_*_main.c)"
    exit 2
fi

if ! command -v "$MOJO" >/dev/null 2>&1; then
    echo "ERROR: mojo toolchain not found on PATH; set MOJO=<path-to-mojo>"
    exit 2
fi

if [ ! -f "$DYLIB" ]; then
    echo "ERROR: $DYLIB not found; run \`make\` at the repo root first (vendor substrate or build missing)."
    echo "       Tests are NOT run: this is an environment problem, not a test result."
    exit 2
fi

failures=0
reds=0
matrix=""

# --- .mojo drivers ------------------------------------------------------------
for t in $MOJO_TESTS; do
    name=$(basename "$t" .mojo)
    out=$("$MOJO" run -Xlinker "$DYLIB" -I "$SPIKE_DIR" -I "$BINDING_DIR" "$t" 2>&1)
    st=$?
    if [ "$st" -eq 0 ] && printf '%s' "$out" | grep -q "PASS"; then
        row="$name PASS"
    elif printf '%s' "$out" | grep -q "RED"; then
        # RED requires exit 1; exit 0 with RED text is a driver bug (FAIL).
        if [ "$st" -eq 1 ]; then
            row="$name RED (known-red, TDD)"
            reds=$((reds + 1))
        else
            row="$name FAIL (RED text but exit $st)"
            failures=$((failures + 1))
        fi
    else
        row="$name FAIL (exit $st; no PASS/RED verdict)"
        failures=$((failures + 1))
    fi
    matrix="$matrix$row
"
    echo "== $name"
    printf '%s\n' "$out" | tail -n 6 | sed 's/^/   | /'
done

# --- standalone C test drivers --------------------------------------------------
mkdir -p "$BUILD_DIR" || true
for t in $C_TESTS; do
    name=$(basename "$t" .c)
    obj="$BUILD_DIR/${name}_harness"
    if ! $CC -O2 -g -Wall -Wextra -I"$BINDING_DIR/include" "$t" \
            -L"$REPO_ROOT" -lmojito_spike -o "$obj" 2>&1; then
        row="$name FAIL (C build error)"
        failures=$((failures + 1))
    else
        out=$("$obj" 2>&1)
        if printf '%s' "$out" | grep -q "FAIL"; then
            row="$name FAIL"
            failures=$((failures + 1))
        elif printf '%s' "$out" | grep -q "RED"; then
            row="$name RED (known-red, TDD)"
            reds=$((reds + 1))
        elif printf '%s' "$out" | grep -q "PASS"; then
            row="$name PASS"
        else
            row="$name FAIL (no verdict)"
            failures=$((failures + 1))
        fi
    fi
    matrix="$matrix$row
"
    echo "== $name"
    if [ -f "$obj" ]; then
        printf '%s\n' "$out" | tail -n 4 | sed 's/^/   | /'
    fi
done

echo ""
echo "mojito-async A0 colorless-runtime test matrix (issue #11)"
printf '%s' "$matrix" | sed 's/^/  /'
echo ""
if [ "$failures" -ne 0 ]; then
    echo "RESULT: $failures unexpected FAILURE(S)"
    exit 1
fi
if [ "$reds" -ne 0 ]; then
    echo "RESULT: $reds test(s) RED (known-red / TDD — expected until A0.4/A0.6 land)"
    exit 1
fi
echo "RESULT: all green"
exit 0