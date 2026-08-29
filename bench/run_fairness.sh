#!/bin/sh
# bench/run_fairness.sh — A7.9 (issue #83): AOT-build + run the CPU-vs-IO
# scheduler fairness bench (bench/fairness_aot.mojo).
#
# The bench is a *_aot driver: it imports the reactor dependency graph
# (dylib mjs_poller_* symbols) transitively, so it MUST be `mojo build` +
# execute (modular/modular#6971). Wired into precommit/run-suite.sh (the
# `suite` gate) and `make test` via this runner, same discipline as
# bench/run.sh (A2.8) / bench/run_timer_scale.sh (A6.5) / bench/
# run_echo.sh (A7.8).
#
# Build flag: -O 0 — same default-optimization compiler crash as every
# other driver compiled against the full reactor dependency graph in one
# `main()` (documented in mojito_async/test/run.sh's AOT_O0_DRIVERS
# header).
#
# Like bench/run_echo.sh's echo bench: the PASS/RED verdict is a pure
# STRUCTURAL validation (every I/O fiber completes within a small,
# fixed CPU-slice bound — issue #83's "measured floor, not a strict
# starvation" gate) — deterministic regardless of host noise, so a
# single run is the honest gate. Wall-clock numbers are ALWAYS a report
# artifact, never a pass/fail threshold (the CPU class's own dispatch
# overhead dominates wall time, not I/O fairness — see the bench's own
# in-file report line).
#
# Verdicts (same discipline as mojito_async/test/run.sh):
#   PASS   exit 0 + "bench_fairness: PASS"
#   RED    exit 1 + "bench_fairness: RED" — intentional TDD-red
#          (allow-listed at gate level via precommit/known-red.tsv);
#          REMOVE the row when green
#   FAIL   anything else (build error, crash, missing PASS line)
set -u

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
BUILD_DIR="$REPO_ROOT/build"
MOJO=${MOJO:-mojo}
command -v "$MOJO" >/dev/null 2>&1 || { echo "ERROR: mojo not found"; exit 2; }

DYLIB="$REPO_ROOT/libmojito_spike.dylib"
LINK_FLAGS=""
if [ -f "$DYLIB" ]; then
    LINK_FLAGS="-Xlinker $DYLIB"
fi

mkdir -p "$BUILD_DIR" || true
name="fairness_aot"
bin="$BUILD_DIR/$name"
if ! "$MOJO" build -O 0 -I "$REPO_ROOT" "$SCRIPT_DIR/$name.mojo" -o "$bin" \
        $LINK_FLAGS >"$BUILD_DIR/$name.build.log" 2>&1; then
    echo "bench: FAIL (AOT build error)"
    tail -n 5 "$BUILD_DIR/$name.build.log" | sed 's/^/   | /'
    exit 1
fi

t0=$(date +%s)
out=$("$bin" 2>&1); st=$?
t1=$(date +%s)
dur=$((t1 - t0))
printf '%s\n' "$out"

if [ "$st" -eq 0 ] && printf '%s' "$out" | grep -q "bench_fairness: PASS"; then
    printf 'bench: PASS (wall=%ds)\n' "$dur"
    exit 0
elif printf '%s' "$out" | grep -q "bench_fairness: RED"; then
    printf 'bench: RED (wall=%ds)\n' "$dur"
    exit 1
fi
printf 'bench: FAIL (exit %s, wall=%ds)\n' "$st" "$dur"
exit 1
