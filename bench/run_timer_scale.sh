#!/bin/sh
# bench/run_timer_scale.sh — A6.5 (issue #88): AOT-build + run the F6
# timer-scale bench (bench/timer_scale_aot.mojo).
#
# The bench is a *_aot driver: its wall-clock reads (`clock_gettime`) are
# module-scope externs, so it MUST be `mojo build` + execute (modular/
# modular#6971 — the b2 JIT miscompiles imported-module externs).  It is
# wired into precommit/run-suite.sh (the `suite` gate) and `make test` via
# this runner, same discipline as bench/run.sh (A2.8).
#
# Build flag: -O 0, same rationale as bench/run.sh (b2 1.0.0b2's O3
# optimizer is nondeterministic on AOT driver builds on this host; O0 is the
# one config every case in the bench is built and run under).
#
# Unlike bench/run.sh's scheduler-scale bench (H4 repeatability, issue #74 —
# that bench measures WALL-CLOCK throughput on a possibly-contended host and
# needs N reps to show the verdict is not flaky), the timer-scale bench's
# PASS/RED verdict is a pure STRUCTURAL validation (ascending-deadline pop
# order, exact drain counts, live-generation bookkeeping) — deterministic
# regardless of host noise, so a single run is the honest gate; the reported
# ns/op numbers are ALWAYS a report artifact, never a pass/fail threshold
# (issue #88 step 7).
#
# Verdicts (same discipline as mojito_async/test/run.sh):
#   PASS   exit 0 + "timer_scale: PASS"
#   RED    exit 1 + "timer_scale: RED" — intentional TDD-red (allow-listed
#          at gate level via precommit/known-red.tsv); REMOVE the row when
#          green
#   FAIL   anything else (build error, crash, missing PASS line)
set -u

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
BUILD_DIR="$REPO_ROOT/build"
MOJO=${MOJO:-mojo}
command -v "$MOJO" >/dev/null 2>&1 || { echo "ERROR: mojo not found"; exit 2; }

# The substrate is a .dylib on Darwin and a .so elsewhere (issue #141:
# the Linux lanes must be able to run at all).
DYLIB="$REPO_ROOT/libmojito_spike.dylib"
[ -f "$DYLIB" ] || DYLIB="$REPO_ROOT/libmojito_spike.so"
LINK_FLAGS=""
if [ -f "$DYLIB" ]; then
    LINK_FLAGS="-Xlinker $DYLIB"
fi

mkdir -p "$BUILD_DIR" || true
name="timer_scale_aot"
bin="$BUILD_DIR/$name"
if ! "$MOJO" build -O 0 -I "$REPO_ROOT" "$SCRIPT_DIR/$name.mojo" -o "$bin" \
        $LINK_FLAGS >"$BUILD_DIR/$name.build.log" 2>&1; then
    printf 'VERDICT\t%s\tFAIL\n' "bench_timer_scale"
    echo "bench: FAIL (AOT build error)"
    tail -n 5 "$BUILD_DIR/$name.build.log" | sed 's/^/   | /'
    exit 1
fi

t0=$(date +%s)
out=$("$bin" 2>&1); st=$?
t1=$(date +%s)
dur=$((t1 - t0))
printf '%s\n' "$out"

# --- per-driver verdict row (issue #141) -----------------------------------
# precommit/gate.sh scores known-red allow-listing PER DRIVER; before #141 a
# single row named `suite` covered every driver and every bench at once.
if [ "$st" -eq 0 ] && printf '%s' "$out" | grep -q "timer_scale: PASS"; then
    printf 'VERDICT\t%s\tPASS\n' "bench_timer_scale"
    printf 'bench: PASS (wall=%ds)\n' "$dur"
    exit 0
elif printf '%s' "$out" | grep -q "timer_scale: RED"; then
    printf 'VERDICT\t%s\tRED\n' "bench_timer_scale"
    printf 'bench: RED (wall=%ds)\n' "$dur"
    exit 1
fi
printf 'VERDICT\t%s\tFAIL\n' "bench_timer_scale"
printf 'bench: FAIL (exit %s, wall=%ds)\n' "$st" "$dur"
exit 1
