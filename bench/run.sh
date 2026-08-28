#!/bin/sh
# bench/run.sh — A2.8 (issue #74): AOT-build + run the scheduler-scale bench.
#
# The scheduler-scale bench (scheduler_scale_aot.mojo) is a *_aot driver: it
# embeds the pool trampoline (thread_entry EMBEDDING RULE) and layer-local
# externs, so it MUST be `mojo build` + execute (modular/modular#6971 — the
# JIT cannot resolve @export symbols / imported-module externs).  It is
# wired into precommit/run-suite.sh (the `suite` gate) and `make test` via
# this runner.
#
# Build flag: -O 0.  b2 1.0.0b2 crashes the O3 optimizer nondeterministically
# on this driver (probed: same source, O0 builds and runs deterministically);
# every config inside the bench is built and run identically, so the relative
# baseline-vs-experiment numbers remain the §78.2 evidence.
#
# H4 REPEATABILITY (review fold, issue #74): the binary is built once and
# executed REPEAT_RUNS times (default 3; override REPEAT_RUNS=<n>).  Every
# run must independently reach the PASS verdict (exit 0 + the final PASS
# line) — a single RED/FAIL run fails the gate, so a flaky verdict cannot
# hide behind one lucky run.  The per-run wall time is logged (the stability
# log) with a min/median/max/spread summary.  The wall spread is REPORTED,
# NOT GATED: on a shared/contended host wall time is not a stable
# measurement, and §78.4's discipline is "report with statistics, don't gate
# on an unquiet host".  The honest gates stay inside the bench where they
# belong (the phase-1 throughput-ratio gate, the wake-stress/no-migration/
# idle assertions), each of which must pass on EVERY repetition.
#
# Verdicts (same discipline as mojito_async/test/run.sh):
#   PASS   exit 0 + "PASS"
#   RED    exit 1 + "RED" — intentional TDD-red (allow-listed at gate level
#          via precommit/known-red.tsv row `suite`); REMOVE the row when green
#   FAIL   anything else
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
name="scheduler_scale_aot"
bin="$BUILD_DIR/$name"
if ! "$MOJO" build -O 0 -I "$REPO_ROOT" "$SCRIPT_DIR/$name.mojo" -o "$bin" \
        $LINK_FLAGS >"$BUILD_DIR/$name.build.log" 2>&1; then
    echo "bench: FAIL (AOT build error)"
    tail -n 5 "$BUILD_DIR/$name.build.log" | sed 's/^/   | /'
    exit 1
fi

# --- H4 repeatability section (N runs + stability log) ----------------------
# Every run must PASS independently; wall times feed the report-only
# stability log (min/median/max/spread).  Full output of the first PASS run
# is shown as the representative run; any non-PASS run prints its full
# output for diagnostics.
REPEAT_RUNS=${REPEAT_RUNS:-3}
runs=1
passed=0
mins=""; maxs=""; sum=0; num=0
while [ "$runs" -le "$REPEAT_RUNS" ]; do
    t0=$(date +%s)
    out=$("$bin" 2>&1); st=$?
    t1=$(date +%s)
    dur=$((t1 - t0))
    if [ "$st" -eq 0 ] && printf '%s' "$out" | grep -q "bench_scheduler_scale: PASS"; then
        verdict="PASS"
    elif printf '%s' "$out" | grep -q "bench_scheduler_scale: RED"; then
        verdict="RED"
    else
        verdict="FAIL"
    fi
    printf 'bench: stability run %d/%d: wall=%ds verdict=%s\n' "$runs" "$REPEAT_RUNS" "$dur" "$verdict"
    if [ "$verdict" = "PASS" ] && [ "$runs" -eq 1 ]; then
        printf '%s\n' "$out"
    elif [ "$verdict" != "PASS" ]; then
        printf '%s\n' "$out"
    fi
    if [ "$verdict" = "PASS" ]; then
        passed=$((passed + 1))
    fi
    if [ -z "$mins" ] || [ "$dur" -lt "$mins" ]; then mins=$dur; fi
    if [ -z "$maxs" ] || [ "$dur" -gt "$maxs" ]; then maxs=$dur; fi
    sum=$((sum + dur)); num=$((num + 1))
    runs=$((runs + 1))
done
if [ "$num" -gt 0 ]; then
    med=$((sum / num))
    spread=0
    if [ "$maxs" -gt 0 ]; then
        spread=$(( (maxs - mins) * 100 / maxs ))
    fi
    printf 'bench: stability log: runs=%d min=%ds median=%ds max=%ds wall-spread=%d%% (report-only; gates are inside the bench)\n' \
        "$num" "$mins" "$med" "$maxs" "$spread"
fi

if [ "$passed" -eq "$REPEAT_RUNS" ]; then
    echo "bench: PASS"
    exit 0
fi
echo "bench: FAIL (exit $st; $passed/$REPEAT_RUNS runs passed; verdict must hold on EVERY repetition)"
exit 1