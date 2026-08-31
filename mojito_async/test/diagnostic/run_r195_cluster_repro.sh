#!/bin/sh
# mojito_async/test/diagnostic/run_r195_cluster_repro.sh
#
# Repeat-invocation wrapper for r195_cluster_visibility_repro.mojo (issue
# #195, round 6).  Sibling of round 5's run_r195_repro.sh — same shape,
# pointed at the round-6 "fatter" binary instead.  Builds ONCE at the
# DEFAULT optimization level (never -O 0) and runs the resulting binary
# REPEAT times, aggregating the per-run visibility-miss counts.
#
# This is NOT wired into mojito_async/test/run.sh or precommit/gate.sh —
# standalone diagnostic, nothing here for the gate to regress-test.
# Invoke by hand:
#
#   mojito_async/test/diagnostic/run_r195_cluster_repro.sh
#
# Env:
#   REPEAT=<n>          process invocations (default 60)
#   RUN_TIMEOUT=<secs>  per-run bound (default 45 -- this driver does more
#                       work per round than round 5's, so its default
#                       ROUNDS takes a bit longer per run)
#   LOAD=<n>            spawn n CPU hogs for the duration (default 4,
#                       matching round 5's default-on contention).
set -u

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/../../.." && pwd)
BUILD_DIR="$REPO_ROOT/build"
MOJO=${MOJO:-mojo}
REPEAT=${REPEAT:-60}
RUN_TIMEOUT=${RUN_TIMEOUT:-45}
LOAD=${LOAD:-4}

command -v "$MOJO" >/dev/null 2>&1 || { echo "ERROR: mojo not found"; exit 2; }

TIMEOUT_BIN=""
for cand in timeout gtimeout; do
    if command -v "$cand" >/dev/null 2>&1; then TIMEOUT_BIN=$cand; break; fi
done
if [ -z "$TIMEOUT_BIN" ]; then
    echo "ERROR: neither timeout nor gtimeout on PATH; this lane must be able"
    echo "       to bound a run (a driver-internal pacing bug would hang)."
    exit 2
fi

mkdir -p "$BUILD_DIR" || true
SRC="$SCRIPT_DIR/r195_cluster_visibility_repro.mojo"
BIN="$BUILD_DIR/r195_cluster_visibility_repro"

echo "R195 CLUSTER visibility repro batch (issue #195, round 6)"
echo "  repeat=$REPEAT, per-run bound=${RUN_TIMEOUT}s, background load=$LOAD hog(s)"
echo "  built at the DEFAULT optimization level (not -O 0)."
echo ""

if ! "$MOJO" build "$SRC" -o "$BIN" -I "$REPO_ROOT" \
        > "$BUILD_DIR/r195_cluster_repro.build.log" 2>&1; then
    echo "BUILD FAILED — see $BUILD_DIR/r195_cluster_repro.build.log"
    tail -n 20 "$BUILD_DIR/r195_cluster_repro.build.log" | sed 's/^/  | /'
    exit 2
fi

load_pids=""
i=0
while [ "$i" -lt "$LOAD" ]; do
    sh -c 'while :; do :; done' &
    load_pids="$load_pids $!"
    i=$((i + 1))
done
stop_load() {
    for p in $load_pids; do kill "$p" 2>/dev/null || true; done
}
trap stop_load EXIT INT TERM

npass=0
nhang=0
ncrash=0
nrepro=0
nstaterepro=0
total_rounds=0
total_miss=0
total_state_miss=0
total_readback_fail=0
first_repro_detail=""

r=1
while [ "$r" -le "$REPEAT" ]; do
    out=$("$TIMEOUT_BIN" "$RUN_TIMEOUT" "$BIN" 2>&1)
    st=$?
    if [ "$st" -eq 124 ] || [ "$st" -eq 137 ]; then
        nhang=$((nhang + 1))
        printf '  run %3d: HANG (no verdict within %ss)\n' "$r" "$RUN_TIMEOUT"
    elif [ "$st" -ne 0 ] && [ "$st" -ne 2 ] && ! printf '%s' "$out" | grep -q "REPRODUCED\|no visibility miss"; then
        ncrash=$((ncrash + 1))
        printf '  run %3d: CRASH (exit %s): %s\n' "$r" "$st" "$(printf '%s' "$out" | tr '\n' ' ' | cut -c1-160)"
    else
        line=$(printf '%s' "$out" | grep "rounds_done=")
        rounds=$(printf '%s' "$line" | sed -n 's/.*rounds_done=\([0-9]*\).*/\1/p')
        miss=$(printf '%s' "$line" | sed -n 's/.*next_visibility_fail=\([0-9]*\).*/\1/p')
        smiss=$(printf '%s' "$line" | sed -n 's/.*state_visibility_fail=\([0-9]*\).*/\1/p')
        rbfail=$(printf '%s' "$line" | sed -n 's/.*readback_fail=\([0-9]*\).*/\1/p')
        [ -n "$rounds" ] && total_rounds=$((total_rounds + rounds))
        [ -n "$miss" ] && total_miss=$((total_miss + miss))
        [ -n "$smiss" ] && total_state_miss=$((total_state_miss + smiss))
        [ -n "$rbfail" ] && total_readback_fail=$((total_readback_fail + rbfail))
        if [ "${miss:-0}" -gt 0 ]; then
            nrepro=$((nrepro + 1))
            printf '  run %3d: REPRODUCED, %s next-miss(es) / %s rounds\n' "$r" "$miss" "$rounds"
            if [ -z "$first_repro_detail" ]; then
                first_repro_detail=$(printf '%s' "$out" | grep "first miss:")
            fi
        else
            npass=$((npass + 1))
        fi
        if [ "${smiss:-0}" -gt 0 ]; then
            nstaterepro=$((nstaterepro + 1))
            printf '  run %3d: SECONDARY state-miss, %s state-miss(es) / %s rounds\n' "$r" "$smiss" "$rounds"
        fi
    fi
    r=$((r + 1))
done

stop_load
trap - EXIT INT TERM

echo ""
echo "  --- summary ---"
printf '  runs: %d total, %d clean, %d REPRODUCED (next), %d hang, %d crash\n' \
    "$REPEAT" "$npass" "$nrepro" "$nhang" "$ncrash"
printf '  aggregate: %d rounds, %d next-visibility miss(es), %d state-visibility miss(es), %d same-thread readback fail(s)\n' \
    "$total_rounds" "$total_miss" "$total_state_miss" "$total_readback_fail"
printf '  runs with a secondary state-visibility miss: %d\n' "$nstaterepro"
if [ -n "$first_repro_detail" ]; then
    echo "  $first_repro_detail"
fi

if [ "$nrepro" -gt 0 ]; then
    echo ""
    echo "  VERDICT: REPRODUCED — the FATTER repro (full real field-cluster"
    echo "  touch sequence around the same 4 crossings) missed a _next write"
    echo "  that round 5's single-field repro never missed across 560M"
    echo "  attempts.  This is evidence FOR candidate 6 option 1 (the field"
    echo "  cluster / register pressure / clustered-stores shape is a"
    echo "  necessary ingredient)."
    exit 1
fi
if [ "$nhang" -gt 0 ]; then
    echo ""
    echo "  VERDICT: no visibility miss, but $nhang run(s) HUNG — that is this"
    echo "  driver's OWN pacing handshake, investigate before trusting a clean"
    echo "  result from this batch."
    exit 1
fi
echo ""
echo "  VERDICT: NOT reproduced across $total_rounds total rounds ($REPEAT"
echo "  process invocations), even with the full field cluster touched"
echo "  around every crossing."
exit 0
