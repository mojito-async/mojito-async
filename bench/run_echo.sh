#!/bin/sh
# bench/run_echo.sh — A7.8 (issue #82): AOT-build + run the high-
# concurrency echo benchmark (bench/echo_aot.mojo).
#
# The bench is a *_aot driver: it imports the reactor/net vendor externs
# (dylib mjs_poller_*/mjs_socket_* symbols) transitively, so it MUST be
# `mojo build` + execute (modular/modular#6971 — the b2 JIT cannot resolve
# dylib symbols through an imported module). Wired into precommit/run-
# suite.sh (the `suite` gate) and `make test` via this runner, same
# discipline as bench/run.sh (A2.8) / bench/run_timer_scale.sh (A6.5).
#
# Build flag: -O 0 — same default-optimization compiler crash as every
# other driver compiled against the full reactor dependency graph in one
# `main()` (t39_reactor_aot/t40_io_token_aot/t45_reactor_race_aot etc.,
# documented in mojito_async/test/run.sh's AOT_O0_DRIVERS header).
#
# Like bench/run_timer_scale.sh's timer-scale bench (and UNLIKE bench/
# run.sh's scheduler-scale bench, which needs H4 repeatability because it
# gates on wall-clock throughput): the echo bench's PASS/RED verdict is a
# pure STRUCTURAL validation (byte-exact echoes, zero fd/registration
# leak, N_CONN connections all genuinely concurrent, a positive park-event
# count proving the reactor path was actually exercised) — deterministic
# regardless of host noise, so a single run is the honest gate. The
# reported echoes/s and latency percentiles are ALWAYS a report artifact,
# never a pass/fail threshold (matches bench/run_timer_scale.sh's ns/op
# convention).
#
# Verdicts (same discipline as mojito_async/test/run.sh):
#   PASS   exit 0 + "bench_echo: PASS"
#   RED    exit 1 + "bench_echo: RED" — intentional TDD-red (allow-listed
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
name="echo_aot"
bin="$BUILD_DIR/$name"
if ! "$MOJO" build -O 0 -I "$REPO_ROOT" "$SCRIPT_DIR/$name.mojo" -o "$bin" \
        $LINK_FLAGS >"$BUILD_DIR/$name.build.log" 2>&1; then
    printf 'VERDICT\t%s\tFAIL\n' "bench_echo"
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
if [ "$st" -eq 0 ] && printf '%s' "$out" | grep -q "bench_echo: PASS"; then
    printf 'VERDICT\t%s\tPASS\n' "bench_echo"
    printf 'bench: PASS (wall=%ds)\n' "$dur"
    exit 0
elif printf '%s' "$out" | grep -q "bench_echo: RED"; then
    printf 'VERDICT\t%s\tRED\n' "bench_echo"
    printf 'bench: RED (wall=%ds)\n' "$dur"
    exit 1
fi
printf 'VERDICT\t%s\tFAIL\n' "bench_echo"
printf 'bench: FAIL (exit %s, wall=%ds)\n' "$st" "$dur"
exit 1
