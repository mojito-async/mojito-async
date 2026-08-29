#!/bin/sh
# mojito_async/test/t51_default_o_repeat.sh
#
# RED driver for issue #143 — the cross-worker safety properties this project
# claims are validated exclusively on unoptimized binaries.
#
# `mojito_async/test/run.sh` pins 21 of 38 AOT drivers to `-O 0`, including
# every driver that runs two real OS threads.  The comment block there
# documents why: at the default optimization level the compiler hoists the
# plain non-atomic `TaskControlBlock` state read out of the driver's spin
# loop, the driver never re-reads the other thread's write, and tasks get
# "permanently stuck".
#
# That pin has two causes and only one of them is upstream.  The `mojo build`
# optimizer CRASH is Modular's and is out of scope for EPIC #140.  Hoisting a
# plain non-atomic load out of a spin loop is not a crash and not a compiler
# bug: it is legal optimization of racy code, and the race is ours.  `_state`,
# `_generation`, `_early`, `_claim_epoch`, `_started` and `_owner_runtime` are
# plain `Int`/`Bool` (runtime/task_control_block.mojo:141-232) and foreign
# threads read them unguarded all over the tree.
#
# So what ships at default `-O` is not what the suite tests, and no document
# tells a downstream user that a default-`-O` build may lose wakeups.
#
# THIS SCRIPT builds the two drivers that exercise the real cross-worker
# handoff — t47_channel_cross_worker_aot and t38_mutex_cross_worker_aot — at
# the DEFAULT optimization level, deliberately NOT at `-O 0`, and runs each
# of them REPEAT times.  Both are in `AOT_O0_DRIVERS` today, which is exactly
# the thing under test; this lane is the one place they are built the way a
# downstream user would build them.  Neither driver's source is touched.
#
# Every run is bounded by `timeout`, because the failure mode IS a hang: a
# permanently-stuck task with no watchdog left to fire.  A timed-out run is
# reported as HANG, never left to wedge the suite.
#
# Green means 0 non-PASS runs across the whole matrix at default `-O`, which
# is what #143 asks for once `_state` is atomic and the LICM-class entries
# come off the pin list.
#
# Env:
#   REPEAT=<n>            runs per driver (default 30, the count in the issue)
#   RUN_TIMEOUT=<secs>    per-run bound (default 5; a healthy run is ~15ms)
#   LOAD=<n>              spawn n CPU hogs for the duration, to reproduce the
#                         contended-host condition PR #139 measured under
#
# Verdict: "PASS" + exit 0 when every run passes; "RED" + exit 1 otherwise.
set -u

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
REPO_ROOT=$(CDPATH= cd -- "$REPO_ROOT/.." && pwd)
BUILD_DIR="$REPO_ROOT/build"
MOJO=${MOJO:-mojo}
REPEAT=${REPEAT:-30}
RUN_TIMEOUT=${RUN_TIMEOUT:-5}
LOAD=${LOAD:-0}

command -v "$MOJO" >/dev/null 2>&1 || { echo "ERROR: mojo not found"; exit 2; }

# A hang is the defect's primary presentation, so a per-run bound is not
# optional.  Without one this lane cannot be in a gate at all.
TIMEOUT_BIN=""
for cand in timeout gtimeout; do
    if command -v "$cand" >/dev/null 2>&1; then TIMEOUT_BIN=$cand; break; fi
done
if [ -z "$TIMEOUT_BIN" ]; then
    echo "ERROR: neither timeout nor gtimeout on PATH; this lane must be able"
    echo "       to bound a run, because the failure being tested IS a hang."
    exit 2
fi

DYLIB="$REPO_ROOT/libmojito_spike.dylib"
[ -f "$DYLIB" ] || DYLIB="$REPO_ROOT/libmojito_spike.so"
LINK_FLAGS=""
[ -f "$DYLIB" ] && LINK_FLAGS="-Xlinker $DYLIB"

DRIVERS="t47_channel_cross_worker_aot t38_mutex_cross_worker_aot"

mkdir -p "$BUILD_DIR" || true

echo "T51 default-O repeatability (issue #143)"
echo "  repeat=$REPEAT per driver, per-run bound=${RUN_TIMEOUT}s, extra load=$LOAD"
echo "  drivers are built at the DEFAULT optimization level, NOT -O 0."
echo ""

# --- optional host load ----------------------------------------------------
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

total_bad=0
matrix=""

for d in $DRIVERS; do
    src="$SCRIPT_DIR/stress/$d.mojo"
    bin="$BUILD_DIR/${d}_defaultO"
    if [ ! -f "$src" ]; then
        echo "  $d: MISSING SOURCE $src"
        stop_load
        exit 2
    fi
    # shellcheck disable=SC2086
    if ! "$MOJO" build "$src" -o "$bin" -I "$REPO_ROOT" $LINK_FLAGS \
            > "$BUILD_DIR/${d}_defaultO.build.log" 2>&1; then
        # A build failure here is the OTHER, upstream cause of the -O 0 pin
        # (the `mojo build` optimizer crash, modular/modular#6971's sibling),
        # which EPIC #140 excludes.  Report it as an environment result, not
        # as this issue's red.
        echo "  $d: BUILD FAILED at default -O (upstream optimizer crash;"
        echo "      excluded from #140 — see the build log)"
        tail -n 3 "$BUILD_DIR/${d}_defaultO.build.log" | sed 's/^/        | /'
        stop_load
        exit 2
    fi

    npass=0; nred=0; nhang=0; ncrash=0
    first_detail=""
    r=1
    while [ "$r" -le "$REPEAT" ]; do
        out=$("$TIMEOUT_BIN" "$RUN_TIMEOUT" "$bin" 2>&1)
        st=$?
        if [ "$st" -eq 0 ] && printf '%s' "$out" | grep -q "PASS"; then
            npass=$((npass + 1))
        elif [ "$st" -eq 124 ] || [ "$st" -eq 137 ]; then
            nhang=$((nhang + 1))
            [ -z "$first_detail" ] && first_detail="run $r HANG (no verdict within ${RUN_TIMEOUT}s): $(printf '%s' "$out" | tr '\n' ' ' | cut -c1-160)"
        elif printf '%s' "$out" | grep -q "RED"; then
            nred=$((nred + 1))
            [ -z "$first_detail" ] && first_detail="run $r RED: $(printf '%s' "$out" | tr '\n' ' ' | cut -c1-160)"
        else
            ncrash=$((ncrash + 1))
            [ -z "$first_detail" ] && first_detail="run $r CRASH (exit $st): $(printf '%s' "$out" | tr '\n' ' ' | cut -c1-160)"
        fi
        r=$((r + 1))
    done

    bad=$((nred + nhang + ncrash))
    total_bad=$((total_bad + bad))
    printf '  %-34s %d/%d pass  (red=%d hang=%d crash=%d)\n' \
        "$d" "$npass" "$REPEAT" "$nred" "$nhang" "$ncrash"
    if [ -n "$first_detail" ]; then
        printf '      first failure: %s\n' "$first_detail"
    fi
    matrix="$matrix  $d $npass/$REPEAT
"
done

stop_load

echo ""
if [ "$total_bad" -eq 0 ]; then
    printf 'VERDICT\tt51_default_o_repeat\tPASS\n'
    echo "T51 default-O repeatability: PASS"
    exit 0
fi
echo "  The drivers pass at -O 0 and fail here. Nothing about the runtime"
echo "  changed between the two builds, only whether the compiler was allowed"
echo "  to hoist a plain non-atomic TaskControlBlock read out of a spin loop."
printf 'VERDICT\tt51_default_o_repeat\tRED\n'
echo "T51 default-O repeatability: RED ($total_bad failing run(s))"
exit 1
