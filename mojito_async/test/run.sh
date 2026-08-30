#!/bin/sh
# mojito-async A1 acceptance suite (runtime #33, sync #34, channel #35,
# timer #36, stress #37; stack cache #52).
#
# Runs:
#   - the A1 unit drivers in test/unit/ (t[0-9][0-9]_*.mojo — runtime
#     t11..t18, sync t21_mutex/t22_semaphore, channel t20..t22, timer
#     t19..t22; later A-lanes add tNN_* drivers that this glob picks up);
#   - the A1.5 stress suites in test/stress/ (t*_stress.mojo, issue #37);
#   - the AOT drivers (unit + stress, `mojo build` + execute): drivers that
#     import the fiber/stack seams (NativeStack, ms_* externs) or local
#     libc externs (getrusage/malloc) run ONLY AOT — the b2 JIT cannot
#     resolve dylib symbols through an imported module (modular/modular#6971).
#
# Linking: the mojito-sys dylib (libmojito_spike.dylib) is produced by the
# root Makefile from the vendored C/asm substrate.  When it exists we pass
# `-Xlinker <dylib>` to both `mojo run` and `mojo build` so seam drivers
# link; when it is absent the flags are omitted so the suite stays runnable
# pre-dylib.
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

# The substrate is a .dylib on Darwin and a .so elsewhere (issue #141:
# the Linux lanes must be able to run at all).
DYLIB="$REPO_ROOT/libmojito_spike.dylib"
[ -f "$DYLIB" ] || DYLIB="$REPO_ROOT/libmojito_spike.so"
LINK_FLAGS=""
if [ -f "$DYLIB" ]; then
    LINK_FLAGS="-Xlinker $DYLIB"
fi

# *_aot.mojo drivers are excluded from the JIT loops; they are built+run in
# the AOT loop below.
UNIT_TESTS=$(ls "$SCRIPT_DIR"/unit/t[0-9][0-9]_*.mojo 2>/dev/null | grep -v "_aot\.mojo$" || true)
STRESS_TESTS=$(ls "$SCRIPT_DIR"/stress/t*_*.mojo 2>/dev/null | grep -v "_aot\.mojo$" | sort || true)
# AOT drivers (unit + stress): `mojo build` + execute.
AOT_TESTS=$(ls "$SCRIPT_DIR"/unit/t*_aot.mojo "$SCRIPT_DIR"/stress/t*_aot.mojo 2>/dev/null | sort || true)
if [ -z "$UNIT_TESTS" ] && [ -z "$STRESS_TESTS" ] && [ -z "$AOT_TESTS" ]; then
    echo "ERROR: no tests under $SCRIPT_DIR/unit or $SCRIPT_DIR/stress"
    exit 2
fi

# A2.5 two-phase/affinity/duplicate drivers (t34/t34b/t34c — H4-partial and
# M10 of PR #109): their cross-thread handshake cells are PLAIN Ints
# published with release/acquire fences, so these drivers MUST be built at
# -O 0 — a higher optimization level can hoist the plain handshake reads and
# deadlock or desynchronize the driver.  A2.6 t35 (PR #110, issue #72): the
# wake-burst probe trips a 1.0.0b2 codegen SEGV at -O 3, so it also builds
# at -O 0 (the unoptimized build lowers the burst loop cleanly).  A5.1 (A5
# batch review, issue #89): t24_rendezvous_oneshot's large single-`main()`
# acceptance driver hangs the b2 compiler itself (not a runtime deadlock —
# `mojo build` never returns) at the default optimization level once the
# full A3+A4-grown dependency graph (Scope, the two-phase park kernel,
# RWLock/Barrier/Condvar) is compiled alongside it; `-O 0` compiles in
# seconds and the driver passes every scenario (RENAMED to `_aot.mojo` so
# it goes through this O0-capable loop instead of the plain JIT unit-test
# loop above, which has no optimization-level override).  A7.1/A7.2 (issue
# #75/#76): t39_reactor_aot and t40_io_token_aot's large single-`main()`
# drivers (real Reactor + real kqueue fds + a multi-iteration cancellation
# battery) hit the SAME default-optimization compiler crash as
# t24_rendezvous_oneshot above once compiled alongside the reactor
# package's own dependency graph; `-O 0` compiles cleanly and every
# scenario passes.  A7.1/A7.2 (issue #75/#76) also fold in
# t41_tcp_connect_aot/t42_tcp_accept_aot (issues #77/#78) hitting the same
# crash once TcpStream/TcpListener join the dependency graph.  A7.6 (issue
# #80): t42_io_cancel_deadline_aot hits the identical crash for the
# identical reason (real Reactor + real pipe fds compiled alongside
# reactor/cancel.mojo's own six-scenario battery); same `-O 0` fix.
# A7.5/A7.6 (issue #79/#80): t44_tcp_read_write_aot hits the SAME crash
# (real Reactor + a real TCP loopback pair + tcp_stream.mojo's own
# six-scenario battery incl. write_all/deadline/cancel/close paths); same
# `-O 0` fix.  A7.7 (issue #81): t45_reactor_race_aot hits the SAME crash
# for the same reason (real Reactor + the full runtime/park/timer
# dependency graph in one driver) — also built at `-O 0`.
#
# #112 (item 10, EPIC #2 review consensus): "rebuild t30/t33/t34/t35/t36/
# bench at -O 0 (b2 -O3 miscompiles cross-thread code)".  t34/t34b/t34c/
# t35 were already on this list; t30/t33/t36 join them here (all four are
# real 2+-OS-thread pool/steal/fairness scheduler drivers — the exact class
# the review flagged, even though none had individually tripped a
# miscompile yet).  bench/scheduler_scale_aot.mojo already builds at -O 0
# unconditionally (bench/run.sh's own hardcoded flag, H4 repeatability).
# t47_pool_scheduler_aot (issue #112 item 1) and t49_pool_churn_aot
# (item 5) are NEW drivers this same fold adds: both hit the IDENTICAL
# default-optimization compiler CRASH (not a runtime bug — `mojo build`
# itself segfaults) as t24/t39/t40/t41/t42/t44/t45 once compiled alongside
# their full WorkerPool/scheduler dependency graph — `-O 0` is REQUIRED
# for them to build at all, not merely a defensive choice.  Discovered the
# SAME way: this fold's growth of idle.mojo/thread_entry.mojo/worker.mojo/
# worker_pool.mojo/runtime.mojo pushed t41_idle_timer_wake_aot (issue #86)
# over the same default-optimization crash threshold (it built fine before
# this fold; a fresh `mojo build` segfault, not a runtime bug, confirmed
# `-O 0` fixes it) — every OTHER driver that imports the SAME modules
# keeps the default optimization level (they stayed under the threshold).
# Issue #128: t47_channel_cross_worker_aot hits the SAME class of bug
# t34/t34b/t34c/t35 document above (NOT the pool/scheduler-dependency-
# graph compiler crash the previous paragraph describes): its two REAL
# worker OS threads spin on `while not h.is_completed(): scheduler_loop(
# ...); sleep(...)` waiting for a cross-worker wake delivered via a plain
# (non-atomic) TaskControlBlock state field — at the default optimization
# level the compiler can hoist that plain read out of the loop (never
# re-reading the OTHER thread's write), producing a driver-side false
# "lost wakeup" (empirically: 100% reproducible within a handful of
# iterations at default -O, 0/30+ at `-O 0`) that is a MISCOMPILATION
# artifact, not a defect in Channel[T]'s guard/two-phase-park fix (issue
# #128) itself.  Every OTHER AOT driver keeps the default optimization
# level (the suite is NOT rebuilt at -O 0).
# Issue #138 (follow-up review of #112/#128): t38_mutex_cross_worker_aot
# (A4.1, issue #55) drives the SAME `while not h.is_completed():
# scheduler_loop(...); sleep(...)` outer spin over a plain (non-atomic)
# TaskControlBlock completion read across its two real worker OS threads
# as t47_channel_cross_worker_aot above — it was simply never folded into
# this list when t47 was.  #128's own sandbox observed the identical
# symptom class on it (~1-in-15-30 runs: a permanently-stuck WAITING task,
# SPIN_BUDGET watchdog trip, zero progress for the full spin window) at
# the default optimization level.  Local verification for this fold: the
# repro methodology was validated against t47 first (still on this list
# for the identical reason) — under real host contention (concurrent
# `mojo build`/CPU-load processes, load average ~3-7 on a 10-core host)
# t47 built at default -O failed 3/30 runs with genuine internal RED
# verdicts (not external timeouts); the SAME load level and run count
# produced 0/441 failures for t38 built at default -O on this particular
# host/toolchain build (Mojo 1.0.0b2, arm64) — this class of bug is a
# compiler LICM decision that is known to be sensitive to unrelated IR
# shape (Mutex[Int] call graph vs Channel[T]'s), so a clean local run does
# not clear the driver; it shares the EXACT vulnerable source pattern
# already fixed for t34/t34b/t34c/t35/t47 above, so it gets the same `-O 0`
# treatment defensively, matching the precedent already set for
# t30/t33/t36 (added to this list purely by risk-class membership, "even
# though none had individually tripped a miscompile" at addition time).
# t38 built at -O 0 stayed clean across the same 30-run moderate-load
# batch.
# Issue #145: t58_stack_registry_aot would rather be a DEFAULT-`-O` driver
# (its out-slot-origin scenario is only meaningful there), but `mojo build`
# crashes on it at default -O — the upstream optimizer crash #140 excludes.
AOT_O0_DRIVERS="t58_stack_registry_aot t30_worker_pool_aot t33_steal_aot t34_two_phase_aot t34b_affinity_aot t34c_duplicate_wake_aot t35_idle_sleep_aot t36_fairness_aot t41_idle_timer_wake_aot t24_rendezvous_oneshot_aot t39_reactor_aot t40_io_token_aot t41_tcp_connect_aot t42_tcp_accept_aot t42_io_cancel_deadline_aot t44_tcp_read_write_aot t45_reactor_race_aot t46_reactor_fairness_aot t47_pool_scheduler_aot t49_pool_churn_aot t47_channel_cross_worker_aot t38_mutex_cross_worker_aot"

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
    matrix="$matrix$row
"
    echo "== $name"; printf '%s\n' "$out" | tail -n 2 | sed 's/^/   | /'
}

# --- A1 unit drivers -----------------------------------------------------------
for t in $UNIT_TESTS; do
    name=$(basename "$t" .mojo)
    out=$("$MOJO" run -I "$REPO_ROOT" $LINK_FLAGS "$t" 2>&1); st=$?
    run_one "$name" "$out" "$st"
done

# --- A1.5 stress drivers (JIT) --------------------------------------------------
for t in $STRESS_TESTS; do
    name=$(basename "$t" .mojo)
    out=$("$MOJO" run -I "$REPO_ROOT" $LINK_FLAGS "$t" 2>&1); st=$?
    run_one "$name" "$out" "$st"
done

# --- AOT drivers (unit + stress): fiber/stack-seam + libc externs ---------------
# `mojo build` + execute; link the dylib for the seam externs (modular/
# modular#6971: JIT cannot resolve dylib symbols through an imported module).
mkdir -p "$BUILD_DIR" || true
for t in $AOT_TESTS; do
    name=$(basename "$t" .mojo)
    bin="$BUILD_DIR/$name"
    o0=""
    for d in $AOT_O0_DRIVERS; do
        [ "$d" = "$name" ] && o0="-O 0"
    done
    # shellcheck disable=SC2086  # o0 expands to nothing or "-O 0"
    if ! "$MOJO" build "$t" -o "$bin" -I "$REPO_ROOT" $LINK_FLAGS $o0 \
            >"$BUILD_DIR/$name.build.log" 2>&1; then
        row="$name FAIL (AOT build error)"
        failures=$((failures+1))
        matrix="$matrix$row
"
        echo "== $name"; tail -n 3 "$BUILD_DIR/$name.build.log" | sed 's/^/   | /'
    else
        out=$("$bin" 2>&1); st=$?
        run_one "$name" "$out" "$st"
    fi
done

echo ""
echo "mojito-async A1 acceptance matrix (runtime #33, sync #34, channel #35, timer #36, stress #37, stack cache #52)"
printf '%b' "$matrix" | sed 's/^/  /'
echo ""

# --- per-driver verdict rows (issue #141) ----------------------------------
# One machine-readable line per driver for precommit/gate.sh, which scores
# known-red allow-listing PER DRIVER.  Before #141 the gate saw a single
# check named `suite`, so one allow-list row covered every driver and every
# bench in the tree at once.
printf '%b' "$matrix" | awk 'NF>=2 {print "VERDICT\t" $1 "\t" $2}'
echo ""
[ "$failures" -ne 0 ] && { echo "RESULT: $failures FAILURE(S)"; exit 1; }
[ "$reds" -ne 0 ] && { echo "RESULT: $reds RED (intentional TDD-red)"; exit 1; }
echo "RESULT: all green"; exit 0
