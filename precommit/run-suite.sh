#!/bin/sh
# precommit/run-suite.sh — wires every suite in the tree into the pre-commit
# gate (A0.2, issue #11).  precommit/gate.sh runs this on every commit when
# MOJITO_GATE_FAST is not set.
#
# Each runner prints one machine-readable verdict line per driver:
#
#     VERDICT<TAB><driver-name><TAB><PASS|RED|FAIL>
#
# and gate.sh scores known-red allow-listing PER DRIVER against them (issue
# #141).  Before #141 the gate saw a single check named `suite` covering
# this entire script, so one allow-list row disarmed the whole tree.
#
# Every runner runs even when an earlier one fails, so a single commit
# reports every red rather than only the first (the gate needs the complete
# verdict set to score it).  The A0 spike harness's exit status used to be
# DISCARDED here — it is honoured now.
#
# Exit codes (the gate treats >=2 as a hard environment failure, 1 as
# TDD-red/test-fail, 0 as all-green):
#   0  every runner green
#   1  at least one driver is RED/FAIL
#   2  make preflight failed — the dylib cannot be produced (environment)
#   3  a runner script is missing — suite coverage lost (environment)
#
# issue #169: three cost tiers, selected by precommit/gate.sh via
# MOJITO_GATE_TIER (full | affected | hermetic) and, for "affected",
# MOJITO_GATE_STAGED (newline-separated staged paths):
#   hermetic  gate self-test only (always runs regardless of tier — see
#             below); every other lane skipped. For docs-only/hermetic-safe
#             diffs.
#   affected  gate self-test, plus ONLY the suites whose own tree is in
#             MOJITO_GATE_STAGED, with the A1.1 suite further scoped to the
#             exact driver files touched (mojito_async/test/run.sh's own
#             MOJITO_TEST_FILES support). For test-only diffs.
#   full      everything, unscoped — the original, unconditional behaviour
#             below. The default when MOJITO_GATE_TIER is unset, so a bare
#             `precommit/run-suite.sh` invocation (or CI, which never has
#             anything staged) is unchanged from before tiering existed.
set -u

cd "$(git rev-parse --show-toplevel)" || exit 2

TIER="${MOJITO_GATE_TIER:-full}"
STAGED="${MOJITO_GATE_STAGED:-}"

# touches <prefix> — true if any staged path starts with <prefix>.
touches() {
    prefix=$1
    result=1
    OLDIFS=$IFS; IFS='
'
    for p in $STAGED; do
        case "$p" in "$prefix"*) result=0 ;; esac
    done
    IFS=$OLDIFS
    return $result
}

rc=0

# --- #141 gate self-test — the gate's OWN tripwire test --------------------
# Runs first and needs no toolchain: it drives precommit/gate.sh inside a
# throwaway sandbox and asserts the gate actually blocks what it claims to
# block.  Nothing else in this tree tests the gate, which is how a blanket
# `suite` allow-list row survived on main for weeks. Always runs, in every
# tier: it is the "hermetic" baseline issue #169's fix section calls for.
SELFTEST="precommit/test-gate.sh"
if [ ! -x "$SELFTEST" ]; then
    echo "precommit/run-suite.sh: $SELFTEST missing; gate self-test coverage LOST."
    exit 3
fi
"$SELFTEST" || rc=1

if [ "$TIER" = "hermetic" ]; then
    exit "$rc"
fi

if [ "$TIER" = "affected" ]; then
    # Preflight: the A1.1 scoped run still needs the dylib.
    if ! make -s >/dev/null 2>&1; then
        echo "precommit/run-suite.sh: \`make\` at repo root failed; dylib cannot"
        echo "       be produced. This is an environment problem, not a test RED."
        exit 2
    fi

    A11SH="mojito_async/test/run.sh"
    test_files=""
    OLDIFS=$IFS; IFS='
'
    for p in $STAGED; do
        case "$p" in mojito_async/test/*.mojo) test_files="$test_files$p
" ;; esac
    done
    IFS=$OLDIFS
    if [ -n "$test_files" ]; then
        if [ ! -x "$A11SH" ]; then
            echo "precommit/run-suite.sh: $A11SH missing; A1.1 suite coverage LOST."
            exit 3
        fi
        MOJITO_TEST_FILES="$test_files" "$A11SH" || rc=1
    fi

    if touches "spike/colorless_runtime/tests/"; then
        RUNSH="spike/colorless_runtime/tests/run.sh"
        if [ ! -x "$RUNSH" ]; then
            echo "precommit/run-suite.sh: $RUNSH missing; suite coverage LOST."
            exit 3
        fi
        "$RUNSH" || rc=1
    fi

    if touches "bench/"; then
        for b in bench/run.sh bench/run_timer_scale.sh bench/run_echo.sh bench/run_fairness.sh; do
            if [ ! -x "$b" ]; then
                echo "precommit/run-suite.sh: $b missing; bench coverage LOST."
                exit 3
            fi
            "$b" || rc=1
        done
    fi

    exit "$rc"
fi

# --- full tier (default) ----------------------------------------------------
RUNSH="spike/colorless_runtime/tests/run.sh"
if [ ! -x "$RUNSH" ]; then
    echo "precommit/run-suite.sh: $RUNSH missing; suite coverage LOST."
    echo "       This is a hard gate failure (exit 3), not a pass: the spike"
    echo "       harness must exist for the gate to enforce anything."
    exit 3
fi

# Preflight: build the dylib so a missing artifact is not mistaken for TDD
# red. `make` only builds inside the repo (vendor C/asm -> build/ -> dylib).
if ! make -s >/dev/null 2>&1; then
    echo "precommit/run-suite.sh: \`make\` at repo root failed; dylib cannot"
    echo "       be produced. This is an environment problem, not a test RED."
    exit 2
fi

# --- A0 spike harness (issue #11) ------------------------------------------
"$RUNSH" || rc=1

# --- A1.1 runtime suite (issue #33) ----------------------------------------
A11SH="mojito_async/test/run.sh"
if [ ! -x "$A11SH" ]; then
    echo "precommit/run-suite.sh: $A11SH missing; A1.1 suite coverage LOST."
    exit 3
fi
"$A11SH" || rc=1

# --- A2.8 scheduler-scale bench (issue #74) --------------------------------
BENCHSH="bench/run.sh"
if [ ! -x "$BENCHSH" ]; then
    echo "precommit/run-suite.sh: $BENCHSH missing; bench coverage LOST."
    exit 3
fi
"$BENCHSH" || rc=1

# --- A6.5 F6 timer-scale bench (issue #88) ---------------------------------
TIMERBENCHSH="bench/run_timer_scale.sh"
if [ ! -x "$TIMERBENCHSH" ]; then
    echo "precommit/run-suite.sh: $TIMERBENCHSH missing; bench coverage LOST."
    exit 3
fi
"$TIMERBENCHSH" || rc=1

# --- A7.8 echo bench (issue #82) -------------------------------------------
ECHOBENCHSH="bench/run_echo.sh"
if [ ! -x "$ECHOBENCHSH" ]; then
    echo "precommit/run-suite.sh: $ECHOBENCHSH missing; bench coverage LOST."
    exit 3
fi
"$ECHOBENCHSH" || rc=1

# --- A7.9 fairness bench (issue #83) ---------------------------------------
FAIRNESSBENCHSH="bench/run_fairness.sh"
if [ ! -x "$FAIRNESSBENCHSH" ]; then
    echo "precommit/run-suite.sh: $FAIRNESSBENCHSH missing; bench coverage LOST."
    exit 3
fi
"$FAIRNESSBENCHSH" || rc=1

exit "$rc"
