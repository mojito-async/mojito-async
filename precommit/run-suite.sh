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
set -u

cd "$(git rev-parse --show-toplevel)" || exit 2

rc=0

# --- #141 gate self-test — the gate's OWN tripwire test --------------------
# Runs first and needs no toolchain: it drives precommit/gate.sh inside a
# throwaway sandbox and asserts the gate actually blocks what it claims to
# block.  Nothing else in this tree tests the gate, which is how a blanket
# `suite` allow-list row survived on main for weeks.
SELFTEST="precommit/test-gate.sh"
if [ ! -x "$SELFTEST" ]; then
    echo "precommit/run-suite.sh: $SELFTEST missing; gate self-test coverage LOST."
    exit 3
fi
"$SELFTEST" || rc=1

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
