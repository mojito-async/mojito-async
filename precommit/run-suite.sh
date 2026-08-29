#!/bin/sh
# precommit/run-suite.sh — wires the colorless-runtime spike harness into the
# pre-commit gate (A0.2, issue #11). precommit/gate.sh runs this as its check
# named `suite` on every commit when MOJITO_GATE_FAST is not set.
#
# Exit codes (the gate treats >=2 as a hard environment failure, 1 as
# TDD-red/test-fail, 0 as all-green):
#   0  harness ran and everything is green
#   1  harness ran; some test is RED/FAIL (not-yet-green)
#   2  make preflight failed — the dylib cannot be produced (environment)
#   3  harness script missing — suite coverage lost (environment)
set -u

cd "$(git rev-parse --show-toplevel)" || exit 2

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

"$RUNSH"

# --- A1.1 runtime suite (issue #33) — additional run_check on the same gate ---
A11SH="mojito_async/test/run.sh"
if [ ! -x "$A11SH" ]; then
    echo "precommit/run-suite.sh: $A11SH missing; A1.1 suite coverage LOST."
    exit 3
fi
"$A11SH" || exit 1
# --- A2.8 scheduler-scale bench (issue #74) — same gate discipline --------
BENCHSH="bench/run.sh"
if [ ! -x "$BENCHSH" ]; then
    echo "precommit/run-suite.sh: $BENCHSH missing; bench coverage LOST."
    exit 3
fi
"$BENCHSH" || exit 1
# --- A6.5 F6 timer-scale bench (issue #88) — same gate discipline ---------
TIMERBENCHSH="bench/run_timer_scale.sh"
if [ ! -x "$TIMERBENCHSH" ]; then
    echo "precommit/run-suite.sh: $TIMERBENCHSH missing; bench coverage LOST."
    exit 3
fi
"$TIMERBENCHSH" || exit 1
