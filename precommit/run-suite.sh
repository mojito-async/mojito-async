#!/bin/sh
# precommit/run-suite.sh — wires the colorless-runtime spike harness into the
# pre-commit gate (A0.2, issue #11). precommit/gate.sh runs this as its check
# named `suite` on every commit when MOJITO_GATE_FAST is not set.
#
# When the harness exists it is executed; its exit status propagates so the
# gate can distinguish PASS (exit 0) from not-yet-green (nonzero, allow-listed
# via the `suite` row in precommit/known-red.tsv) and from environment trouble.
# If the harness is not present yet, exit 0 with a note (nothing to enforce).

set -u

cd "$(git rev-parse --show-toplevel)" || exit 2

RUNSH="spike/colorless_runtime/tests/run.sh"
if [ -x "$RUNSH" ]; then
    "$RUNSH"
else
    echo "precommit/run-suite.sh: $RUNSH not present yet; spike suite not wired (A0.2)."
    exit 0
fi