#!/bin/sh
# precommit/test-gate.sh — self-test for the pre-commit gate (issue #141).
#
# The gate is the one thing in this tree that nothing else tests, and EPIC
# #140's meta-finding is that it has been disarmed on `main` since the a3.2
# red commit: `precommit/known-red.tsv` carries a single blanket row named
# `suite`, and `suite` is the ENTIRE unit + stress run plus all four bench
# gates.  One row therefore converts any failure of anything into
# "RED (known-red, TDD)" and lets the commit through.
#
# This script is the missing test.  It drives the REAL precommit/gate.sh
# inside a throwaway git sandbox with a stubbed run-suite.sh, so each case
# is hermetic and fast (no Mojo, no dylib, no 30s suite), and asserts on the
# gate's OBSERVABLE behaviour: its exit status and what it printed.
#
# The verdict protocol the per-driver cases assume is one line per driver on
# the suite runner's stdout:
#
#     VERDICT<TAB><driver-name><TAB><PASS|RED|FAIL>
#
# and an allow-list row of three TAB-separated fields:
#
#     <driver-name><TAB><tracking-issue-url><TAB><yyyy-mm-dd added>
#
# Exit: 0 all cases pass; 1 at least one case failed (TDD red); 2 harness
# error (the gate under test is missing, mktemp failed, ...).
#
# Host rules: everything this script writes lives under a mktemp sandbox; it
# never deletes or modifies anything in the repository itself.
set -u

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
GATE="$SCRIPT_DIR/gate.sh"
REAL_KNOWN_RED="$SCRIPT_DIR/known-red.tsv"

[ -x "$GATE" ] || { echo "test-gate.sh: $GATE missing or not executable"; exit 2; }

TAB=$(printf '\t')
failures=0
cases=0

pass_case() { cases=$((cases + 1)); printf '  %-34s PASS\n' "$1"; }
fail_case() {
    cases=$((cases + 1))
    failures=$((failures + 1))
    printf '  %-34s RED   %s\n' "$1" "$2"
}

# ---------------------------------------------------------------------------
# sandbox: a throwaway git repo carrying a copy of the gate under test
# ---------------------------------------------------------------------------
# $1 = known-red.tsv body, $2 = run-suite.sh body.  Echoes the sandbox path.
make_sandbox() {
    kr=$1
    suite_body=$2
    sb=$(mktemp -d 2>/dev/null) || return 1
    mkdir -p "$sb/precommit" || return 1
    cp "$GATE" "$sb/precommit/gate.sh"
    chmod +x "$sb/precommit/gate.sh"
    printf '%s\n' "$kr" > "$sb/precommit/known-red.tsv"
    printf '%s\n' "$suite_body" > "$sb/precommit/run-suite.sh"
    chmod +x "$sb/precommit/run-suite.sh"
    (
        cd "$sb" || exit 1
        git init -q .
        git config user.email gate-selftest@example.invalid
        git config user.name  "gate selftest"
        git config commit.gpgsign false
        echo probe > probe.txt
        git add probe.txt
    ) >/dev/null 2>&1 || return 1
    printf '%s' "$sb"
}

run_gate() { # $1 = sandbox; sets GATE_OUT / GATE_STATUS
    GATE_OUT=$(cd "$1" && MOJITO_GATE_FAST=0 ./precommit/gate.sh 2>&1)
    GATE_STATUS=$?
}

echo "pre-commit gate self-test (issue #141)"
echo ""

# ---------------------------------------------------------------------------
# Case 1 (control): an unlisted suite failure must block the commit.
# This is the one property the gate has always had, and it is here so a
# regression in the OTHER direction — a gate that blocks nothing at all —
# cannot masquerade as a fix.
# ---------------------------------------------------------------------------
sb=$(make_sandbox \
    "# no allow-list rows" \
    "#!/bin/sh
echo 'boom: a driver failed'
exit 1")
if [ -z "$sb" ]; then
    echo "test-gate.sh: sandbox creation failed"; exit 2
fi
run_gate "$sb"
if [ "$GATE_STATUS" -ne 0 ]; then
    pass_case "unlisted-failure-blocks"
else
    fail_case "unlisted-failure-blocks" "gate exited 0 with an unlisted failing check"
fi

# ---------------------------------------------------------------------------
# Case 2: the shipped allow-list must not carry a blanket suite-wide row.
# `suite` is the whole test run plus every bench gate; a row naming it is
# not a TDD red, it is an off switch.
# ---------------------------------------------------------------------------
blanket=$(grep -v '^#' "$REAL_KNOWN_RED" 2>/dev/null \
    | awk -F"$TAB" 'NF>=1 && $1!="" {print $1}' \
    | grep -E '^(suite|t1-t7|t8-t14|bench|selftest|all)$' || true)
if [ -z "$blanket" ]; then
    pass_case "no-blanket-suite-row"
else
    fail_case "no-blanket-suite-row" "known-red.tsv allow-lists whole suite(s): $(printf '%s' "$blanket" | tr '\n' ' ')"
fi

# ---------------------------------------------------------------------------
# Case 3: every allow-list row must carry a name, a tracking issue and the
# date it was added, so a row can be aged out.  Two-field rows cannot be.
# ---------------------------------------------------------------------------
malformed=$(grep -v '^#' "$REAL_KNOWN_RED" 2>/dev/null \
    | awk -F"$TAB" '$0!="" { if (NF < 3 || $2 !~ /^https?:\/\// || $3 !~ /^[0-9]{4}-[0-9]{2}-[0-9]{2}$/) print $1 }' || true)
if [ -z "$malformed" ]; then
    pass_case "rows-carry-issue-and-date"
else
    fail_case "rows-carry-issue-and-date" "row(s) missing issue-url and/or yyyy-mm-dd: $(printf '%s' "$malformed" | tr '\n' ' ')"
fi

# ---------------------------------------------------------------------------
# Case 4: a row whose tracking issue is CLOSED must be refused by the gate.
# Issue #61 has been closed for weeks and its row is still what disarms the
# tripwire; a stale row has to become loud, not silent.
# ---------------------------------------------------------------------------
# The row is named `suite` on purpose: that is the check name today's gate
# matches on, so the row IS honoured and the only thing left under test is
# whether the gate notices the issue behind it is closed.
sb=$(make_sandbox \
    "suite${TAB}https://github.com/mojito-async/mojito-async/issues/61${TAB}2026-08-28" \
    "#!/bin/sh
printf 'VERDICT\tsuite\tRED\n'
echo 'boom: a driver failed'
exit 1")
run_gate "$sb"
if [ "$GATE_STATUS" -ne 0 ]; then
    pass_case "closed-issue-row-refused"
else
    fail_case "closed-issue-row-refused" "gate exited 0 honouring a row that tracks CLOSED issue #61"
fi

# ---------------------------------------------------------------------------
# Case 5: a row older than the staleness horizon must be refused.
# ---------------------------------------------------------------------------
# Named `suite` for the same reason as case 4: the row is honoured today, so
# the assertion isolates the staleness horizon and nothing else.
sb=$(make_sandbox \
    "suite${TAB}https://github.com/mojito-async/mojito-async/issues/140${TAB}2025-01-01" \
    "#!/bin/sh
printf 'VERDICT\tsuite\tRED\n'
echo 'boom: a driver failed'
exit 1")
run_gate "$sb"
if [ "$GATE_STATUS" -ne 0 ]; then
    pass_case "stale-row-refused"
else
    fail_case "stale-row-refused" "gate exited 0 honouring a row added 2025-01-01"
fi

# ---------------------------------------------------------------------------
# Case 6: allow-listing is PER DRIVER.  A row for t99_demo must cover
# t99_demo's red and nothing else — a second, unlisted driver failing in the
# same run still has to block the commit.  The `suite` row is deliberately
# present here: this is the exact shape that has been waving real defects
# through (PR #136's post-PASS SIGSEGV, root-caused as a UAF in PR #139).
# ---------------------------------------------------------------------------
sb=$(make_sandbox \
    "suite${TAB}https://github.com/mojito-async/mojito-async/issues/61${TAB}2026-08-28
t99_demo${TAB}https://github.com/mojito-async/mojito-async/issues/140${TAB}$(date +%Y-%m-%d)" \
    "#!/bin/sh
printf 'VERDICT\tt99_demo\tRED\n'
printf 'VERDICT\tt98_other\tFAIL\n'
exit 1")
run_gate "$sb"
if [ "$GATE_STATUS" -ne 0 ]; then
    pass_case "per-driver-scope-not-suite-wide"
else
    fail_case "per-driver-scope-not-suite-wide" "gate exited 0 with unlisted driver t98_other failing"
fi

# ---------------------------------------------------------------------------
# Case 7: the converse — a live per-driver row DOES cover its own driver, so
# a genuine TDD red still commits.  Without this the fix could be "block
# everything", which is not a working gate either.
# ---------------------------------------------------------------------------
sb=$(make_sandbox \
    "t99_demo${TAB}https://github.com/mojito-async/mojito-async/issues/140${TAB}$(date +%Y-%m-%d)" \
    "#!/bin/sh
printf 'VERDICT\tt99_demo\tRED\n'
printf 'VERDICT\tt98_other\tPASS\n'
exit 1")
run_gate "$sb"
if [ "$GATE_STATUS" -eq 0 ]; then
    pass_case "live-per-driver-row-honoured"
else
    fail_case "live-per-driver-row-honoured" "gate exited $GATE_STATUS refusing an open, dated, per-driver row"
fi

# ---------------------------------------------------------------------------
# Case 8: a harness/environment failure is never allow-listable.  The suite
# runner exiting >= 2 means the suite never ran; a known-red row must not be
# able to turn "I could not measure anything" into a pass.
# ---------------------------------------------------------------------------
sb=$(make_sandbox \
    "suite${TAB}https://github.com/mojito-async/mojito-async/issues/140${TAB}$(date +%Y-%m-%d)
t99_demo${TAB}https://github.com/mojito-async/mojito-async/issues/140${TAB}$(date +%Y-%m-%d)" \
    "#!/bin/sh
echo 'run-suite.sh: dylib cannot be produced'
exit 2")
run_gate "$sb"
if [ "$GATE_STATUS" -ne 0 ]; then
    pass_case "env-failure-not-allow-listable"
else
    fail_case "env-failure-not-allow-listable" "gate exited 0 on a suite runner that never ran (exit 2)"
fi

echo ""
if [ "$failures" -ne 0 ]; then
    echo "gate self-test: RED ($failures of $cases case(s) failed)"
    exit 1
fi
echo "gate self-test: PASS ($cases cases)"
exit 0
