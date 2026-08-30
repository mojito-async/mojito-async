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
#
# The sandbox includes a deterministic `gh` stub so:
#   - issue checks work in sandboxes without real network access
#   - results do not depend on whether a real GitHub issue is open or closed
#     (removing the time-bomb where a test fails the moment an issue closes)
#   - case 4 (closed-issue-row-refused) is correctly tested: the stub returns
#     CLOSED for issue/61 and OPEN for everything else, so the closed-issue
#     detection path in gate.sh is genuinely exercised, not bypassed by the
#     blocked-names check.
make_sandbox() {
    kr=$1
    suite_body=$2
    sb=$(mktemp -d 2>/dev/null) || return 1
    # Every filesystem/git operation below is anchored to $sb explicitly
    # (an absolute path, and `git -C`, never a bare `cd`) rather than
    # relying on a subshell's cwd. A bare `cd "$sb"` inside `( ... )` is
    # supposed to be isolated to that subshell, and `[ -z "$sb" ]` guards
    # downstream are supposed to catch mktemp failing outright — but this
    # sandbox WAS observed leaking into the real repo's shared git config
    # under concurrent gate runs this session (issue #169: several commits
    # across this session, in both mojito-async and mojito-sys, ended up
    # authored as "gate selftest <gate-selftest@example.invalid>" — `sh`
    # on this host treats `cd ""` as a silent no-op success rather than an
    # error, so any path where a sandbox var went missing under contention
    # would fall through to whatever cwd was already current, i.e. the
    # real repo, without any command here noticing). `git -C` removes the
    # dependency on `cd` succeeding and on subshell cwd propagation
    # entirely, so whatever the exact trigger was, it can't recur here.
    [ -n "$sb" ] && [ -d "$sb" ] || return 1
    mkdir -p "$sb/precommit" "$sb/bin" || return 1
    cp "$GATE" "$sb/precommit/gate.sh" || return 1
    chmod +x "$sb/precommit/gate.sh"
    printf '%s\n' "$kr" > "$sb/precommit/known-red.tsv"
    printf '%s\n' "$suite_body" > "$sb/precommit/run-suite.sh"
    chmod +x "$sb/precommit/run-suite.sh"
    # Deterministic gh stub: CLOSED for issue/61 (the canonical closed test
    # issue); OPEN for everything else.  No network call, no time-bomb.
    cat > "$sb/bin/gh" <<'GH_STUB'
#!/bin/sh
# Stub for gh api repos/OWNER/REPO/issues/NUM --jq .state
# Returns 'closed' for issue #61 (closed test issue), 'open' otherwise.
# Matches what `gh api ... --jq .state` actually outputs (lowercase string).
for a in "$@"; do
    case "$a" in */61) echo closed; exit 0 ;; esac
done
echo open
GH_STUB
    chmod +x "$sb/bin/gh"
    git -C "$sb" init -q . || return 1
    # Hard safety net, not just a convenience check: verify the sandbox
    # git considers ITSELF ($sb) really is its own repo, distinct from the
    # real one this script lives in, before touching anything with `add` or
    # `config`. This was added after `probe.txt` and a bogus commit author
    # leaked into the REAL repo from this sandbox under real (non-isolated
    # test) invocation — root cause not fully pinned down, so this check
    # exists to turn any recurrence into a loud `return 1` here instead of
    # a silent write to the wrong repository.
    sb_toplevel=$(git -C "$sb" rev-parse --show-toplevel 2>/dev/null)
    real_toplevel=$(git -C "$REPO_ROOT" rev-parse --show-toplevel 2>/dev/null)
    if [ -z "$sb_toplevel" ] || [ "$sb_toplevel" = "$real_toplevel" ]; then
        echo "make_sandbox: sandbox toplevel ('$sb_toplevel') is not a distinct repo from '$real_toplevel'; refusing to proceed" >&2
        return 1
    fi
    git -C "$sb" config --local user.email gate-selftest@example.invalid
    git -C "$sb" config --local user.name  "gate selftest"
    git -C "$sb" config --local commit.gpgsign false
    echo probe > "$sb/probe.txt"
    # Absolute path, not "probe.txt": a bare relative pathspec here is what
    # was observed leaking into the real repo's index under real (hook-
    # driven) invocation, even with `git -C "$sb"` — using $sb/probe.txt
    # removes any ambiguity about which repo's pathspec resolution applies.
    git -C "$sb" add "$sb/probe.txt" || return 1
    added=$(git -C "$sb" diff --cached --name-only)
    if [ "$added" != "probe.txt" ]; then
        echo "make_sandbox: expected only probe.txt staged in sandbox, got: $added" >&2
        return 1
    fi
    printf '%s' "$sb"
}

run_gate() { # $1 = sandbox; sets GATE_OUT / GATE_STATUS
    sb=$1
    if [ -z "$sb" ] || [ ! -d "$sb" ]; then
        GATE_OUT="run_gate: empty or missing sandbox path ('$sb')"
        GATE_STATUS=2
        return
    fi
    GATE_OUT=$(cd "$sb" && PATH="$sb/bin:$PATH" MOJITO_GATE_FAST=0 ./precommit/gate.sh 2>&1)
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
# The row is named t99_closed_probe (not a blocked suite name) so gate.sh
# reaches the closed-issue detection path rather than short-circuiting on the
# name check.  The sandbox gh stub returns CLOSED for issue/61 deterministically.
sb=$(make_sandbox \
    "t99_closed_probe${TAB}https://github.com/mojito-async/mojito-async/issues/61${TAB}2026-08-28" \
    "#!/bin/sh
printf 'VERDICT\tt99_closed_probe\tRED\n'
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
# Per-driver verdict row for precommit/gate.sh (issue #141): the gate's own
# test is a driver like any other, and is allow-listable by name.
if [ "$failures" -ne 0 ]; then
    printf 'VERDICT\tgate_selftest\tRED\n'
    echo "gate self-test: RED ($failures of $cases case(s) failed)"
    exit 1
fi
printf 'VERDICT\tgate_selftest\tPASS\n'
echo "gate self-test: PASS ($cases cases)"
exit 0
