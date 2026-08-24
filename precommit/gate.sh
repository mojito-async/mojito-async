#!/bin/sh
# mojito pre-commit gate — runs locally before every commit.
#
# Tier 0: structural validators.                   Always run; block on failure.
# Tier 1: test suite. The A0 spike lanes add suites under mojito/tests and
#         wire them into precommit/run-suite.sh as they land; until then the
#         suite step reports "no suite defined" and passes. Failures block
#         the commit UNLESS allow-listed in precommit/known-red.tsv as an
#         intentional TDD-red test with a tracking issue.
#
# Env:
#   MOJITO_GATE_FAST=1  skip the suite step entirely.
#   MOJO=</path/to/mojo> override the Mojo toolchain.
#
# Host rules (same as claude/OX agents on this host):
#   - This gate NEVER deletes or modifies anything outside the workspace; it
#     only builds inside the repo and runs the project's own test suites.
set -u

cd "$(git rev-parse --show-toplevel)" || exit 2

GATE_DIR="$PWD/precommit"
KNOWN_RED="$GATE_DIR/known-red.tsv"
FAST="${MOJITO_GATE_FAST:-0}"
failures=0

say() { printf '%s\n' "$*"; }

# ---------------------------------------------------------------- Tier 0 ----
ws_errors=$(git diff --cached --check 2>&1)
if [ -n "$ws_errors" ]; then
    say "Tier 0 FAIL: whitespace errors in staged diff:"
    printf '%s\n' "$ws_errors" | sed 's/^/  | /'
    failures=$((failures + 1))
fi

blocked=$(git diff --cached --name-only \
    | grep -E '(^|/)(build|\.build)/|\.(dylib|o|a|pyc|class|tmp|swp)$|\.DS_Store' || true)
if [ -n "$blocked" ]; then
    say "Tier 0 FAIL: build artifacts / junk must not be committed:"
    printf '%s\n' "$blocked" | sed 's/^/  | /'
    failures=$((failures + 1))
fi

conflicts=$(git grep --cached -n -E '^(<<<<<<< |>>>>>>> )' -- \
    '*.mojo' '*.c' '*.S' '*.h' '*.sh' '*.md' 2>/dev/null || true)
if [ -n "$conflicts" ]; then
    say "Tier 0 FAIL: unresolved conflict markers in staged content:"
    printf '%s\n' "$conflicts" | sed 's/^/  | /'
    failures=$((failures + 1))
fi

# ---------------------------------------------------------------- Tier 1 ----
run_check() { # <name> <command...>
    name=$1
    shift
    out=$("$@" 2>&1)
    st=$?
    if [ "$st" -eq 0 ]; then
        printf '%-38s PASS\n' "$name"
    elif grep -q "^$name	" "$KNOWN_RED"; then
        printf '%-38s RED (known-red, TDD)\n' "$name"
        printf '%s\n' "$out" | tail -n 4 | sed 's/^/    | /'
    else
        printf '%-38s FAIL\n' "$name"
        printf '%s\n' "$out" | tail -n 12 | sed 's/^/    | /'
        failures=$((failures + 1))
    fi
}

if [ "$FAST" != "1" ]; then
    if [ -x "$GATE_DIR/run-suite.sh" ]; then
        run_check suite "$GATE_DIR/run-suite.sh"
    else
        say "suite                                   n/a (no precommit/run-suite.sh yet; A0 lanes will wire suites here)"
    fi
fi

# ---------------------------------------------------------------- summary ----
if [ "$failures" -ne 0 ]; then
    say ""
    say "GATE FAILED ($failures issue(s))."
    say "  - Unexpected test failure? Fix the code. After the change the"
    say "    commit may proceed."
    say "  - Intentional TDD red? Add the test name to precommit/known-red.tsv"
    say "    with its tracking issue, and remove the row when it goes green."
    say "  - Emergency escape hatch: git commit --no-verify (see"
    say "    precommit/README.md for why this should stay rare)."
    exit 1
fi
say "gate: all checks passed"
exit 0