#!/bin/sh
# precommit/test-docs.sh — RED check for issue #153.
#
# The README opens as pre-implementation above 115 merged PRs and points at a
# stale mirror; the spec's module map lists three modules that do not exist;
# the §7.1 public `sleep()` surface unconditionally raises; two validated
# config fields are never read; and a constant documented as "False in
# release" is hardcoded True.
#
# Individually each of these is minor. Together they are why two independent
# reviewers used the docs as the oracle for "what should this do", had to stop,
# and re-derived intent from the code. A spec that lies costs more than no
# spec, and this is the cheapest thing in EPIC #140 to fix.
#
# So this is a test rather than a checklist: every claim below is mechanically
# checkable against the tree, and the point of #153 is that it STAYS that way.
#
# Verdict: "PASS" + exit 0 when the docs match the tree; "RED" + exit 1
# otherwise.
set -u

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
SPEC="$ROOT/docs/mojito-async_IMPLEMENTATION_SPEC.md"
README="$ROOT/README.md"

findings=0
note() { findings=$((findings + 1)); printf '  - %s\n' "$1"; }

echo "T-docs consistency (issue #153)"
echo ""

# ---------------------------------------------------------------------------
# 1. the README's status section against the repository's own history
# ---------------------------------------------------------------------------
merged=$(git -C "$ROOT" log --oneline --merges 2>/dev/null | wc -l | tr -d ' ')
commits=$(git -C "$ROOT" log --oneline 2>/dev/null | wc -l | tr -d ' ')
if grep -qiE 'implementation has not started|^\*\*Pre-A0' "$README" 2>/dev/null; then
    note "README says implementation has not started, above $commits commits and $merged merges"
fi

# ---------------------------------------------------------------------------
# 2. canonical is GitHub; git.opsite.ca is a stale mirror
# ---------------------------------------------------------------------------
stale=$(grep -rn 'git\.opsite\.ca' "$README" "$ROOT/docs" 2>/dev/null || true)
if [ -n "$stale" ]; then
    note "docs point at git.opsite.ca, a stale mirror; canonical is github.com/mojito-async:"
    printf '%s\n' "$stale" | sed "s|$ROOT/||" | sed 's/^/        /'
fi

# ---------------------------------------------------------------------------
# 3. every module the spec's map lists either exists or is marked absent
# ---------------------------------------------------------------------------
# Names come from the §6 tree-drawing block ONLY — the first block that opens
# with a bare `mojito_async/` line — so later illustrative trees in the spec
# are not scored. A module the spec has since marked "not implemented" on the
# same line is fine. A driver that exists under a different name with the
# tree's `_aot` suffix convention counts as present.
map_lines=$(awk '/^mojito_async\/$/ { inblock = 1; next } inblock && /^```/ { exit } inblock { print }' "$SPEC")
missing=""
n_missing=0
for name in $(printf '%s\n' "$map_lines" | grep -oE '[A-Za-z0-9_]+\.mojo' | sort -u); do
    printf '%s\n' "$map_lines" | grep -F "$name" | grep -qi 'not implemented' && continue
    stem=${name%.mojo}
    if find "$ROOT" -path "$ROOT/build" -prune -o \
            \( -name "$name" -o -name "${stem}_aot.mojo" \) -print 2>/dev/null | grep -q .; then
        continue
    fi
    missing="$missing $name"
    n_missing=$((n_missing + 1))
done
if [ -n "$missing" ]; then
    note "the spec's §6 module map lists $n_missing module(s) that do not exist and are not marked absent:$missing"
    case "$missing" in
        *blocking_pool.mojo*)
            note "blocking_pool.mojo in particular: there is NO blocking pool, so any blocking call (DNS, file I/O) wedges a worker, and no user-facing doc says so" ;;
    esac
fi

# ---------------------------------------------------------------------------
# 4. a public surface that unconditionally raises must say so where a user
#    will read it
# ---------------------------------------------------------------------------
for fn in sleep sleep_until; do
    body=$(awk -v f="def $fn(" '
        index($0, f) == 1 { on = 1; next }
        on && /^def / { exit }
        on { print }' "$ROOT/mojito_async/time/sleep.mojo" 2>/dev/null)
    printf '%s' "$body" | grep -q 'raise Error' || continue
    printf '%s' "$body" | grep -qE '^\s*(park_|return|var |if )' && continue
    if ! grep -qiE "\`?$fn\(\)?\`?[^\n]*(not implemented|unavailable|raises)" "$README" "$SPEC" 2>/dev/null; then
        note "$fn() is spec §7.1 public surface and unconditionally raises, and neither the README nor the spec says so"
    fi
done

# ---------------------------------------------------------------------------
# 5. config fields that are validated and then never read
# ---------------------------------------------------------------------------
# Conservative on purpose: a file that merely MENTIONS the field counts as a
# consumer, so this under-reports rather than blocking a commit on a grep.
# stack_initial_commit_bytes currently escapes for exactly that reason — its
# only hit outside config.mojo is a comment in vendor/mojito_sys.mojo saying
# it has no consumer. It is still dead; this check just will not swear to it.
for field in enable_tracing stack_initial_commit_bytes; do
    users=$(grep -rl "$field" "$ROOT/mojito_async" 2>/dev/null \
        | grep -v '/test/' | grep -v '/config\.mojo$' | wc -l | tr -d ' ')
    if [ "$users" = "0" ]; then
        note "RuntimeConfig.$field is validated and then read by nothing outside config.mojo: either wire it or delete it"
    fi
done

# ---------------------------------------------------------------------------
# 6. a constant whose comment contradicts its value
# ---------------------------------------------------------------------------
idle="$ROOT/mojito_async/runtime/idle.mojo"
# The comment wraps across two lines, so normalise whitespace before matching.
if tr '\n' ' ' < "$idle" 2>/dev/null | tr -s ' ' | grep -q 'False in # release' \
   && grep -qE '^comptime IDLE_PAIR_ASSERT = True' "$idle" 2>/dev/null; then
    note "idle.mojo documents IDLE_PAIR_ASSERT as 'False in release' and hardcodes it True, so a debug-only raise ships"
fi

echo ""
if [ "$findings" -eq 0 ]; then
    printf 'VERDICT\tt_docs_consistency\tPASS\n'
    echo "T-docs consistency: PASS"
    exit 0
fi
printf 'VERDICT\tt_docs_consistency\tRED\n'
echo "T-docs consistency: RED ($findings finding(s))"
exit 1
