#!/bin/sh
# mojito_async/vendor/check-vendored.sh
#
# RED check for mojito-sys#164 — mojito-async runs a vendored spike
# substrate, not the frozen ABI mojito-sys hardened.
#
# The vendoring ITSELF is out of scope: it is the documented
# modular/modular#6971 workaround (externs must sit at concrete module scope
# in the consuming tree) and EPIC #140 excludes it. What is in scope, and
# what this script checks, is that the fork has been diverging in both
# directions with nothing to notice:
#
#   1. The vendored C must be byte-identical to canonical, or the divergence
#      must be RECORDED in a machine-readable manifest with a content hash
#      and a tracking issue. Today the only record is prose in
#      VENDORED_AT*.txt, which nothing checks and nothing can check, and the
#      Makefile's "byte-identical substrate" comment is false.
#
#   2. Every symbol mojito-async actually LINKS must be defined somewhere in
#      canonical mojito-sys `native/`. This is the structural finding: the
#      runtime calls ms_ctx_make / ms_ctx_switch, and mojito-sys froze
#      ms_context_capture / _destroy / _entry with a 200-byte v3 record, an
#      atomic arm, DEAD/RUNNING/FINISHED validation and _Static_asserts
#      pinning every asm-touched offset. Every context-switch safety property
#      built across PRs #66/#107/#114 protects an ABI the runtime never
#      calls, and anyone building against canonical mojito-sys does not get
#      the substrate mojito-async needs.
#
#   3. Every symbol the vendored header DECLARES should be implemented by
#      some vendored translation unit, or the header is promising an ABI the
#      tree cannot supply.
#
# Canonical is resolved, in order: $MOJITO_SYS_DIR, a sibling ../mojito-sys
# checkout, then a fetch through `gh api`. A canonical tree that cannot be
# resolved is an ENVIRONMENT failure (exit 2), never a pass: "I could not
# compare" must not read as "they match".
#
# Verdict: "PASS" + exit 0 when nothing has diverged unrecorded; "RED" +
# exit 1 otherwise.
set -u

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)
VENDOR="$SCRIPT_DIR/mojito-sys"
EXCEPTIONS="$SCRIPT_DIR/VENDORED_EXCEPTIONS.tsv"

[ -d "$VENDOR" ] || { echo "check-vendored.sh: $VENDOR missing"; exit 2; }

# --- resolve canonical mojito-sys ------------------------------------------
CANON=""
if [ -n "${MOJITO_SYS_DIR:-}" ] && [ -d "$MOJITO_SYS_DIR/native" ]; then
    CANON="$MOJITO_SYS_DIR"
elif [ -d "$REPO_ROOT/../mojito-sys/native" ]; then
    CANON=$(CDPATH= cd -- "$REPO_ROOT/../mojito-sys" && pwd)
elif command -v gh >/dev/null 2>&1; then
    CANON=$(mktemp -d) || exit 2
    if ! gh repo clone mojito-async/mojito-sys "$CANON/mojito-sys" -- --depth 1 -q 2>/dev/null; then
        echo "check-vendored.sh: could not obtain canonical mojito-sys"
        echo "       (set MOJITO_SYS_DIR, or place a checkout beside this repo)"
        exit 2
    fi
    CANON="$CANON/mojito-sys"
else
    echo "check-vendored.sh: no canonical mojito-sys and no gh to fetch one."
    echo "       This is an environment failure, not a pass: an unverified"
    echo "       vendor tree must never read as an unchanged one."
    exit 2
fi

echo "T-vendor substrate divergence (mojito-sys#164)"
echo "  vendored:  $VENDOR"
echo "  canonical: $CANON"
echo ""

TAB=$(printf '\t')
findings=0

hash_of() {
    if command -v shasum >/dev/null 2>&1; then
        shasum -a 256 "$1" | awk '{print $1}'
    else
        sha256sum "$1" | awk '{print $1}'
    fi
}

recorded_hash() { # <basename> -> the hash recorded for it, or empty
    [ -f "$EXCEPTIONS" ] || return 0
    awk -F"$TAB" -v f="$1" '$1==f {print $2; exit}' "$EXCEPTIONS"
}

# ---------------------------------------------------------------------------
# 1. vendored C/asm vs canonical
# ---------------------------------------------------------------------------
echo "  [1] vendored sources against canonical native/"
for v in "$VENDOR"/*.c "$VENDOR"/*.S "$VENDOR"/include/*.h; do
    [ -f "$v" ] || continue
    base=$(basename "$v")
    c=""
    for cand in "$CANON/native/posix/$base" "$CANON/native/include/$base"; do
        [ -f "$cand" ] && c="$cand" && break
    done
    if [ -z "$c" ]; then
        printf '      %-22s NO CANONICAL COUNTERPART\n' "$base"
        continue
    fi
    if diff -q "$c" "$v" >/dev/null 2>&1; then
        printf '      %-22s identical\n' "$base"
        continue
    fi
    nlines=$(diff -u "$c" "$v" | grep -c '^[+-][^+-]' 2>/dev/null || echo '?')
    want=$(hash_of "$v")
    got=$(recorded_hash "$base")
    if [ "$got" = "$want" ]; then
        printf '      %-22s diverged, RECORDED (%s lines)\n' "$base" "$nlines"
    else
        printf '      %-22s DIVERGED, UNRECORDED (%s lines)\n' "$base" "$nlines"
        if [ ! -f "$EXCEPTIONS" ]; then
            echo "          no $(basename "$EXCEPTIONS") exists at all: the only"
            echo "          record of this fork is prose in VENDORED_AT*.txt,"
            echo "          which nothing checks and nothing can check."
        elif [ -z "$got" ]; then
            echo "          not listed in $(basename "$EXCEPTIONS")"
        else
            echo "          recorded hash $got does not match $want:"
            echo "          the file changed again after the exception was filed"
        fi
        findings=$((findings + 1))
    fi
done

# ---------------------------------------------------------------------------
# 2. the symbols mojito-async actually links
# ---------------------------------------------------------------------------
echo ""
echo "  [2] externs mojito-async declares, against canonical native/"
externs=$(grep -rhoE '@extern\("[A-Za-z0-9_]+"\)' "$SCRIPT_DIR"/*.mojo "$SCRIPT_DIR"/*/*.mojo 2>/dev/null \
    | sed 's/@extern("//; s/")//' | grep -E '^(ms|mjs)_' | sort -u)
missing=""
for sym in $externs; do
    if ! grep -rqE "\\b$sym\\b" "$CANON/native" 2>/dev/null; then
        missing="$missing $sym"
    fi
done
if [ -z "$missing" ]; then
    echo "      every ms_/mjs_ extern resolves in canonical native/"
else
    echo "      NOT DEFINED ANYWHERE IN canonical native/:"
    for sym in $missing; do
        where=$(grep -rl "\\b$sym\\b" "$CANON" 2>/dev/null | head -2 | sed "s|$CANON/||" | tr '\n' ' ')
        if [ -n "$where" ]; then
            printf '        %-24s (canonical has it only under: %s)\n' "$sym" "$where"
        else
            printf '        %-24s (absent from canonical entirely)\n' "$sym"
        fi
    done
    echo "      mojito-async LINKS these. Anyone building against canonical"
    echo "      mojito-sys does not get the substrate mojito-async needs, and"
    echo "      every safety property PRs #66/#107/#114 built into the frozen"
    echo "      ms_context_* ABI protects code the runtime never executes."
    findings=$((findings + 1))
fi

# ---------------------------------------------------------------------------
# 3. header promises the vendored tree cannot keep
# ---------------------------------------------------------------------------
echo ""
echo "  [3] symbols the vendored header declares but no vendored .c defines"
undecl=""
for h in "$VENDOR"/include/*.h; do
    [ -f "$h" ] || continue
    for sym in $(grep -oE '\b(ms|mjs)_[a-z0-9_]+\s*\(' "$h" | sed 's/[[:space:]]*($//; s/($//' | tr -d '(' | sort -u); do
        if ! grep -rqE "\\b$sym\\b" "$VENDOR"/*.c "$VENDOR"/*.S 2>/dev/null; then
            undecl="$undecl $sym"
        fi
    done
done
undecl=$(printf '%s' "$undecl" | tr ' ' '\n' | sort -u | tr '\n' ' ')
n_undecl=$(printf '%s' "$undecl" | wc -w | tr -d ' ')
if [ "$n_undecl" -eq 0 ]; then
    echo "      none"
else
    echo "      $n_undecl declared-but-unimplemented symbol(s); the header was"
    echo "      vendored WHOLE while the C was vendored in slices, so it"
    echo "      promises an ABI this tree cannot supply. First few:"
    printf '%s' "$undecl" | tr ' ' '\n' | grep -v '^$' | head -8 | sed 's/^/        /'
    findings=$((findings + 1))
fi

echo ""
if [ "$findings" -eq 0 ]; then
    printf 'VERDICT\tvendor_substrate_divergence\tPASS\n'
    echo "T-vendor substrate divergence: PASS"
    exit 0
fi
printf 'VERDICT\tvendor_substrate_divergence\tRED\n'
echo "T-vendor substrate divergence: RED ($findings finding(s))"
exit 1
