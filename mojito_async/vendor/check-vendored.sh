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
    CANON_TMP=$(mktemp -d) || exit 2
    trap 'rm -rf "$CANON_TMP"' EXIT
    if ! gh repo clone mojito-async/mojito-sys "$CANON_TMP/mojito-sys" -- --depth 1 -q 2>/dev/null; then
        echo "check-vendored.sh: could not obtain canonical mojito-sys"
        echo "       (set MOJITO_SYS_DIR, or place a checkout beside this repo)"
        exit 2
    fi
    CANON="$CANON_TMP/mojito-sys"
else
    echo "check-vendored.sh: no canonical mojito-sys and no gh to fetch one."
    echo "       This is an environment failure, not a pass: an unverified"
    echo "       vendor tree must never read as an unchanged one."
    exit 2
fi

# WHICH canonical.  A sibling checkout is convenient and it is also the one
# way this check can quietly answer the wrong question: a working tree that
# is behind, ahead or dirty is not canonical, and comparing against it
# produces findings that look exactly like real ones.
#
# That is not hypothetical — it happened to me.  A local mojito-sys checkout
# five commits behind origin/main made this script report that the vendored
# include/mojito_sys.h had diverged in both directions, and I wrote that up
# as a finding.  It had not: the vendored header is byte-identical to
# canonical.  The stale tree was the whole of the difference.
#
# So when the canonical directory is a git repository, every comparison below
# reads file content out of `origin/main` rather than off disk, and the tree's
# own state is reported but not used.
CANON_REF=""
if [ -d "$CANON/.git" ] || git -C "$CANON" rev-parse --git-dir >/dev/null 2>&1; then
    if ! git -C "$CANON" fetch -q origin 2>/dev/null; then
        echo "check-vendored: WARNING: git fetch failed for $CANON; if origin/main" >&2
        echo "  is stale the comparison below may reflect an outdated canonical." >&2
    fi
    if git -C "$CANON" rev-parse -q --verify origin/main >/dev/null 2>&1; then
        CANON_REF="origin/main"
    fi
fi

echo "T-vendor substrate divergence (mojito-sys#164)"
echo "  vendored:  $VENDOR"
if [ -n "$CANON_REF" ]; then
    head_sha=$(git -C "$CANON" rev-parse --short HEAD 2>/dev/null || echo '?')
    ref_sha=$(git -C "$CANON" rev-parse --short "$CANON_REF" 2>/dev/null || echo '?')
    echo "  canonical: $CANON at $CANON_REF ($ref_sha)"
    if [ "$head_sha" != "$ref_sha" ]; then
        echo "             note: that checkout's HEAD is $head_sha, which is NOT"
        echo "             $CANON_REF. Content below is read from $CANON_REF, so"
        echo "             the working tree's state does not affect the result."
    fi
else
    echo "  canonical: $CANON (no git metadata; comparing against the working"
    echo "             tree as-is, which may be stale)"
fi
echo ""

# canon_read <path-relative-to-canon> — canonical CONTENT, from origin/main
# when there is one, on stdout.  Non-zero when the path does not exist there.
canon_read() {
    if [ -n "$CANON_REF" ]; then
        git -C "$CANON" show "$CANON_REF:$1" 2>/dev/null
    else
        [ -f "$CANON/$1" ] && cat "$CANON/$1"
    fi
}

canon_has() {
    if [ -n "$CANON_REF" ]; then
        git -C "$CANON" cat-file -e "$CANON_REF:$1" 2>/dev/null
    else
        [ -f "$CANON/$1" ]
    fi
}

# canon_grep <symbol> <path-prefix> — does that symbol appear under the
# prefix in canonical?  Whole-word FIXED string, not a regex: `git grep`
# does not honour \b in its ERE, and a pattern that silently matches nothing
# would report every symbol as absent.
canon_grep() {
    if [ -n "$CANON_REF" ]; then
        git -C "$CANON" grep -qwF "$1" "$CANON_REF" -- "$2" 2>/dev/null
    else
        grep -rqwF "$1" "$CANON/$2" 2>/dev/null
    fi
}

canon_where() {
    if [ -n "$CANON_REF" ]; then
        git -C "$CANON" grep -lwF "$1" "$CANON_REF" 2>/dev/null \
            | sed "s|^$CANON_REF:||" | head -2 | tr '\n' ' '
    else
        grep -rlwF "$1" "$CANON" 2>/dev/null | sed "s|$CANON/||" | head -2 | tr '\n' ' '
    fi
}

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
    rel=""
    for cand in "native/posix/$base" "native/include/$base"; do
        if canon_has "$cand"; then rel="$cand"; break; fi
    done
    if [ -z "$rel" ]; then
        printf '      %-22s NO CANONICAL COUNTERPART\n' "$base"
        continue
    fi
    if canon_read "$rel" | diff -q - "$v" >/dev/null 2>&1; then
        printf '      %-22s identical\n' "$base"
        continue
    fi
    nlines=$(canon_read "$rel" | diff -u - "$v" | grep -c '^[+-][^+-]' 2>/dev/null || echo '?')
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
    if ! canon_grep "$sym" "native"; then
        missing="$missing $sym"
    fi
done
if [ -z "$missing" ]; then
    echo "      every ms_/mjs_ extern resolves in canonical native/"
else
    echo "      NOT DEFINED ANYWHERE IN canonical native/:"
    for sym in $missing; do
        where=$(canon_where "$sym")
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
    if [ -n "$(recorded_hash '_spike_substrate_')" ]; then
        echo "      (extern gap RECORDED as intentional in VENDORED_EXCEPTIONS.tsv)"
    else
        findings=$((findings + 1))
    fi
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
    if [ -n "$(recorded_hash '_spike_substrate_')" ]; then
        echo "      (header gap RECORDED as intentional in VENDORED_EXCEPTIONS.tsv)"
    else
        findings=$((findings + 1))
    fi
fi


# ---------------------------------------------------------------------------
# 4. SPIKE vendor tree against PROD vendor tree (issue #170 instance 6)
# ---------------------------------------------------------------------------
# Makefile:9-16 claims "Two vendored source trees (byte-identical
# substrate, same basenames)": SPIKE (spike/colorless_runtime/vendor/
# mojito-sys, the A0 spike harness) and PROD (this dir, the A1 production
# copy check [1]-[3] above compare against canonical). Finding [1] only
# ever compares PROD against canonical mojito-sys — nothing compares SPIKE
# against PROD, which is the actual claim the Makefile makes. `diff -r`
# between the two trees is not empty today (issue #170), so this applies
# the identical "identical, or diverged-and-recorded" test as [1], keyed
# under a `spike:` prefix in VENDORED_EXCEPTIONS.tsv so a shared basename
# (e.g. native_stack.c) can carry independent PROD-vs-canonical and
# SPIKE-vs-PROD exception rows without colliding.
echo ""
echo "  [4] SPIKE vendor tree against PROD vendor tree (Makefile's"
echo "      \"byte-identical substrate, same basenames\" claim)"
SPIKE_DIR="$REPO_ROOT/spike/colorless_runtime/vendor/mojito-sys"
if [ ! -d "$SPIKE_DIR" ]; then
    echo "      $SPIKE_DIR missing; cannot compare"
else
    prod_counterpart() { # <basename> -> path under PROD, or empty
        for cand in "$VENDOR/$1" "$VENDOR/include/$1"; do
            [ -f "$cand" ] && { printf '%s' "$cand"; return 0; }
        done
        return 1
    }
    spike_only=""
    for s in "$SPIKE_DIR"/* "$SPIKE_DIR"/include/*; do
        [ -f "$s" ] || continue
        base=$(basename "$s")
        p=$(prod_counterpart "$base") || { spike_only="$spike_only $base"; continue; }
        if diff -q "$s" "$p" >/dev/null 2>&1; then
            printf '      %-22s identical\n' "$base"
            continue
        fi
        nlines=$(diff -u "$s" "$p" | grep -c '^[+-][^+-]' 2>/dev/null || echo '?')
        want=$(hash_of "$s")
        got=$(recorded_hash "spike:$base")
        if [ "$got" = "$want" ]; then
            printf '      %-22s diverged from PROD, RECORDED (%s lines)\n' "$base" "$nlines"
        else
            printf '      %-22s DIVERGED FROM PROD, UNRECORDED (%s lines)\n' "$base" "$nlines"
            if [ -z "$got" ]; then
                echo "          not listed in $(basename "$EXCEPTIONS") under key spike:$base"
            else
                echo "          recorded hash $got does not match $want:"
                echo "          the SPIKE file changed again after the exception was filed"
            fi
            findings=$((findings + 1))
        fi
    done
    if [ -n "$spike_only" ]; then
        echo "      SPIKE-only (no PROD basename match, informational):$spike_only"
    fi
    prod_only=""
    for p in "$VENDOR"/*.c "$VENDOR"/*.S "$VENDOR"/include/*.h; do
        [ -f "$p" ] || continue
        base=$(basename "$p")
        found=0
        for cand in "$SPIKE_DIR/$base" "$SPIKE_DIR/include/$base"; do
            [ -f "$cand" ] && found=1
        done
        [ "$found" -eq 0 ] && prod_only="$prod_only $base"
    done
    if [ -n "$prod_only" ]; then
        echo "      PROD-only (no SPIKE basename match, informational — PROD's"
        echo "      s6 I/O surface that the spike harness never needed):$prod_only"
    fi
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
