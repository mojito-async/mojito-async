#!/bin/sh
# A0.1 — self-contained build + verification of the vendored mojito-sys
# S0 substrate (issue #10). Owned by lane A0.1; does NOT touch the repo-
# root Makefile (A0.2 owns that).
#
# Usage:
#   ./spike/colorless_runtime/build.sh          # full build + verify (matrix)
#   ./spike/colorless_runtime/build.sh build    # compile the dylib only
#
# Steps (all documented in spike/colorless_runtime/README.md):
#   1. cc -O2 -g -Wall -Wextra -I vendor/mojito-sys/include
#        -c vendor/mojito-sys/native_stack.c vendor/mojito-sys/ms_ctx.c
#   2. cc -I vendor/mojito-sys/include -c vendor/mojito-sys/aarch64_switch.S
#   3. cc -dynamiclib -o <repo-root>/libmojito_spike.dylib <objects>
#   4. contract_verify (compiled directly with cc; no dylib needed)
#   5. t0_contract.mojo (linked against the dylib)

set -u

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
SPIKE="$SCRIPT_DIR"
VENDOR="$SPIKE/vendor/mojito-sys"
TESTS="$SPIKE/tests"
BUILD="$SPIKE/build"
REPO_ROOT=$(CDPATH= cd -- "$SPIKE/../.." && pwd)
DYLIB="$REPO_ROOT/libmojito_spike.dylib"
MOJO=${MOJO:-mojo}

CC="cc"
CFLAGS="-O2 -g -Wall -Wextra"

failures=0
declare -a LABELS
declare -a VERDICTS
n=0

report() { # report <label> <status=0|nonzero> [extra lines...]
    label=$1
    ok=$2
    shift 2
    LABELS[$n]="$label"
    VERDICTS[$n]="$ok"
    n=$((n + 1))
    if [ "$ok" -eq 0 ]; then
        printf '%-40s PASS\n' "$label"
    else
        printf '%-40s FAIL\n' "$label"
        failures=$((failures + 1))
    fi
    for line in "$@"; do
        printf '    | %s\n' "$line"
    done
}

mkdir -p "$BUILD"

printf '%s\n' "== build libmojito_spike.dylib from vendored C/asm"
if ! $CC $CFLAGS -I"$VENDOR/include" -c "$VENDOR/native_stack.c" -o "$BUILD/native_stack.o"; then
    echo "FAIL: native_stack.c"
    exit 1
fi
if ! $CC $CFLAGS -I"$VENDOR/include" -c "$VENDOR/ms_ctx.c" -o "$BUILD/ms_ctx.o"; then
    echo "FAIL: ms_ctx.c"
    exit 1
fi
if ! $CC -I"$VENDOR/include" -c "$VENDOR/aarch64_switch.S" -o "$BUILD/aarch64_switch.o"; then
    echo "FAIL: aarch64_switch.S"
    exit 1
fi
if ! $CC -dynamiclib -o "$DYLIB" \
        "$BUILD/native_stack.o" "$BUILD/ms_ctx.o" "$BUILD/aarch64_switch.o"; then
    echo "FAIL: dylib link"
    exit 1
fi
echo "dylib built: $DYLIB"

if [ "${1:-}" = "build" ]; then
    exit 0
fi

echo ""
echo "== contract_verify (C-level, no dylib)"
$CC $CFLAGS -I"$VENDOR/include" \
    -DAARCH64_SWITCH_S="\"$VENDOR/aarch64_switch.S\"" \
    "$TESTS/contract_verify.c" \
    "$VENDOR/native_stack.c" "$VENDOR/ms_ctx.c" "$VENDOR/aarch64_switch.S" \
    -o "$BUILD/contract_verify" > "$BUILD/verify.log" 2>&1
if [ -f "$BUILD/contract_verify" ]; then
    out=$("$BUILD/contract_verify" 2>&1)
    st=$?
    report contract_verify "$st" "$(printf '%s\n' "$out" | tail -n 1)"
    printf '%s\n' "$out" | tail -n 8 | sed 's/^/    | /'
else
    report contract_verify 1 "compilation failed; see build/verify.log"
    printf '%s\n' "$(cat "$BUILD/verify.log" 2>/dev/null)" | tail -n 8 | sed 's/^/    | /'
fi

echo ""
echo "== t0_contract.mojo (driver linked against the dylib)"
if [ -f "$DYLIB" ]; then
    out=$("$MOJO" run -I "$VENDOR" -Xlinker "$DYLIB" "$TESTS/t0_contract.mojo" 2>&1)
    st=$?
    if [ "$st" -eq 0 ] && printf '%s' "$out" | grep -q "PASS"; then
        report t0_contract 0
    else
        report t0_contract 1
    fi
    printf '%s\n' "$out" | tail -n 10 | sed 's/^/    | /'
else
    report t0_contract 1 "dylib absent: driver cannot link (TDD red)"
fi

echo ""
echo "A0.1 substrate verification matrix (issue #10)"
for i in $(seq 0 $((n - 1))); do
    printf '  %-40s %s\n' "${LABELS[$i]}" "$([ "${VERDICTS[$i]}" -eq 0 ] && echo PASS || echo FAIL)"
done
if [ "$failures" -ne 0 ]; then
    echo "RESULT: $failures FAILED"
    exit 1
fi
echo "RESULT: all green"
exit 0