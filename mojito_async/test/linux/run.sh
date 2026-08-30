#!/bin/sh
# mojito_async/test/linux/run.sh — dedicated runner for the Linux
# io_uring completion-poller lane (issue #171, EPIC #140).
#
# Deliberately NOT wired into precommit/run-suite.sh or
# mojito_async/test/run.sh's globs: this is a Linux-host-and-flag-gated
# lane (mirrors mojito-sys's own tests/s6/iouring_submit, invoked by its
# own tests/docker/run-linux-lanes.sh rather than folded into the ambient
# suite every commit runs everywhere), not part of the suite every commit
# runs on every host. .github/workflows/ci.yml's `suite-linux-iouring`
# job calls this script directly with MOJITO_IO_URING=1 set.
#
# Exit codes (mirrors precommit/run-suite.sh's own convention, and the
# driver's own three-way exit code — see the header comment of
# t61_reactor_iouring_completion_aot.mojo):
#   0  the driver ran to completion and reported PASS: a real io_uring
#      ring was created and a registration + delivered event round-
#      tripped through it.
#   1  the driver ran and reported RED: this host passed the driver's own
#      guard (real Linux, real io_uring, MOJITO_IO_URING=1) and something
#      it actually checked came back wrong. A real defect.
#   2  ENVIRONMENT — either the driver itself could not be built (most
#      likely: `make` has not produced a linkable substrate; on Linux
#      today that is mojito-sys#164 — vendor/mojito-sys/aarch64_switch.S
#      is Apple/Mach-O only, so `make` cannot produce libmojito_spike.so
#      at all yet, tracked separately, not this lane's job to fix), or
#      the driver ran and reported its OWN UNSUPPORTED-PLATFORM (host
#      lacks io_uring, or MOJITO_IO_URING=1 is unset). Either way: nothing
#      was verified, positive or negative. NEVER treat this as green.
set -u
SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/../../.." && pwd)
BUILD_DIR="$REPO_ROOT/build"
MOJO=${MOJO:-mojo}
DRIVER="$SCRIPT_DIR/t61_reactor_iouring_completion_aot.mojo"
BIN="$BUILD_DIR/t61_reactor_iouring_completion_aot"

# Local manual runs default the flag on so `./run.sh` alone is enough to
# try the real path on a real Linux io_uring host; CI sets it explicitly.
MOJITO_IO_URING=${MOJITO_IO_URING:-1}
export MOJITO_IO_URING

command -v "$MOJO" >/dev/null 2>&1 || {
    echo "run.sh: mojo not found"
    echo "RESULT: ENVIRONMENT"
    exit 2
}

# Same .dylib/.so probe as mojito_async/test/run.sh (issue #141): the
# substrate is a .dylib on Darwin, .so elsewhere.
DYLIB="$REPO_ROOT/libmojito_spike.dylib"
[ -f "$DYLIB" ] || DYLIB="$REPO_ROOT/libmojito_spike.so"
LINK_FLAGS=""
if [ -f "$DYLIB" ]; then
    LINK_FLAGS="-Xlinker $DYLIB"
fi

mkdir -p "$BUILD_DIR" || true

# -O 0: same b2 1.0.0b2 default-optimization compiler crash class
# documented in mojito_async/test/run.sh's AOT_O0_DRIVERS note (this
# driver imports the same reactor package t39/t40/t41/... do).
if ! "$MOJO" build "$DRIVER" -o "$BIN" -I "$REPO_ROOT" $LINK_FLAGS -O 0 \
        >"$BUILD_DIR/t61_reactor_iouring_completion_aot.build.log" 2>&1; then
    echo "run.sh: build failed (most likely: the substrate could not be"
    echo "  linked -- see mojito-sys#164 if this is Linux). Last 20 lines:"
    tail -n 20 "$BUILD_DIR/t61_reactor_iouring_completion_aot.build.log" \
        | sed 's/^/    | /'
    echo "RESULT: ENVIRONMENT"
    exit 2
fi

out=$("$BIN" 2>&1)
st=$?
printf '%s\n' "$out" | sed 's/^/  | /'

case "$st" in
    0)
        echo "RESULT: PASS"
        exit 0
        ;;
    1)
        echo "RESULT: RED"
        exit 1
        ;;
    *)
        echo "RESULT: ENVIRONMENT (driver exit $st)"
        exit 2
        ;;
esac
