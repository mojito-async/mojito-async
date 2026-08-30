#!/bin/sh
# tests/docker/run-linux-suite.sh — run the suite on Linux, from any host.
#
# WHY.  mojito-async#141: the Linux lanes have never executed anywhere, and
# mojito-sys#162/#163 are gated on exactly that.  A macOS developer cannot
# run them at all, and waiting for CI to learn whether a Linux change works
# is a slow way to find out.
#
# The repository is mounted READ-ONLY and copied inside the container before
# anything is built, so a run never writes into the host tree: no build/, no
# libmojito_spike.so, nothing to clean up afterwards.  That also means the
# macOS dylib sitting in the host tree cannot confuse the Linux build.
#
# Usage:
#   tests/docker/run-linux-suite.sh                 # the A1 runtime suite
#   tests/docker/run-linux-suite.sh gate            # the full pre-commit gate
#   tests/docker/run-linux-suite.sh shell           # poke around inside
#
# Env:
#   DOCKER=<cmd>        container runtime (default: docker)
#   PLATFORM=<os/arch>  default: the daemon's own architecture.  Emulated
#                       platforms can silently lack syscalls the real one has
#                       (io_uring under Rosetta's linux/amd64 is the case that
#                       bit us in mojito-sys#163), so native is the default
#                       and a cross-architecture run has to be asked for.
#
# Exit: whatever the suite exits with; 2 for an environment failure.
set -u

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)
DOCKER=${DOCKER:-docker}
MODE=${1:-suite}
IMAGE=mojito-async-linux-suite

command -v "$DOCKER" >/dev/null 2>&1 || {
    echo "run-linux-suite.sh: $DOCKER not found."
    echo "RESULT: ENVIRONMENT"
    exit 2
}

if [ -z "${PLATFORM:-}" ]; then
    host_arch=$("$DOCKER" version --format '{{.Server.Arch}}' 2>/dev/null || echo "")
    [ -n "$host_arch" ] && PLATFORM="linux/$host_arch"
fi
plat=""
[ -n "${PLATFORM:-}" ] && plat="--platform $PLATFORM"

# shellcheck disable=SC2086
if ! "$DOCKER" build -q $plat -t "$IMAGE" -f "$SCRIPT_DIR/Dockerfile" "$SCRIPT_DIR" >/dev/null; then
    echo "run-linux-suite.sh: image build failed"
    echo "RESULT: ENVIRONMENT"
    exit 2
fi

if [ "$MODE" = "shell" ]; then
    # shellcheck disable=SC2086
    exec "$DOCKER" run --rm -it $plat -v "$REPO_ROOT":/src:ro "$IMAGE" bash
fi

# shellcheck disable=SC2086
"$DOCKER" run --rm -i $plat -e "MODE=$MODE" -v "$REPO_ROOT":/src:ro "$IMAGE" bash -s <<'INNER'
set -u
mkdir -p /work && cp -a /src/. /work/ && cd /work
# The host tree may carry a macOS build; start from nothing.
rm -rf build libmojito_spike.dylib libmojito_spike.so

echo "linux suite: kernel $(uname -r) $(uname -m)"
./.github/scripts/install-mojo.sh || exit 2
export MODULAR_HOME="$HOME/mojo-toolchain/share/max"
export PATH="$HOME/mojo-toolchain/bin:$PATH"

make || exit 2

case "$MODE" in
    gate)  exec ./precommit/gate.sh ;;
    *)     exec ./mojito_async/test/run.sh ;;
esac
INNER
