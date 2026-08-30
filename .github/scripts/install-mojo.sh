#!/bin/sh
# .github/scripts/install-mojo.sh — install the Mojo toolchain on a CI
# runner (issue #141).
#
# The dev host installs Mojo through the mojito/brew tap, whose formula pulls
# the toolchain straight from Modular's conda channel and unpacks it into
# libexec.  That formula is arm64/macOS only, so this script does the same
# three steps directly and picks the right channel subdirectory per platform,
# which is what lets the Linux lane exist at all.
#
# The .conda archive is a zip containing a pkg-*.tar.zst payload.  The
# toolchain bakes its build-machine prefix into share/max/modular.cfg and the
# driver resolves std/compilerrt through it, so the prefix has to be
# rewritten after unpacking exactly as the formula does.
#
# CI runs every Mojo lane against Modular's max-nightly channel by default,
# same reasoning as the mojolang-nightly Homebrew formula: mojito-sys is
# mid-migration to Mojo-first and wants to see a nightly compiler regression
# here before it reaches anyone building against a released toolchain. Set
# MOJO_CHANNEL=max to run a lane against the stable channel instead.
#
# Nightly filenames roll roughly daily (mojo-compiler-<date-stamped
# version>-release.conda), so there is no fixed version to pin the way the
# stable formula does — MOJO_VERSION, when unset on the nightly channel,
# resolves to whatever repodata.json currently reports as newest. Set
# MOJO_VERSION explicitly to pin a reproducible nightly build instead. This
# resolution runs before the issue #169 cache check below so a cache hit is
# still keyed on the actual version, not an empty placeholder — and it still
# costs a network round trip on a cache hit, which is the point: a nightly
# channel should notice a newer build exists even when yesterday's is cached.
set -eu

CHANNEL=${MOJO_CHANNEL:-max-nightly}
case "$CHANNEL" in
    max|max-nightly) ;;
    *) echo "install-mojo.sh: unknown MOJO_CHANNEL '$CHANNEL' (want max or max-nightly)"; exit 2 ;;
esac
VERSION=${MOJO_VERSION:-}
PREFIX=${MOJO_PREFIX:-$HOME/mojo-toolchain}

os=$(uname -s)
arch=$(uname -m)
case "$os/$arch" in
    Darwin/arm64)   subdir=osx-arm64 ;;
    Linux/x86_64)   subdir=linux-64 ;;
    Linux/aarch64)  subdir=linux-aarch64 ;;
    *) echo "install-mojo.sh: no Mojo build published for $os/$arch"; exit 2 ;;
esac

if [ -z "$VERSION" ]; then
    if [ "$CHANNEL" = max ]; then
        VERSION=1.0.0b2
    else
        echo "install-mojo.sh: resolving latest mojo-compiler on $CHANNEL/$subdir"
        VERSION=$(curl -fsSL "https://conda.modular.com/$CHANNEL/$subdir/repodata.json" | python3 -c '
import json, sys
d = json.load(sys.stdin)
pkgs = {**d.get("packages", {}), **d.get("packages.conda", {})}
names = sorted(n for n in pkgs if n.startswith("mojo-compiler-"))
if not names:
    sys.exit("install-mojo.sh: no mojo-compiler package found in repodata.json")
latest = names[-1]
v = latest[len("mojo-compiler-"):]
for suf in ("-release.conda", "-release.tar.bz2"):
    if v.endswith(suf):
        v = v[: -len(suf)]
        break
print(v)
')
        [ -n "$VERSION" ] || { echo "install-mojo.sh: could not resolve latest $CHANNEL version"; exit 2; }
    fi
fi

HOME_DIR="$PREFIX/share/max"

# issue #169: CI caches $PREFIX (actions/cache, keyed on this script's hash +
# VERSION) to turn the ~4 min download+unpack into a restore. A cache HIT
# means $PREFIX/bin/mojo already exists; skip straight to the env-export +
# compile-probe below instead of re-downloading. A version marker file (not
# just the binary's existence) guards against a stale cache entry surviving
# a MOJO_VERSION bump: if the marker doesn't match, fall through and
# re-install exactly as a cold run would. On the nightly channel VERSION is
# now resolved above before this check runs, so a cache entry from an
# earlier build today only hits while it's still actually the latest.
marker="$PREFIX/.install-mojo-version"
if [ -x "$PREFIX/bin/mojo" ] && [ -f "$marker" ] && [ "$(cat "$marker")" = "$VERSION" ]; then
    echo "install-mojo.sh: cache hit, $PREFIX already has mojo-compiler-$VERSION"
    skip_install=1
else
    skip_install=0
fi

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

if [ "$skip_install" != 1 ]; then

# zstd and unzip are the only external tools needed to unpack the payload.
# Inside a container this runs as root and there is no sudo; on a runner VM
# there is sudo and we are not root.  Pick whichever applies rather than
# assuming either.
SUDO=""
if [ "$(id -u)" != "0" ] && command -v sudo >/dev/null 2>&1; then
    SUDO=sudo
fi

if ! command -v zstd >/dev/null 2>&1; then
    if command -v apt-get >/dev/null 2>&1; then
        $SUDO apt-get update -qq && $SUDO apt-get install -y -qq zstd unzip
    elif command -v brew >/dev/null 2>&1; then
        brew install zstd
    else
        echo "install-mojo.sh: zstd not available and no package manager to get it"; exit 2
    fi
fi

echo "install-mojo.sh: fetching mojo-compiler-$VERSION from $CHANNEL/$subdir"
curl -fsSL -o "$work/mojo.conda" \
    "https://conda.modular.com/$CHANNEL/$subdir/mojo-compiler-$VERSION-release.conda"

mkdir -p "$work/stage" "$PREFIX"
unzip -q "$work/mojo.conda" -d "$work/stage"
payload=$(find "$work/stage" -name 'pkg-mojo-compiler-*.tar.zst' | head -1)
[ -n "$payload" ] || { echo "install-mojo.sh: no pkg-*.tar.zst in the .conda archive"; exit 2; }
zstd -dqc "$payload" | tar -xf - -C "$PREFIX"

cfg="$PREFIX/share/max/modular.cfg"
[ -f "$cfg" ] || { echo "install-mojo.sh: $cfg missing after unpack"; exit 2; }
baked=$(sed -n 's/^package_root[[:space:]]*=[[:space:]]*\(.*\)$/\1/p' "$cfg" | head -1)
[ -n "$baked" ] || { echo "install-mojo.sh: could not read the baked prefix from $cfg"; exit 2; }
awk -v old="$baked" -v new="$PREFIX" '{ gsub(old, new); print }' "$cfg" > "$cfg.rewritten"
mv "$cfg.rewritten" "$cfg"

# MODULAR_HOME points at the directory CONTAINING modular.cfg, which is
# share/max — not the package prefix.  The brew formula's wrapper does
# `export MODULAR_HOME="${MODULAR_HOME:-#{opt_libexec}/share/max}"`, and
# getting this wrong is silent: `mojo --version` still works, and then every
# compile fails with "use of unknown declaration 'Int'" and bogus indentation
# errors, because the driver cannot resolve the stdlib.
# (HOME_DIR is set above, before the skip_install branch, so the cache-hit
# path can reuse it too.)
printf '%s' "$VERSION" > "$marker"

fi  # skip_install

if [ -n "${GITHUB_ENV:-}" ]; then
    echo "MODULAR_HOME=$HOME_DIR" >> "$GITHUB_ENV"
fi
if [ -n "${GITHUB_PATH:-}" ]; then
    echo "$PREFIX/bin" >> "$GITHUB_PATH"
fi

# Prove the toolchain can actually COMPILE, not merely report a version:
# --version passes even with a broken MODULAR_HOME, which is exactly how the
# first version of this script looked fine and produced 100 failing drivers.
probe="$work/probe.mojo"
cat > "$probe" <<'PROBE'
def main():
    print("mojo-ok", 6 * 7)
PROBE
MODULAR_HOME="$HOME_DIR" "$PREFIX/bin/mojo" --version
out=$(MODULAR_HOME="$HOME_DIR" "$PREFIX/bin/mojo" run "$probe" 2>&1) || {
    echo "install-mojo.sh: the toolchain cannot compile a hello-world:"
    printf '%s\n' "$out" | tail -n 10
    exit 2
}
case "$out" in
    *"mojo-ok 42"*) ;;
    *) echo "install-mojo.sh: probe produced unexpected output: $out"; exit 2 ;;
esac
echo "install-mojo.sh: mojo installed at $PREFIX (MODULAR_HOME=$HOME_DIR)"
