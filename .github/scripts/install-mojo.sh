#!/bin/sh
# .github/scripts/install-mojo.sh — install the pinned Mojo toolchain on a CI
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
set -eu

VERSION=${MOJO_VERSION:-1.0.0b2}
PREFIX=${MOJO_PREFIX:-$HOME/mojo-toolchain}

os=$(uname -s)
arch=$(uname -m)
case "$os/$arch" in
    Darwin/arm64)   subdir=osx-arm64 ;;
    Linux/x86_64)   subdir=linux-64 ;;
    Linux/aarch64)  subdir=linux-aarch64 ;;
    *) echo "install-mojo.sh: no Mojo build published for $os/$arch"; exit 2 ;;
esac

# zstd and unzip are the only external tools needed to unpack the payload.
if ! command -v zstd >/dev/null 2>&1; then
    if command -v apt-get >/dev/null 2>&1; then
        sudo apt-get update -qq && sudo apt-get install -y -qq zstd unzip
    elif command -v brew >/dev/null 2>&1; then
        brew install zstd
    else
        echo "install-mojo.sh: zstd not available and no package manager to get it"; exit 2
    fi
fi

work=$(mktemp -d)
echo "install-mojo.sh: fetching mojo-compiler-$VERSION from $subdir"
curl -fsSL -o "$work/mojo.conda" \
    "https://conda.modular.com/max/$subdir/mojo-compiler-$VERSION-release.conda"

mkdir -p "$work/stage" "$PREFIX"
unzip -q "$work/mojo.conda" -d "$work/stage"
payload=$(find "$work/stage" -name 'pkg-mojo-compiler-*.tar.zst' | head -1)
[ -n "$payload" ] || { echo "install-mojo.sh: no pkg-*.tar.zst in the .conda archive"; exit 2; }
zstd -dqc "$payload" | tar -xf - -C "$PREFIX"

cfg="$PREFIX/share/max/modular.cfg"
[ -f "$cfg" ] || { echo "install-mojo.sh: $cfg missing after unpack"; exit 2; }
baked=$(sed -n 's/^package_root[[:space:]]*=[[:space:]]*\(.*\)$/\1/p' "$cfg" | head -1)
[ -n "$baked" ] || { echo "install-mojo.sh: could not read the baked prefix from $cfg"; exit 2; }
sed "s|$baked|$PREFIX|g" "$cfg" > "$cfg.rewritten"
mv "$cfg.rewritten" "$cfg"

if [ -n "${GITHUB_ENV:-}" ]; then
    echo "MODULAR_HOME=$PREFIX" >> "$GITHUB_ENV"
fi
if [ -n "${GITHUB_PATH:-}" ]; then
    echo "$PREFIX/bin" >> "$GITHUB_PATH"
fi

MODULAR_HOME="$PREFIX" "$PREFIX/bin/mojo" --version
echo "install-mojo.sh: mojo installed at $PREFIX"
