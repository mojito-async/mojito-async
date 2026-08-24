#!/bin/sh
# Enables the mojito pre-commit gate for this clone:
#   precommit/install-hooks.sh
# Equivalent: git config core.hooksPath .githooks
set -u
root=$(git rev-parse --show-toplevel) || exit 1
git config core.hooksPath .githooks
printf 'pre-commit gate installed: %s/.githooks/pre-commit\n' "$root"
printf 'gate script:              %s/precommit/gate.sh\n' "$root"