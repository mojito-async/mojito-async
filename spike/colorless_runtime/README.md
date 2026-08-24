# A0.1 — vendored mojito-sys substrate + contract verification (issue #10)

Suite entry point (handoff to A0.2): `precommit/run-suite.sh` (owned by
A0.2) SHALL invoke `spike/colorless_runtime/tests/run.sh` when present;
run.sh may call this lane's `build.sh` to produce the dylib first.

Lane A0.1 of the A0 spike: vendors the **frozen** mojito-sys S0 substrate
(byte-identical, SHA-pinned) and proves the C-level contract it is built
on. Everything here is self-contained; it does NOT touch the repo-root
Makefile (A0.2 owns that) and does not wire any suite into
`precommit/run-suite.sh` (also A0.2).

## Layout

```text
spike/colorless_runtime/
  vendor/mojito-sys/          # frozen S0 substrate, copied VERBATIM from
    include/mojito_spike.h    #   github.com/mojito-async/mojito-sys
    native_stack.c            #   spike/context_switch/ @ 0ad48315
    aarch64_switch.S          #   (unchanged since contract commit 8454212)
    ms_ctx.c
    mojito_spike.mojo
    VENDORED_AT.txt           # provenance: repo, source commit, date
  tests/
    contract_verify.c         # C-level layout + allocator contract (no dylib)
    t0_contract.mojo          # Mojo driver linked against the dylib
  build.sh                    # self-contained build + verify
  build/                      # object files / test binaries (gitignored)
  <repo-root>/libmojito_spike.dylib   # built dylib (gitignored, where drivers run)
```

The boundary rule (docs/A0_PLAN.md): the spike uses ONLY this vendored
surface (`ms_stack_alloc/free`, `ms_ctx_make/switch`) — never raw
`mmap`/`pthread`/asm of its own.

## Building the dylib (step by step, inside this dir)

From `spike/colorless_runtime/`:

```sh
mkdir -p build
cc -O2 -g -Wall -Wextra -I vendor/mojito-sys/include \
   -c vendor/mojito-sys/native_stack.c -o build/native_stack.o
cc -O2 -g -Wall -Wextra -I vendor/mojito-sys/include \
   -c vendor/mojito-sys/ms_ctx.c -o build/ms_ctx.o
cc -I vendor/mojito-sys/include -c vendor/mojito-sys/aarch64_switch.S \
   -o build/aarch64_switch.o
cc -dynamiclib -o ../../libmojito_spike.dylib \
   build/native_stack.o build/ms_ctx.o build/aarch64_switch.o
```

(macOS arm64 only — `aarch64_switch.S` `#error`s off any other platform.)
`spike/colorless_runtime/build.sh` wraps exactly these steps plus the
verification below.

## Verification

```sh
./spike/colorless_runtime/build.sh          # matrix: contract_verify + t0_contract
./spike/colorless_runtime/build.sh build    # just the dylib
```

- `tests/contract_verify.c` — compiled and run directly with `cc` (no
  dylib needed). `_Static_assert`s the frozen v2 `ms_ctx_t` layout
  (`sizeof == 168`, `regs @ 0`, `fps @ 96`, `sp @ 160`), then at runtime
  checks `ms_page_size() > 0`, `ms_stack_alloc` (base below top, top
  16-byte aligned), `ms_stack_free`, and the `aarch64_switch.S`
  `#if !defined(__APPLE__)` guard.
- `tests/t0_contract.mojo` — the TDD-red driver. Imports the vendored
  `mojito_spike.mojo` and links against `libmojito_spike.dylib`
  (`mojo run -I vendor/mojito-sys -Xlinker <repo-root>/libmojito_spike.dylib`):
  calls `ms_page_size`, `ms_stack_alloc`, `ms_stack_free` and prints
  `PASS`. Without the dylib the externs cannot link — that intentional
  red was allow-listed in `precommit/known-red.tsv` (row `suite` ->
  issue #10) in the red commit and removed here, in the green commit.

## TDD discipline (this lane)

1. Red commit: vendored substrate + `contract_verify.c` + `t0_contract.mojo`
   + `build.sh` + known-red row `suite \t .../issues/10`. The driver is RED
   (dylib absent → externs unresolved). The gate's `suite` step is wired by
   A0.2; until then it reports n/a.
2. Green commit: dylib built from the vendored sources at the repo root
   (gitignored), the known-red row is removed, and both checks PASS.

Vendored files are byte-identical to the mojito-sys source (verified with
`cmp` against `spike/context_switch/` at the recorded commit). The
`.gitattributes` entry scopes a trailing-whitespace exemption to the
vendor tree only: upstream `ms_ctx.c` carries a trailing space that cannot
be edited away without breaking the verbatim-vendor contract.