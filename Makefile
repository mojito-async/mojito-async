# mojito-async — colororess-runtime + A1 production dylib build.
#
# Mirrors the mojito-sys S0 Makefile pattern: objects come exclusively from
# pattern rules into build/, so later lanes that add C/asm sources under
# spike/colorless_runtime/vendor/mojito-sys OR
# mojito_async/vendor/mojito-sys are picked up automatically by the
# wildcards below without editing this file.
#
# Two vendored source trees, same basenames for their shared substrate:
#   - SPIKE  spike/colorless_runtime/vendor/mojito-sys   (A0 spike harness)
#   - PROD   mojito_async/vendor/mojito-sys              (A1 production, #49)
# PROD is authoritative: the dylib is built from PROD only (OBJS below),
# and the spike harness's own Mojo bindings (mojito_spike.mojo) resolve
# their externs against that same PROD-built libmojito_spike.dylib at link
# time — SPIKE's own *.c/*.S are never compiled by any target here (the
# $(BUILD)/spike/%.o pattern rules below exist but nothing depends on
# them; #180). Objects are kept in build/prod/.
#
# The two trees are NOT asserted byte-identical (that claim used to stand
# here unchecked, and had been false since #101 landed on PROD without a
# matching re-vendor of SPIKE — issue #170 instance 6). What IS checked,
# in CI and via `make check-vendored`, is that every divergence between
# them is either absent or explicitly recorded with a content hash in
# mojito_async/vendor/VENDORED_EXCEPTIONS.tsv (the `spike:<basename>`
# rows) — see mojito_async/vendor/check-vendored.sh finding [4].

CC      ?= cc
CFLAGS  ?= -O2 -g -Wall -Wextra
SPIKE   := spike/colorless_runtime/vendor/mojito-sys
PROD    := mojito_async/vendor/mojito-sys
BUILD   := build
MOJO    ?= mojo

# Shared-library naming and flags per platform (issue #141: the Linux lanes
# have never executed anywhere, which is what mojito-sys#162/#163 are gated
# on; they cannot start executing while the only recipe here emits a Mach-O
# dylib).  The suite runners accept either name.
UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
DYLIB   := libmojito_spike.dylib
SHFLAGS := -dynamiclib
else
DYLIB   := libmojito_spike.so
SHFLAGS := -shared
CFLAGS  += -fPIC
endif
RUNSH   := spike/colorless_runtime/tests/run.sh
A11SH   := mojito_async/test/run.sh

CSRCS_S := $(wildcard $(SPIKE)/*.c)
SSRCS_S := $(wildcard $(SPIKE)/*.S)
CSRCS_P := $(wildcard $(PROD)/*.c)
SSRCS_P := $(wildcard $(PROD)/*.S)

# Link objects from PROD only (identical to SPIKE; avoids duplicate symbols).
OBJS   := $(patsubst $(PROD)/%.c,$(BUILD)/prod/%.o,$(CSRCS_P)) \
          $(patsubst $(PROD)/%.S,$(BUILD)/prod/%.o,$(SSRCS_P))

# Non-empty when at least one vendored source exists.
HAS_SOURCES := $(CSRCS_S)$(SSRCS_S)$(CSRCS_P)$(SSRCS_P)

.DELETE_ON_ERROR:
.PHONY: all test bench clean check-vendored

all:
	@$(if $(HAS_SOURCES),$(MAKE) $(DYLIB),echo "make: no vendored C/asm sources yet; nothing to build.")
	@echo "make: done."

$(BUILD):
	mkdir -p $@

$(BUILD)/spike $(BUILD)/prod:
	mkdir -p $@

$(BUILD)/spike/%.o: $(SPIKE)/%.c | $(BUILD)/spike
	$(CC) $(CFLAGS) -I$(SPIKE)/include -c $< -o $@

$(BUILD)/spike/%.o: $(SPIKE)/%.S | $(BUILD)/spike
	$(CC) -I$(SPIKE)/include -c $< -o $@

$(BUILD)/prod/%.o: $(PROD)/%.c | $(BUILD)/prod
	$(CC) $(CFLAGS) -I$(PROD)/include -c $< -o $@

$(BUILD)/prod/%.o: $(PROD)/%.S | $(BUILD)/prod
	$(CC) -I$(PROD)/include -c $< -o $@

$(DYLIB): $(OBJS)
	$(CC) $(SHFLAGS) -o $@ $^

# Build the dylib when present, then run the spike harness + A1 suite. When
# the vendor substrate is absent, run.sh reports a clear environment ERROR
# and exits 2 (a missing substrate is not a test FAIL).
test:
	@$(if $(HAS_SOURCES),$(MAKE) $(DYLIB),echo "make test: no vendored C/asm sources yet; skipping dylib build.")
	@MOJO="$(MOJO)" CC="$(CC)" ./$(RUNSH)
	@MOJO="$(MOJO)" ./$(A11SH)
	@MOJO="$(MOJO)" ./bench/run.sh

# A2.8 (issue #74): standalone benchmark run (bench/run.sh; also wired into
# the pre-commit suite via precommit/run-suite.sh).
bench:
	@MOJO="$(MOJO)" ./bench/run.sh

# mojito-sys#164: diff the vendored substrate against canonical mojito-sys
# and fail on divergence that is not recorded with a content hash. Needs a
# canonical tree: $MOJITO_SYS_DIR, a sibling ../mojito-sys checkout, or gh.
check-vendored:
	@./mojito_async/vendor/check-vendored.sh

clean:
	rm -rf $(BUILD) $(DYLIB)