# mojito-async A0 spike — colorless-runtime harness (A0.2, issue #11).
#
# Mirrors the mojito-sys S0 Makefile pattern: objects come exclusively from
# pattern rules into build/, so later lanes that add C/asm sources under
# spike/colorless_runtime/vendor/mojito-sys are picked up automatically by
# the wildcards below without editing this file.
#
# The production dylib lands at the repo root as libmojito_spike.dylib so
# `mojo run -Xlinker libmojito_spike.dylib ...` works anywhere.

CC      ?= cc
CFLAGS  ?= -O2 -g -Wall -Wextra
SPIKE   := spike/colorless_runtime/vendor/mojito-sys
BUILD   := build
DYLIB   := libmojito_spike.dylib
MOJO    ?= mojo
RUNSH   := spike/colorless_runtime/tests/run.sh
A11SH   := mojito_async/test/run.sh

CSRCS := $(wildcard $(SPIKE)/*.c)
SSRCS := $(wildcard $(SPIKE)/*.S)
OBJS  := $(patsubst $(SPIKE)/%.c,$(BUILD)/%.o,$(CSRCS)) \
         $(patsubst $(SPIKE)/%.S,$(BUILD)/%.o,$(SSRCS))

# Non-empty when at least one vendored source exists.  (OBJS by itself is a
# single space when empty, so concatenate the two source lists to test.)
HAS_SOURCES := $(CSRCS)$(SSRCS)

.DELETE_ON_ERROR:
.PHONY: all test clean

# Build the dylib only when the vendor substrate (A0.1) has landed.
all:
	@$(if $(HAS_SOURCES),$(MAKE) $(DYLIB),echo "make: no vendored C/asm sources under $(SPIKE) yet (A0.1 owns vendor/); nothing to build.")
	@echo "make: done."

$(BUILD):
	mkdir -p $@

$(BUILD)/%.o: $(SPIKE)/%.c | $(BUILD)
	$(CC) $(CFLAGS) -I$(SPIKE)/include -c $< -o $@

$(BUILD)/%.o: $(SPIKE)/%.S | $(BUILD)
	$(CC) -I$(SPIKE)/include -c $< -o $@

$(DYLIB): $(OBJS)
	$(CC) -dynamiclib -o $@ $^

# Build the dylib when present, then run the spike harness. When the vendor
# substrate is absent, run.sh reports a clear environment ERROR and exits 2
# (a missing substrate is not a test FAIL).
test:
	@$(if $(HAS_SOURCES),$(MAKE) $(DYLIB),echo "make test: vendor substrate $(SPIKE) absent yet (A0.1 owns vendor/); skipping dylib build.")
	@MOJO="$(MOJO)" CC="$(CC)" ./$(RUNSH)
	@MOJO="$(MOJO)" ./$(A11SH)

clean:
	rm -rf $(BUILD) $(DYLIB)