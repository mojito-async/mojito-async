# mojito-async — colororess-runtime + A1 production dylib build.
#
# Mirrors the mojito-sys S0 Makefile pattern: objects come exclusively from
# pattern rules into build/, so later lanes that add C/asm sources under
# spike/colorless_runtime/vendor/mojito-sys OR
# mojito_async/vendor/mojito-sys are picked up automatically by the
# wildcards below without editing this file.
#
# Two vendored source trees (byte-identical substrate, same basenames):
#   - SPIKE  spike/colorless_runtime/vendor/mojito-sys   (A0 spike harness)
#   - PROD   mojito_async/vendor/mojito-sys              (A1 production, #49)
# The substrate is byte-identical, so the dylib is built from the PROD tree
# (authoritative production copy); the spike harness and A1 suite both link
# the same libmojito_spike.dylib.  Objects are kept in build/prod/ (the
# spike objects are still produced by pattern rules, but NOT linked, to avoid
# duplicate identical symbols).

CC      ?= cc
CFLAGS  ?= -O2 -g -Wall -Wextra
SPIKE   := spike/colorless_runtime/vendor/mojito-sys
PROD    := mojito_async/vendor/mojito-sys
BUILD   := build
DYLIB   := libmojito_spike.dylib
MOJO    ?= mojo
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
.PHONY: all test clean

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
	$(CC) -dynamiclib -o $@ $^

# Build the dylib when present, then run the spike harness + A1 suite. When
# the vendor substrate is absent, run.sh reports a clear environment ERROR
# and exits 2 (a missing substrate is not a test FAIL).
test:
	@$(if $(HAS_SOURCES),$(MAKE) $(DYLIB),echo "make test: no vendored C/asm sources yet; skipping dylib build.")
	@MOJO="$(MOJO)" CC="$(CC)" ./$(RUNSH)
	@MOJO="$(MOJO)" ./$(A11SH)

clean:
	rm -rf $(BUILD) $(DYLIB)