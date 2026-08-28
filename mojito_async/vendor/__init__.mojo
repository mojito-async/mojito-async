# mojito_async/vendor/__init__.mojo
#
# A1.1 (issue #49) — vendored mojito-sys substrate package.
#
# The C/asm substrate lives under mojito_async/vendor/mojito-sys/ (frozen
# S0, see VENDORED_AT.txt); the Mojo type surface + extern bindings live in
# mojito_async/vendor/mojito_sys.mojo — the batch-wide SINGLE home of
# NativeStack and the ms_* externs (fiber #49, continuation #50, stack pool
# #52 all import from here; nothing redefines them).
#
# Spec §6: integration/sys.mojo remains the runtime's extern-free low-level
# adapter; this vendor module is the raw mojito-sys C-ABI firewall.
from mojito_async.vendor.mojito_sys import (
    BytePtr,
    MS_CTX_SIZE,
    NativeStack,
    OutSlots,
    entry_pointer,
    ms_ctx_make,
    ms_ctx_switch,
    ms_page_size,
    ms_stack_alloc,
    ms_stack_free,
    ms_stack_is_live,
    ms_stack_total_size,
    stack_free,
)