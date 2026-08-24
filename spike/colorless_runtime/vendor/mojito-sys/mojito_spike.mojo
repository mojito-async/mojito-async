# Mojo bindings for libmojito_spike.dylib — S0 spike (issue #10).
#
# b2-legal formulation of the frozen binding names from
# spike/context_switch/CONTRACT.md ("Mojo bindings — AMENDED per #16") for
# Mojo 1.0.0b2 (2cf4d08a):
#
#   - Function declarations use `def`; `fn` is removed.
#   - External symbols are declared with `@extern("<c_symbol>")` plus an
#     explicit `abi("C")` effect between parameter list and result type, and a
#     `...` body. The decorator takes exactly ONE argument (the symbol name);
#     the library is chosen at link time (`mojo run/build -Xlinker <dylib>`),
#     not in the decorator.
#   - UnsafePointer carries mutability/origin parameters that must be
#     concrete inside extern signatures (no call-site inference there).
#     Origin story (single convention): pointers handed to / received from C
#     as raw addresses are `MutAnyOrigin`; Mojo-side scratch storage carved
#     out with std.memory.stack_allocation is `MutUntrackedOrigin`.
#
# ENTRY-CALLBACK MECHANISM (#10 owns this; proven end-to-end in demo.mojo):
#   The C side calls `entry(userdata)` with AAPCS64 semantics (x0 = userdata)
#   from its trampoline. A bare Mojo `def` value cannot be converted to a raw
#   code pointer (function types here are nominal; every conversion path is
#   rejected), and JIT-run exports are invisible to dlsym. The working
#   mechanism is therefore:
#     1. declare the callback `abi("C")` so its lowering IS the C ABI,
#     2. give it a stable link name with `@export("<name>")` (Mach-O prefixes
#        an underscore, so asm references `_<name>`),
#     3. materialize its address with `entry_pointer["<name>"]()` below,
#        which emits an adrp/add pair via std.sys.intrinsics
#        .inlined_assembly and passes the pointer to ms_ctx_make.
#   The trampoline then executes real Mojo code on the synthetic stack and
#   returns into ms_ctx_switch bookkeeping (verified end-to-end in
#   demo.mojo against the real dylib: yield, resume, and exit-back).
#
# Link with:  mojo run -Xlinker <path>/libmojito_spike.dylib demo.mojo


from std.sys.intrinsics import inlined_assembly

comptime LIB = "libmojito_spike.dylib"

comptime BytePtr = UnsafePointer[Byte, MutAnyOrigin]

# Pointer to caller-provided scratch holding one BytePtr each; this is what
# ms_stack_alloc writes *out_base / *out_top through.
comptime OutSlots = UnsafePointer[BytePtr, MutUntrackedOrigin]

# sizeof(ms_ctx_t) per the frozen v2 header: 12 GPRs + 8 FP lows + sp.
comptime MS_CTX_SIZE = 168


# Code address of an @export'd abi("C") Mojo callback, as a C function
# pointer (ms_entry_fn). `symbol_name` is the @export name WITHOUT the
# Mach-O underscore prefix.
def entry_pointer[symbol_name: String]() -> BytePtr:
    comptime asm_str = (
        "adrp ${0:x}, _" + symbol_name + "@PAGE\n"
        "add ${0:x}, ${0:x}, _" + symbol_name + "@PAGEOFF\n"
    )
    var addr = inlined_assembly[asm_str, UInt, constraints="=r"]()
    return UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=Int(addr))


@extern("ms_page_size")
def ms_page_size() abi("C") -> Int32:
    ...


@extern("ms_stack_alloc")
def ms_stack_alloc(
    bytes: Int,
    out_base: OutSlots,
    out_top: OutSlots,
) abi("C") -> Int32:
    ...


@extern("ms_stack_free")
def ms_stack_free(base: BytePtr) abi("C"):
    ...


@extern("ms_stack_total_size")
def ms_stack_total_size() abi("C") -> Int:
    ...


# ctx: 168-byte ms_ctx_t write target; stack_top: initial sp (16-aligned);
# entry: ms_entry_fn code pointer (see entry_pointer above);
# userdata: passed through unmodified to entry(userdata).
@extern("ms_ctx_make")
def ms_ctx_make(
    ctx: BytePtr,
    stack_top: BytePtr,
    entry: BytePtr,
    userdata: BytePtr,
) abi("C"):
    ...


# Saves current callee-saved state (x19-x30, d8-d15, sp) into *from_;
# resumes *to. Also records return_to := from_ for the trampoline.
@extern("ms_ctx_switch")
def ms_ctx_switch(from_: BytePtr, to: BytePtr) abi("C"):
    ...
