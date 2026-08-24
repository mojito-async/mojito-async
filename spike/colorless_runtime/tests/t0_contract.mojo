# A0.1 Mojo driver (issue #10) — verifies the vendored mojito-sys bindings
# against the dylib built from the vendored C/asm sources.
#
# Links against libmojito_spike.dylib at build time (S0 convention, see
# mojito_spike.mojo header):
#   mojo run -I spike/colorless_runtime/vendor/mojito-sys \
#            -Xlinker libmojito_spike.dylib \
#            spike/colorless_runtime/tests/t0_contract.mojo
#
# Without the dylib the externs cannot link, so the driver cannot run:
# that is the intentional TDD-red state (commit A). With the dylib built
# from the vendored sources it exercises ms_page_size + ms_stack_alloc/
# free and prints PASS.

from std.memory import stack_allocation

from mojito_spike import (
    BytePtr,
    ms_page_size,
    ms_stack_alloc,
    ms_stack_free,
)


def main() raises:
    var ps = Int(ms_page_size())
    print("page size:", ps)
    if ps <= 0:
        raise Error("ms_page_size must return a positive page size")

    # Out-slots handed to ms_stack_alloc; C writes base/top pointers here.
    var slots = stack_allocation[2, BytePtr]()

    var rc = ms_stack_alloc(2 * ps, slots, slots + 1)
    if rc != 0:
        raise Error("ms_stack_alloc failed with rc " + String(rc))
    print("stack alloc ok; base:", slots[], "top:", (slots + 1)[])

    if (slots + 1)[] < slots[]:
        raise Error("ms_stack_alloc: top must be above base")
    if (Int((slots + 1)[]) & 0xF) != 0:
        raise Error("ms_stack_alloc: top (initial SP) must be 16-byte aligned")

    ms_stack_free(slots[])
    print("stack free ok")

    print("PASS")