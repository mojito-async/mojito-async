# A0-T4b — AOT wiring/lifecycle smoke for fiber.mojo (issue #13).
#
# SCOPE (read before "fixing"): the b2 compiler cannot lower extern calls
# that live inside fiber.mojo (modular/modular#6971 family — JIT *and*
# AOT), so Fiber.resume()/suspend()/destroy() MUST NOT be invoked here;
# they are exercised today only by the raw-substrate choreography in
# t4_fiber.mojo.  This driver covers what CAN safely run: module import,
# create()'s heap-block allocation + scratch wiring, the address
# accessors used by the switch path, alive()/destroy() state flips.
#
# Verdict convention: prints PASS / FAIL, exits 0 / 1.

from fiber import Fiber, create
from mojito_spike import MS_CTX_SIZE, ms_stack_alloc
from std.memory import stack_allocation


@extern("exit")
def _c_exit(code: Int32) abi("C"): ...


def main() raises:
    var failures = List[String]()

    var fp = stack_allocation[1, Fiber]()
    fp[0] = Fiber()

    var entry_stub = UnsafePointer[Byte, MutAnyOrigin](
        unsafe_from_address=0x1000
    )
    var ud_stub = UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=0x2000)

    var slots = stack_allocation[2, UnsafePointer[Byte, MutAnyOrigin]]()
    if ms_stack_alloc(256 * 1024, slots, slots + 1) != 0:
        print("FAIL: ms_stack_alloc")
        _c_exit(1)

    create(
        Int(slots[0]),
        Int((slots + 1)[0]),
        entry_stub,
        ud_stub,
        fp,
    )

    if not fp[0].alive():
        failures.append("fiber not alive after create")

    if Int(fp[0].stack_top()) & 0xF != 0:
        failures.append("stack_top not 16-byte aligned")

    if Int(fp[0].stack_base()) == 0:
        failures.append("stack_base unset")

    # Scratch wiring: create() must have stored entry/userdata where the
    # switch path will read them (block tail: +FRAME_BYTES / +FRAME_BYTES+8).
    var blk = Int(fp[0]._block)
    var ep = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=blk + 2 * MS_CTX_SIZE + 24
    )
    var upp = UnsafePointer[Int, MutAnyOrigin](
        unsafe_from_address=blk + 2 * MS_CTX_SIZE + 32
    )
    if Int(ep[]) != 0x1000:
        failures.append("entry scratch not wired")
    if Int(upp[]) != 0x2000:
        failures.append("userdata scratch not wired")

    fp[0].destroy()
    if fp[0].alive():
        failures.append("destroy left fiber alive")

    if len(failures) == 0:
        print("T4B fiber module wiring smoke: PASS")
    else:
        for msg in failures:
            print("FAIL:", msg)
        print(
            "T4B fiber module wiring smoke: FAIL (" + String(len(failures)) + ")"
        )
        _c_exit(1)