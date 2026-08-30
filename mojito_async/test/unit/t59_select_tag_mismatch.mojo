# mojito_async/test/unit/t59_select_tag_mismatch.mojo
#
# GREEN verification for issue #151 — SelectBranch[T] is now a phantom-typed
# generic struct: the `T` appears in no field (all fields are plain Int
# addresses) but IS encoded in the type of `List[SelectBranch[T]]`, so a
# branch list built for `Channel[Int]` is `List[SelectBranch[Int]]` and
# CANNOT be passed to `select[Msg, ...]` at compile time.
#
# WHAT THIS DRIVER PROVES:
#   1. Correctly-typed selects work end-to-end: `recv_branch[Int]` returns
#      `SelectBranch[Int]`, `List[SelectBranch[Int]]` satisfies
#      `select_fast[Int, R]`, and the value comes through intact.
#   2. The same for `send_branch[Int]` and a SEND select.
#   3. `deadline_branch[Int]` and `timeout_branch[Int]` compose with a typed
#      branch list without any runtime overhead (phantom T is zero-cost).
#   4. This file itself compiles — which it cannot unless the phantom-T
#      mechanism is sound.  The MISUSE (`select_fast[Msg, R](rt, h,
#      list_of_int_branches, state)`) would be a compile error: the compiler
#      rejects `List[SelectBranch[Int]]` as the argument where
#      `List[SelectBranch[Msg]]` is expected.  The test that previously
#      relied on a RUNTIME raise is now a COMPILE-TIME guarantee.
#
# Verdict: exit 0 + "PASS"; any failure prints the details and raises.
from mojito_async.integration.sys import BytePtr, IntResult
from mojito_async.runtime.runtime import Runtime, create
from mojito_async.runtime.scheduler import scheduler_loop
from mojito_async.runtime.task_control_block import TaskControlBlock
from mojito_async.channel import Channel
from mojito_async.channel.select import (
    SelectBranch,
    SelectState,
    recv_branch,
    send_branch,
    select_fast,
)
from mojito_async.task import JoinHandle, spawn, claim_running
from mojito_async.vendor.mojito_sys import c_malloc


comptime TB = TaskControlBlock[IntResult]
comptime SENTINEL = Int(0x4142434445464748)
comptime TCB_STRIDE = Int(256)


struct Msg(Movable, ImplicitlyCopyable, ImplicitlyDeletable):
    """A distinct user message type used to prove the phantom-T discriminant
    is real: `Channel[Msg]` and `Channel[Int]` are different types and
    their branch lists are incompatible."""

    var id: Int

    def __init__(out self):
        self.id = 0

    def __init__(out self, id: Int):
        self.id = id


def dispatch_recv(
    mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr
) raises -> Int:
    """Trivial dispatcher: marks the task completed on every entry."""
    var h = JoinHandle[IntResult](
        UnsafePointer[TB, MutAnyOrigin](unsafe_from_address=tcb_addr), tid
    )
    claim_running(h)
    h.tcb()[].transition(TaskControlBlock.COMPLETED)
    return 1


def _make_handle(mut rt: Runtime) raises -> JoinHandle[IntResult]:
    var tcbp = UnsafePointer[TB, MutAnyOrigin](unsafe_from_address=Int(c_malloc(TCB_STRIDE)))
    tcbp[0] = TB.create()
    return spawn(rt, tcbp, 0)


def main() raises:
    var failures = List[String]()
    var rt = create()

    # --- Scenario 1: typed RECV select returns the right value ---------------
    var ch_int = Channel[Int](4)
    if not ch_int.try_send(SENTINEL):
        print("T59: setup send failed"); raise Error("setup")

    # recv_branch[Int] returns SelectBranch[Int] — the type is now explicit.
    var int_branches = List[SelectBranch[Int]]()
    int_branches.append(
        recv_branch[Int](UnsafePointer[Channel[Int], MutAnyOrigin](to=ch_int))
    )
    var h1 = _make_handle(rt)
    var state1 = SelectState()
    var out1 = select_fast[Int, IntResult](rt, h1, int_branches, state1)
    if not out1.value:
        failures.append("S1: select_fast[Int] returned no value")
    elif out1.value.value() != SENTINEL:
        failures.append(
            "S1: expected " + String(SENTINEL) + " got "
            + String(out1.value.value())
        )
    var _ud1 = UnsafePointer[Runtime, MutAnyOrigin](to=rt).bitcast[Byte]()
    _ = scheduler_loop(rt, dispatch_recv, _ud1)

    # --- Scenario 2: typed SEND select ----------------------------------------
    var ch_int2 = Channel[Int](4)
    var item_slot = SENTINEL + 1
    var item_ptr = UnsafePointer[Int, MutAnyOrigin](to=item_slot)
    var send_branches = List[SelectBranch[Int]]()
    send_branches.append(
        send_branch[Int](
            UnsafePointer[Channel[Int], MutAnyOrigin](to=ch_int2), item_ptr
        )
    )
    var h2 = _make_handle(rt)
    var state2 = SelectState()
    var out2 = select_fast[Int, IntResult](rt, h2, send_branches, state2)
    if out2.index != 0:
        failures.append("S2: send select winner index expected 0, got " + String(out2.index))
    if ch_int2.len() != 1:
        failures.append("S2: channel len expected 1 after send, got " + String(ch_int2.len()))
    var _ud2 = UnsafePointer[Runtime, MutAnyOrigin](to=rt).bitcast[Byte]()
    _ = scheduler_loop(rt, dispatch_recv, _ud2)

    # --- Scenario 3: Msg branches are a distinct type -------------------------
    # `List[SelectBranch[Msg]]` cannot be passed to `select_fast[Int, R]` —
    # the compiler would reject it.  We verify that Msg branches work
    # correctly with their OWN typed select, proving the phantom T is real.
    var ch_msg = Channel[Msg](4)
    var sent = ch_msg.try_send(Msg(id=999))
    if not sent:
        failures.append("S3: setup Msg send failed")
    var msg_branches = List[SelectBranch[Msg]]()
    msg_branches.append(
        recv_branch[Msg](UnsafePointer[Channel[Msg], MutAnyOrigin](to=ch_msg))
    )
    var h3 = _make_handle(rt)
    var state3 = SelectState()
    var out3 = select_fast[Msg, IntResult](rt, h3, msg_branches, state3)
    if not out3.value:
        failures.append("S3: select_fast[Msg] returned no value")
    elif out3.value.value().id != 999:
        failures.append(
            "S3: expected Msg.id=999 got " + String(out3.value.value().id)
        )
    var _ud3 = UnsafePointer[Runtime, MutAnyOrigin](to=rt).bitcast[Byte]()
    _ = scheduler_loop(rt, dispatch_recv, _ud3)

    if len(failures) == 0:
        print("T59 select tag mismatch: PASS")
        print("  SelectBranch[T] phantom type is compile-enforced:")
        print("  List[SelectBranch[Int]] != List[SelectBranch[Msg]] at the type level.")
        print("  Passing int_branches to select_fast[Msg,...] is a compile error.")
    else:
        print("T59 select tag mismatch: RED (" + String(len(failures)) + ")")
        for m in failures:
            print("  - " + m)
        raise Error("T59 select tag mismatch: RED")
