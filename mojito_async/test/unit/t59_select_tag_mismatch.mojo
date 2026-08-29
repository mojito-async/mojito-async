# mojito_async/test/unit/t59_select_tag_mismatch.mojo
#
# RED driver for issue #151 — select's branches are type-erased with no tag
# check, unlike every other erased boundary in this tree.
#
# `channel/select.mojo:149-188` — `SelectBranch` carries `chan_addr: Int`,
# `item_addr: Int`, `kind: Int`.  No type tag.  `classify_branch[T]`
# (`:298-315`) and `_claim_at[T]` (`:488-514`) reinterpret `chan_addr` as
# `Channel[T]` via `unsafe_from_address` and call `try_recv`/`try_send`
# through it.  The branch factories are generic — `recv_branch[T]`,
# `send_branch[T]` — but return an UNTYPED `SelectBranch`, so nothing ties
# the `T` a branch was built with to the `T` the `select[T]` call is
# monomorphised with.
#
# This codebase already solved this exact problem one module away: `Scope`'s
# erased registry stamps `ScopeChild.TAG` at the typed boundary and every
# cast is checked, raising `ScopeTagMismatch`.  `select` adopted the erasure
# — its header cites ADR-015 — and dropped the check.
#
# And the erasure buys nothing: `select[T]` is single-`T` per call by
# documentation, so `SelectBranch[T]` with typed pointers would be equally
# expressive and compiler-enforced.
#
# THE MISUSE.  A program with `Channel[Int]` and `Channel[Msg]`, two branch
# lists, one `select[Int]` call handed the wrong list — or a copy-pasted call
# site with the wrong type parameter.  Both compile.
#
# WHAT THIS DRIVER DOES.  Builds a branch list over a `Channel[Int]` holding
# a known sentinel, hands it to `select[Msg]`, and asserts that a
# deterministic tag-mismatch error is raised.  `Msg` is deliberately
# layout-compatible with `Int` (one Int field), so the reinterpretation does
# NOT crash — it succeeds, quietly, and the driver can print the value that
# came through the wrong type.  That is the point of the finding: not a
# segfault a user would investigate, but a silent success.
#
# Verdict: exit 0 + "PASS" (a mismatch was refused); RED otherwise.
from mojito_async.integration.sys import BytePtr, IntResult
from mojito_async.runtime.runtime import Runtime, create
from mojito_async.runtime.task_control_block import TaskControlBlock
from mojito_async.channel import Channel
from mojito_async.channel.select import (
    SelectBranch,
    SelectState,
    recv_branch,
    select_fast,
)
from mojito_async.task import JoinHandle, spawn
from mojito_async.vendor.mojito_sys import c_malloc


comptime TB = TaskControlBlock[IntResult]
comptime SENTINEL = Int(0x4142434445464748)
comptime TCB_STRIDE = Int(256)


struct Msg(Movable, ImplicitlyCopyable, ImplicitlyDeletable):
    """A perfectly ordinary user message type. One Int field, so it is
    layout-compatible with Int: the reinterpretation below therefore
    SUCCEEDS rather than crashing, which is what makes the missing check
    dangerous instead of merely untidy."""

    var id: Int

    def __init__(out self):
        self.id = 0

    def __init__(out self, id: Int):
        self.id = id


def main() raises:
    var failures = List[String]()
    var rt = create()

    var tcbp = UnsafePointer[TB, MutAnyOrigin](
        unsafe_from_address=Int(c_malloc(TCB_STRIDE))
    )
    tcbp[0] = TB.create()
    var h = spawn(rt, tcbp, 0)

    # A channel of Int, with a value in it.
    var ch_int = Channel[Int](4)
    if not ch_int.try_send(SENTINEL):
        print("T59 select tag mismatch: RED (setup send failed)")
        raise Error("setup")

    # A branch list built for Channel[Int] — note recv_branch IS generic and
    # knows the type here, and then throws that knowledge away.
    var branches = List[SelectBranch]()
    branches.append(recv_branch[Int](UnsafePointer[Channel[Int], MutAnyOrigin](to=ch_int)))

    # The misuse: the SAME list handed to a select monomorphised for Msg.
    # Nothing in the type system objects, because SelectBranch is untyped.
    var state = SelectState()
    var len_before = ch_int.len()
    var refused = False
    var got_id = 0
    var got_kind = -1
    var had_value = False
    try:
        var outcome = select_fast[Msg, IntResult](rt, h, branches, state)
        got_kind = outcome.kind
        if outcome.value:
            had_value = True
            got_id = outcome.value.value().id
    except e:
        refused = True
        print("  select refused the mismatched branch list: " + String(e))

    if not refused:
        failures.append(
            "NO TAG CHECK — select[Msg] accepted a branch list built by"
            + " recv_branch[Int] and reinterpreted the Channel[Int] as a"
            + " Channel[Msg]. It claimed kind=" + String(got_kind)
            + " and handed back a Msg whose id is " + String(got_id)
            + " (the Int sentinel was " + String(SENTINEL)
            + "). No diagnostic, no raise, no way for the caller to notice."
        )
        var len_after = ch_int.len()
        if len_after < len_before:
            failures.append(
                "AND THE ITEM IS GONE — the Channel[Int] held " + String(len_before)
                + " item(s) before the mismatched select and " + String(len_after)
                + " after. The Int was consumed through a Channel[Msg]"
                + " reinterpretation and delivered as a Msg carrying the"
                + " Int's raw bits (had_value=" + String(had_value)
                + ", id=" + String(got_id) + "). Msg is layout-compatible"
                + " with Int here on purpose, so this reads as a plain type"
                + " confusion; for a Msg of any other shape the same path"
                + " reinterprets whatever memory follows. Either way the"
                + " item is consumed and nothing points at the cause."
            )
        print("  Scope's erased registry one module away stamps ScopeChild.TAG")
        print("  at the typed boundary and raises ScopeTagMismatch on every")
        print("  unchecked cast. select cites ADR-015 for the erasure and")
        print("  dropped the check, and the erasure buys nothing: select[T] is")
        print("  single-T per call by documentation, so SelectBranch[T] with")
        print("  typed pointers would be equally expressive and compiler-checked.")

    if len(failures) == 0:
        print("T59 select tag mismatch: PASS")
    else:
        print("T59 select tag mismatch: RED (" + String(len(failures)) + ")")
        for m in failures:
            print("  - " + m)
        raise Error("T59 select tag mismatch: RED")
