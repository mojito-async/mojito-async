# spike/colorless_runtime/tests/t6_join_semantics_aot.mojo
#
# A0.6 (issue #15) — JOIN SEMANTICS: A0-T4, A0-T5, A0-T8.
#
#   A0-T4 join-before-completion: the parent joins child A before A
#         completes; sibling B PROGRESSES during the wait — observable via
#         the shared-counter/sequence log (A.step1 < B.run < A.step2) and by
#         the final counter value 1 + 100 + 10 = 111.
#   A0-T5 join-after-completion: joining an already-COMPLETED child performs
#         NO parking; the result is consumed directly.
#   A0-T8 child error propagation: a raising child reaches COMPLETED with its
#         error message preserved; join() re-raises with that message.
#
# MODELING NOTE (b2-honest, per lane brief): this driver proves the
# scheduling/join STATE semantics through the pure-Mojo library protocol
# (spawn/execute/park/wake/claim_running) with the embedding driver acting as
# the scheduler loop (work-first, spec §88).  True MID-FRAME suspension with
# exact-resume fibers is proven separately in tests/t2_worker_reuse_aot.mojo
# (the tests/t4_fiber.mojo shape); modular/modular#6971 forbids putting those
# externs inside imported modules, so raw ms_ctx_* choreography stays in the
# driver module by construction.
#
# Single-worker proof: every event records pthread_self; ALL recorded
# identities must equal the driver's own (INV-5 worker affinity; no OS
# threads are ever created).
#
# Scratch is HEAP-backed (malloc): std.memory.stack_allocation carves from
# THIS frame, and values that escape across calls (the userdata channel) are
# not protected from later frame reuse.
#
# AOT driver (mojo build + execute) per tests/run.sh.  Verdict convention:
# PASS / RED / FAIL with exit 0 / 1 / 1.
from mojito_spike import BytePtr

from runtime import Runtime, create
from task import TaskControlBlock, ResultValue
from spawn import (
    JoinHandle,
    claim_running,
    execute,
    park_commit,
    park_prepare,
    spawn,
    wake,
)


@extern("_exit")
def _iso_exit(code: Int32) abi("C"): ...


@extern("pthread_self")
def _pthread_self() abi("C") -> Int: ...


@extern("malloc")
def _c_malloc(size: Int) abi("C") -> BytePtr: ...


# --- result wrapper ----------------------------------------------------------

struct IRes(ResultValue):
    """Int result satisfying the locked ResultValue constraint."""
    var v: Int
    def __init__(out self): self.v = 0
    def __init__(out self, x: Int): self.v = x


# --- shared test context (userdata channel; no module globals) ---------------

struct Ctx(ImplicitlyCopyable, ImplicitlyDeletable):
    var cnt: UnsafePointer[Int, MutAnyOrigin]        # shared counter @bufs[0]
    var seq: UnsafePointer[Int, MutAnyOrigin]        # event codes @bufs[1..]
    var nseq: UnsafePointer[Int, MutAnyOrigin]       # seq length  @bufs[64]
    var tids: UnsafePointer[Int, MutAnyOrigin]       # per-event tid @bufs[65..]

    def __init__(out self):
        self.cnt = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)
        self.seq = self.cnt
        self.nseq = self.cnt
        self.tids = self.cnt


comptime EV_SPAWN_A = Int(1)
comptime EV_SPAWN_B = Int(2)
comptime EV_JOIN_BEGIN = Int(3)
comptime EV_A1 = Int(4)
comptime EV_A_PARK = Int(5)
comptime EV_B = Int(6)
comptime EV_A_WAKE = Int(7)
comptime EV_A2 = Int(8)
comptime EV_JOIN_END = Int(9)
comptime EV_C = Int(10)
comptime EV_E = Int(11)


def _record(mut ctx: Ctx, ev: Int):
    var n = ctx.nseq[]
    ctx.seq[n] = ev
    ctx.tids[n] = _pthread_self()
    ctx.nseq[] = n + 1


# --- task bodies (module-level defs + userdata; b2 has no captures) ----------

def a_step1(ud: BytePtr) raises -> IRes:
    var ctx = ud.bitcast[Ctx]()
    ctx[].cnt[] = ctx[].cnt[] + 1
    _record(ctx[], EV_A1)
    return IRes(ctx[].cnt[])


def a_step2(ud: BytePtr) raises -> IRes:
    var ctx = ud.bitcast[Ctx]()
    ctx[].cnt[] = ctx[].cnt[] + 10
    _record(ctx[], EV_A2)
    return IRes(ctx[].cnt[])


def b_step(ud: BytePtr) raises -> IRes:
    var ctx = ud.bitcast[Ctx]()
    ctx[].cnt[] = ctx[].cnt[] + 100
    _record(ctx[], EV_B)
    return IRes(ctx[].cnt[])


def c_step(ud: BytePtr) raises -> IRes:
    # A0-T5 child: runs straight to completion; never parks.
    var ctx = ud.bitcast[Ctx]()
    _record(ctx[], EV_C)
    return IRes(777)


def e_step(ud: BytePtr) raises -> IRes:
    # A0-T8 child: fails; the message must survive intact through join().
    var ctx = ud.bitcast[Ctx]()
    _record(ctx[], EV_E)
    raise Error("disk-on-fire: E-child boom")


# --- driver -------------------------------------------------------------------

def main() raises:
    var failures = List[String]()
    var bufs = _c_malloc(96 * 8).bitcast[Int]()
    var ctx_cell = _c_malloc(32).bitcast[Ctx]()
    ctx_cell[] = Ctx()
    ctx_cell[].cnt = bufs
    ctx_cell[].seq = bufs + 1
    ctx_cell[].nseq = bufs + 64
    ctx_cell[].tids = bufs + 65
    bufs[0] = 0
    bufs[64] = 0
    var ud = ctx_cell.bitcast[Byte]()
    var ctx = ctx_cell[]

    var rt = create()

    var tcb_a = TaskControlBlock[IRes]()
    var h_a = JoinHandle[IRes](
        UnsafePointer[TaskControlBlock[IRes], MutAnyOrigin](unsafe_from_address=1), 0
    )
    # ---- RED detection + T4 registration ------------------------------------
    try:
        h_a = spawn(rt, UnsafePointer[TaskControlBlock[IRes], MutAnyOrigin](to=tcb_a), 0)
    except e:
        var msg = String(e)
        if "not implemented" in msg:
            print("T6 join semantics: RED (A0.6 surface not implemented)")
            _iso_exit(1)
        print("T6 join semantics: FAIL (unexpected spawn error: " + msg + ")")
        _iso_exit(1)
    _record(ctx, EV_SPAWN_A)

    var tcb_b = TaskControlBlock[IRes]()
    var h_b = spawn(rt, UnsafePointer[TaskControlBlock[IRes], MutAnyOrigin](to=tcb_b), 0)
    _record(ctx, EV_SPAWN_B)

    if rt.pending() != 2:
        failures.append("expected 2 runnable records after two spawns")

    # ==== A0-T4: join BEFORE completion ======================================
    _record(ctx, EV_JOIN_BEGIN)
    h_a.begin_join()

    # Branch-mutation flags live on the heap: Bool locals mutated inside
    # deeply nested loop/else blocks did not persist across iterations under
    # this b2 AOT backend (heap cells are immune; see PR notes).
    var a_parked_cell = bufs + 80
    var woke_a_cell = bufs + 81
    bufs[80] = 0
    bufs[81] = 0
    while not h_a.is_completed():
        if rt.has_ready():
            var rec = rt.pop_ready()
            if rec.task_id == h_a.id():
                if a_parked_cell[] == 0:
                    # A's first slice runs, then A parks mid-life (it still
                    # has work left — a_step2 — hence "join before completion").
                    claim_running(h_a)
                    _ = a_step1(ud)
                    park_prepare(h_a)
                    park_commit(h_a)
                    _record(ctx, EV_A_PARK)
                    a_parked_cell[] = 1
                else:
                    _ = execute(h_a, a_step2, ud)
            elif rec.task_id == h_b.id():
                _ = execute(h_b, b_step, ud)
            else:
                failures.append("dequeued unknown task id")
        else:
            # Worker idle with the join target unfinished: deliver readiness
            # to A (its blocking dependency resolved when B completed).
            if a_parked_cell[] == 0:
                failures.append("scheduler idle but A never parked")
                break
            wake(rt, h_a)
            _record(ctx, EV_A_WAKE)
            woke_a_cell[] = 1

    if woke_a_cell[] == 0:
        failures.append("A was never woken (worker reuse did not happen)")
    var res_a = h_a.finish_join()
    _record(ctx, EV_JOIN_END)
    if res_a.v != 111:
        failures.append(
            "A0-T4 counter ordering broken: expected 111 (1+100+10), got "
            + String(res_a.v)
        )

    # Exact scheduling order proves sibling progress DURING the join window:
    # expected = SPAWN_A SPAWN_B JOIN_BEGIN A1 A_PARK B A_WAKE A2 JOIN_END
    comptime N_EXPECTED = Int(9)
    if ctx.nseq[] < N_EXPECTED:
        failures.append("sequence log shorter than expected")
    else:
        for i in range(N_EXPECTED):
            var want_ev = EV_SPAWN_A
            if i == 1:
                want_ev = EV_SPAWN_B
            elif i == 2:
                want_ev = EV_JOIN_BEGIN
            elif i == 3:
                want_ev = EV_A1
            elif i == 4:
                want_ev = EV_A_PARK
            elif i == 5:
                want_ev = EV_B
            elif i == 6:
                want_ev = EV_A_WAKE
            elif i == 7:
                want_ev = EV_A2
            elif i == 8:
                want_ev = EV_JOIN_END
            if ctx.seq[i] != want_ev:
                failures.append(
                    "sequence mismatch at "
                    + String(i)
                    + ": got "
                    + String(ctx.seq[i])
                    + " want "
                    + String(want_ev)
                )
    if rt.enqueued() != 3:
        failures.append(
            "expected 3 enqueue events (2 spawns + 1 wake), got "
            + String(rt.enqueued())
        )

    # ==== single-worker identity (INV-5) =====================================
    var my_tid = _pthread_self()
    for i in range(ctx.nseq[]):
        if ctx.tids[i] != my_tid:
            failures.append("event " + String(i) + " ran on another thread")

    # ==== A0-T5: join AFTER completion — no parking ==========================
    var tcb_c = TaskControlBlock[IRes]()
    var h_c = spawn(rt, UnsafePointer[TaskControlBlock[IRes], MutAnyOrigin](to=tcb_c), 0)
    _ = execute(h_c, c_step, ud)
    if not h_c.is_completed():
        failures.append("A0-T5: child not completed before join")
    h_c.begin_join()
    var res_c = h_c.finish_join()
    if res_c.v != 777:
        failures.append("A0-T5: result not moved out intact")
    # Double join must be rejected even on the fast path (also A0-T6 evidence).
    try:
        _ = h_c.join()
        failures.append("A0-T5: double join was NOT rejected")
    except e:
        var m = String(e)
        if not ("double join" in m):
            failures.append("A0-T5: double-join error message unexpected: " + m)

    # ==== A0-T8: child error propagates through join =========================
    var tcb_e = TaskControlBlock[IRes]()
    var h_e = spawn(rt, UnsafePointer[TaskControlBlock[IRes], MutAnyOrigin](to=tcb_e), 0)
    _ = execute(h_e, e_step, ud)
    if not h_e.is_completed():
        failures.append("A0-T8: failed child did not reach COMPLETED")
    if not h_e.is_failed():
        failures.append("A0-T8: failure flag missing")
    h_e.begin_join()
    try:
        _ = h_e.finish_join()
        failures.append("A0-T8: join on failing child did NOT raise")
    except e:
        var m = String(e)
        if not ("disk-on-fire" in m):
            failures.append("A0-T8: child error message NOT preserved: " + m)

    rt.shutdown()

    if len(failures) == 0:
        print("T6 join semantics: PASS")
    else:
        print("T6 join semantics: FAIL (" + String(len(failures)) + ")")
        for m in failures:
            print("  - " + m)
        _iso_exit(1)
