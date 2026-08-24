# spike/colorless_runtime/tests/t2_worker_reuse_aot.mojo
#
# A0.6/A0-T2 (issue #15) — while task A is PARKED (mid-frame, on its own
# synthetic stack), task B executes ON THE SAME WORKER; A later resumes at
# the EXACT suspension point.
#
# This is the exact-resume FIBER half of the A0.6 proof (the cooperative/
# protocol half lives in t6_join_semantics_aot.mojo).  All raw context
# switching is INLINE in this driver module — local ms_ctx save areas +
# direct ms_ctx_make / ms_ctx_switch over @export abi("C") entries reached
# through the S0 entry_pointer mechanism — because modular/modular#6971
# miscompiles imported-module extern calls under JIT AND AOT (the proven
# tests/t4_fiber.mojo shape).  ALL scheduling bookkeeping (runnable queue,
# NEW->RUNNABLE->RUNNING->PARKING->WAITING->RUNNABLE->RUNNING->COMPLETED,
# generation-bumped park commit, wake/re-enqueue, one-shot join) goes through
# the pure-Mojo library modules, which are extern-free by design.
#
# Single-thread proof WITHOUT TLS reads: libsystem TLS access (_pthread_self)
# faults when called on a synthetic stack, so thread identity is proven by
# STACK IDENTITY instead — each entry records the address of its own frame
# local, and the driver asserts both addresses lie INSIDE the corresponding
# ms_stack_alloc'd region.  A brand-new OS thread would have its own default
# stack, never these synthetic stacks; combined with main's unchanged
# pthread_self across the whole flow this proves worker reuse with zero
# OS-thread creation (INV-5).
#
# Assertions (A0-T2):
#   - interleaving: SPAWN_A < SPAWN_B < A.start < A.park < B.start < B.done
#     < A.wake < A.resumed < A.done — B ran entirely inside A's park window;
#   - exact resume: a frame-local address inside A is identical before the
#     suspend and after the resume, and its payload survived (t4 evidence);
#   - lifecycle: both TCBs reach COMPLETED; fast-path joins succeed; double
#     join rejected; enqueue count == 3 (two spawns + one wake).
#
# Scratch cells are HEAP-backed (malloc): std.memory.stack_allocation carves
# from THIS frame, and values that escape across calls are not protected from
# later frame reuse (observed corruption; see PR notes).
#
# AOT driver ONLY (mojo build + execute).  Verdict convention:
# PASS / RED / FAIL with exit 0 / 1 / 1.
from mojito_spike import (
    BytePtr,
    MS_CTX_SIZE,
    entry_pointer,
    ms_ctx_make,
    ms_ctx_switch,
    ms_page_size,
    ms_stack_alloc,
    ms_stack_free,
)

from runtime import Nil, Runtime, create
from task import TaskControlBlock
from spawn import (
    JoinHandle,
    claim_running,
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


def _heap[T: AnyType](nbytes: Int) -> UnsafePointer[T, MutUntrackedOrigin]:
    """Stable scratch (malloc-backed)."""
    return UnsafePointer[T, MutUntrackedOrigin](
        unsafe_from_address=Int(_c_malloc(nbytes))
    )


# --- event log ---------------------------------------------------------------

comptime EV_SPAWN_A = Int(1)
comptime EV_SPAWN_B = Int(2)
comptime EV_A_START = Int(3)
comptime EV_A_PARK = Int(4)
comptime EV_B_START = Int(5)
comptime EV_B_DONE = Int(6)
comptime EV_A_WAKE = Int(7)
comptime EV_A_RESUMED = Int(8)
comptime EV_A_DONE = Int(9)


struct Ctx(ImplicitlyCopyable, ImplicitlyDeletable):
    """Shared scheduling context (userdata channel; no module globals).
    13 pointer/Int fields == 104 bytes."""

    var seq: UnsafePointer[Int, MutUntrackedOrigin]      # bufs[0..]
    var nseq: UnsafePointer[Int, MutUntrackedOrigin]     # bufs[32]
    var tcb_a_addr: Int                                  # bufs[33]
    var id_a: Int                                        # bufs[34]
    var tcb_b_addr: Int                                  # bufs[35]
    var id_b: Int                                        # bufs[36]
    var prep_a: UnsafePointer[Int, MutUntrackedOrigin]   # bufs[37]
    var prep_b: UnsafePointer[Int, MutUntrackedOrigin]   # bufs[38]
    var a_local_addr: UnsafePointer[Int, MutUntrackedOrigin]  # bufs[39]
    var resume_ok: UnsafePointer[Int, MutUntrackedOrigin]     # bufs[40]
    var anchor_a: UnsafePointer[Int, MutUntrackedOrigin]      # bufs[41]
    var anchor_b: UnsafePointer[Int, MutUntrackedOrigin]      # bufs[42]
    var pad: Int                                         # bufs[43]

    def __init__(out self):
        self.seq = UnsafePointer[Int, MutUntrackedOrigin](unsafe_from_address=1)
        self.nseq = self.seq
        self.tcb_a_addr = 0
        self.id_a = 0
        self.tcb_b_addr = 0
        self.id_b = 0
        self.prep_a = self.seq
        self.prep_b = self.seq
        self.a_local_addr = self.seq
        self.resume_ok = self.seq
        self.anchor_a = self.seq
        self.anchor_b = self.seq
        self.pad = 0


def _record(mut ctx: Ctx, ev: Int):
    var n = ctx.nseq[]
    ctx.seq[n] = ev
    ctx.nseq[] = n + 1


# --- fiber sidecar (S0 demo shape: self_ctx / caller_ctx / user) -------------

struct T2Frame(ImplicitlyCopyable, ImplicitlyDeletable):
    var self_ctx: BytePtr
    var caller_ctx: BytePtr
    var user: BytePtr

    def __init__(out self, ac: BytePtr, mc: BytePtr):
        self.self_ctx = ac
        self.caller_ctx = mc
        self.user = UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=1)


def _handle_a(ctx: Ctx) -> JoinHandle[Nil]:
    return JoinHandle[Nil](
        UnsafePointer[TaskControlBlock[Nil], MutAnyOrigin](
            unsafe_from_address=ctx.tcb_a_addr
        ),
        ctx.id_a,
    )


# --- task A: parks MID-FRAME on its synthetic stack --------------------------

@export("t2_entry_a")
def t2_entry_a(ud: BytePtr) abi("C"):
    var fr = ud.bitcast[T2Frame]()
    var ctx = fr[].user.bitcast[Ctx]()

    # A stack-local whose ADDRESS must survive the suspend/resume round trip
    # (exact-resume evidence, A0-T3 style).
    var anchor: Int = 0x5EED
    var ap = UnsafePointer[Int, MutAnyOrigin](to=anchor)
    ctx[].a_local_addr[] = Int(ap)

    _record(ctx[], EV_A_START)
    var h = _handle_a(ctx[])
    # (entries are abi("C") and therefore non-raising; guard every transition)
    try:
        park_prepare(h)    # RUNNING -> PARKING
        park_commit(h)     # PARKING -> WAITING (fresh wait epoch)
    except e:
        pass
    _record(ctx[], EV_A_PARK)

    # ---- SUSPEND: switch away; the worker is free the moment this returns --
    ms_ctx_switch(fr[].self_ctx, fr[].caller_ctx)

    # ---- EXACT RESUME POINT: same frame, same stack, same local ------------
    if ctx[].a_local_addr[] != Int(ap):
        ctx[].resume_ok[] = 0
    if anchor != 0x5EED:
        ctx[].resume_ok[] = 0
    _record(ctx[], EV_A_RESUMED)
    # The driver claimed RUNNING before resuming us; finish the task with a
    # unit result (Nil), mirroring what execute() does for fiber-free tasks.
    try:
        h.tcb()[].transition(TaskControlBlock.COMPLETED)
        h.tcb()[].mark_result(Nil())
    except e:
        pass
    _record(ctx[], EV_A_DONE)


# --- task B: runs to completion inside A's park window ------------------------

@export("t2_entry_b")
def t2_entry_b(ud: BytePtr) abi("C"):
    var fr = ud.bitcast[T2Frame]()
    var ctx = fr[].user.bitcast[Ctx]()
    var anchor: Int = 0xBEEF
    var ap = UnsafePointer[Int, MutAnyOrigin](to=anchor)
    ctx[].anchor_b[] = Int(ap)
    _record(ctx[], EV_B_START)
    var h = JoinHandle[Nil](
        UnsafePointer[TaskControlBlock[Nil], MutAnyOrigin](
            unsafe_from_address=ctx[].tcb_b_addr
        ),
        ctx[].id_b,
    )
    # Driver claimed RUNNING before entering us; run to completion.
    try:
        h.tcb()[].transition(TaskControlBlock.COMPLETED)
        h.tcb()[].mark_result(Nil())
    except e:
        pass
    _record(ctx[], EV_B_DONE)


# --- driver / scheduler loop ---------------------------------------------------

def main() raises:
    var failures = List[String]()
    var bufs = _heap[Int](96 * 8)
    var ctx_cell = _heap[Ctx](128)
    ctx_cell[] = Ctx()
    ctx_cell[].seq = bufs
    ctx_cell[].nseq = bufs + 32
    ctx_cell[].prep_a = bufs + 37
    ctx_cell[].prep_b = bufs + 38
    ctx_cell[].a_local_addr = bufs + 39
    ctx_cell[].resume_ok = bufs + 40
    ctx_cell[].anchor_a = bufs + 41
    ctx_cell[].anchor_b = bufs + 42
    bufs[32] = 0
    bufs[37] = 0
    bufs[38] = 0
    bufs[40] = 1
    var ctx = ctx_cell[]
    var ud = ctx_cell.bitcast[Byte]()

    var rt = create()

    # ---- RED detection + registration ---------------------------------------
    var tcb_a = TaskControlBlock[Nil]()
    var h_a = JoinHandle[Nil](
        UnsafePointer[TaskControlBlock[Nil], MutAnyOrigin](unsafe_from_address=1), 0
    )
    try:
        h_a = spawn(rt, UnsafePointer[TaskControlBlock[Nil], MutAnyOrigin](to=tcb_a), 0)
    except e:
        var msg = String(e)
        if "not implemented" in msg:
            print("T2 worker reuse: RED (A0.6 surface not implemented)")
            _iso_exit(1)
        print("T2 worker reuse: FAIL (unexpected spawn error: " + msg + ")")
        _iso_exit(1)
    _record(ctx, EV_SPAWN_A)

    var tcb_b = TaskControlBlock[Nil]()
    var h_b = spawn(rt, UnsafePointer[TaskControlBlock[Nil], MutAnyOrigin](to=tcb_b), 0)
    _record(ctx, EV_SPAWN_B)

    # NOTE: `ctx` above is a VALUE COPY of the cell — publish the task
    # coordinates through the CELL itself (the fibers reach this exact cell
    # via fs[].user), never through the copy.
    var pa = UnsafePointer[TaskControlBlock[Nil], MutAnyOrigin](to=tcb_a)
    var pb = UnsafePointer[TaskControlBlock[Nil], MutAnyOrigin](to=tcb_b)
    ctx_cell[].tcb_a_addr = Int(pa)
    ctx_cell[].id_a = h_a.id()
    ctx_cell[].tcb_b_addr = Int(pb)
    ctx_cell[].id_b = h_b.id()

    # ---- synthetic stacks + save areas (driver-owned, like t4_fiber) --------
    var ps = Int(ms_page_size())
    var stack_bytes = 4 * ps
    var slots_a = _heap[BytePtr](16)
    var slots_b = _heap[BytePtr](16)
    if ms_stack_alloc(stack_bytes, slots_a, slots_a + 1) != 0:
        failures.append("stack alloc failed for A")
    if ms_stack_alloc(stack_bytes, slots_b, slots_b + 1) != 0:
        failures.append("stack alloc failed for B")

    var main_buf = _heap[Int](MS_CTX_SIZE)
    var alt_abuf = _heap[Int](MS_CTX_SIZE)
    var alt_bbuf = _heap[Int](MS_CTX_SIZE)
    var main_ctx_p = main_buf.bitcast[Byte]()
    var alt_a_p = alt_abuf.bitcast[Byte]()
    var alt_b_p = alt_bbuf.bitcast[Byte]()

    var fs_a = _heap[T2Frame](32)
    var fs_b = _heap[T2Frame](32)
    fs_a[] = T2Frame(alt_a_p, main_ctx_p)
    fs_a[].user = ud
    fs_b[] = T2Frame(alt_b_p, main_ctx_p)
    fs_b[].user = ud

    var tid_before = _pthread_self()

    # ---- schedule: FIFO drive until every event fired ------------------------
    var ep_a = entry_pointer["t2_entry_a"]()
    var ep_b = entry_pointer["t2_entry_b"]()
    var guard = 0
    while ctx.nseq[] < 9:
        guard += 1
        if guard > 20:
            failures.append("scheduler loop did not converge")
            break
        if rt.has_ready():
            var rec = rt.pop_ready()
            if rec.task_id == h_a.id():
                claim_running(h_a)
                if ctx.prep_a[] == 0:
                    ctx.prep_a[] = 1
                    ms_ctx_make(
                        alt_a_p,
                        (slots_a + 1)[],
                        ep_a,
                        fs_a.bitcast[Byte](),
                    )
                ms_ctx_switch(main_ctx_p, alt_a_p)
            elif rec.task_id == h_b.id():
                claim_running(h_b)
                if ctx.prep_b[] == 0:
                    ctx.prep_b[] = 1
                    ms_ctx_make(
                        alt_b_p,
                        (slots_b + 1)[],
                        ep_b,
                        fs_b.bitcast[Byte](),
                    )
                ms_ctx_switch(main_ctx_p, alt_b_p)
            else:
                failures.append("dequeued unknown task id")
        else:
            # Worker idle: deliver readiness to the parked task (its blocking
            # condition is satisfied by construction in this scenario).
            if h_a.state() == TaskControlBlock.WAITING:
                wake(rt, h_a)
                _record(ctx, EV_A_WAKE)

    var tid_after = _pthread_self()

    # ---- assertions ----------------------------------------------------------
    if ctx.nseq[] != 9:
        failures.append("expected exactly 9 events, got " + String(ctx.nseq[]))
    else:
        for i in range(9):
            var want_ev = EV_SPAWN_A
            if i == 1:
                want_ev = EV_SPAWN_B
            elif i == 2:
                want_ev = EV_A_START
            elif i == 3:
                want_ev = EV_A_PARK
            elif i == 4:
                want_ev = EV_B_START
            elif i == 5:
                want_ev = EV_B_DONE
            elif i == 6:
                want_ev = EV_A_WAKE
            elif i == 7:
                want_ev = EV_A_RESUMED
            elif i == 8:
                want_ev = EV_A_DONE
            if ctx.seq[i] != want_ev:
                failures.append(
                    "sequence mismatch at "
                    + String(i)
                    + ": got "
                    + String(ctx.seq[i])
                    + " want "
                    + String(want_ev)
                )

    # Single-worker identity: main's OS thread never changed AND both tasks
    # executed on THEIR synthetic stacks (a new OS thread would have gotten a
    # fresh default stack, never an ms_stack_alloc region).
    if tid_before != tid_after:
        failures.append("main OS thread identity changed mid-run")
    var base_a = Int(slots_a[])
    var top_a = Int((slots_a + 1)[])
    var base_b = Int(slots_b[])
    var top_b = Int((slots_b + 1)[])
    # A's frame-local address doubles as its stack anchor (recorded at entry)
    var anch_a = bufs[39]
    var anch_b = bufs[42]
    if not ((anch_a >= base_a) and (anch_a < top_a)):
        failures.append("A did not execute on its synthetic stack")
    if not ((anch_b >= base_b) and (anch_b < top_b)):
        failures.append("B did not execute on its synthetic stack")

    # Exact-resume evidence:
    if ctx.resume_ok[] != 1:
        failures.append("A did not resume at the exact point (frame corrupted)")

    # Lifecycle: both completed; fast-path joins; double-join rejected.
    if not h_a.is_completed():
        failures.append("A did not reach COMPLETED")
    if not h_b.is_completed():
        failures.append("B did not reach COMPLETED")
    _ = h_a.join()
    _ = h_b.join()
    try:
        _ = h_a.join()
        failures.append("double join was NOT rejected")
    except e:
        var m = String(e)
        if not ("double join" in m):
            failures.append("double-join error message unexpected: " + m)

    if rt.enqueued() != 3:
        failures.append(
            "expected 3 enqueue events (2 spawns + 1 wake), got "
            + String(rt.enqueued())
        )
    if rt.tasks_started() != 0:
        failures.append("root counter moved without a root run")

    ms_stack_free(slots_a[])
    ms_stack_free(slots_b[])
    rt.shutdown()

    if len(failures) == 0:
        print("T2 worker reuse: PASS")
    else:
        print("T2 worker reuse: FAIL (" + String(len(failures)) + ")")
        for m in failures:
            print("  - " + m)
        _iso_exit(1)
