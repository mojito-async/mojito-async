# mojito_async/test/unit/t25_fiber_affinity_aot.mojo
#
# A1.3 (issue #51) — worker-affine started-fiber policy (ADR-006 / ADR-007).
#
# TDD acceptance driver: RED first (compiles only once the A1.3 policy
# surface lands), then green.  Carries the spike's t2 worker-reuse proof onto
# the A1 fiber seam (issue #49) and asserts the per-worker affinity contract
# that EPIC #2's M:N worker pool consumes (E5 started-fiber remote-ready):
#
#   - spec §14.1 `started`: is_started() flips exactly at FIRST BODY ENTRY
#     (not at construction);
#   - owner_worker pinning (spec §19.2 / ADR-006): an unstarted fiber has NO
#     owner pinned (owner_worker() == 0, even after set_owner()); once
#     STARTED the owner is IMMUTABLE — set_owner(foreign) raises and
#     owner_worker() still returns the pinned worker;
#   - wake routing seam (spec §19.2, EPIC #2 E5): for the single worker a
#     started fiber's wake target resolves to the sole worker's queue (the
#     owner; intra-worker wake stays on the FIFO, spec §88 — today's
#     behavior), and a FOREIGN waker routes to the OWNER worker's
#     remote-ready queue — never the general stealable set.  Routing is
#     stable across the park/resume;
#   - ADR-007 live-stack-locality: the fiber's frame-local address recorded
#     at first entry lies INSIDE the ms_stack_alloc'd region and is
#     byte-identical after the park/resume round trip (no relocation); the
#     stack_ptr() reservation is unchanged across the suspend; the switch
#     helpers run assert_never_relocated() (fault-enabled builds);
#   - worker reuse inside the park window (t2 carry-over, single-thread proof
#     WITHOUT TLS reads — b2 has no TLS, so identity is threaded explicitly):
#     a second task (fiber B) runs to completion on the SAME worker while A
#     is parked (event order A_ENTER < A_PARK < B_ENTER < B_DONE <
#     A_RESUMED < A_DONE); the worker's NATIVE stack depth differs across the
#     two switches (markers captured at different call depths) while A's
#     frame-local stays identical — the fiber, not the native frame, held the
#     task.
#
# EXTERN DISCIPLINE (modular/modular#6971): the driver imports only the
# extern-free fiber transition methods (resume/suspend) plus the vendor
# firewall defs it needs (ms_stack_alloc/ms_page_size/ms_ctx_switch/c_malloc
# etc.), so it MUST be AOT (`mojo build` + execute; *_aot.mojo pattern —
# picked up by test/run.sh) exactly like t23_fiber_aot.
#
# Verdict: exit 0 + "PASS"; any failure prints FAIL + raises (exit 1).
from mojito_async.vendor.mojito_sys import (
    BytePtr,
    NativeStack,
    c_free,
    c_malloc,
    entry_pointer,
    ms_ctx_switch,
    ms_page_size,
    ms_stack_alloc,
)
from mojito_async.fiber.fiber import Fiber, FiberFrame, make_fiber
from mojito_async.runtime.scheduler import wake_target_worker


@extern("_exit")
def _iso_exit(code: Int32) abi("C"): ...


# Explicit worker identity (b2 has no TLS): this driver IS the sole worker.
comptime WORKER_SELF = Int(1)       # the sole worker's own id
comptime WORKER_FOREIGN = Int(2)    # a hypothetical second worker (foreign wake)
comptime STACK_PAGES = 4
comptime ALIGN_MASK = 15

# Event log slots (t2 carry-over: schedule interleaving is observable).
comptime EV_A_ENTER = Int(1)
comptime EV_A_PARK = Int(2)
comptime EV_B_ENTER = Int(3)
comptime EV_B_DONE = Int(4)
comptime EV_A_RESUMED = Int(5)
comptime EV_A_DONE = Int(6)
comptime N_EVENTS = Int(6)


# Heap-backed payload (t2 lesson: values that escape across calls are not
# protected against later frame reuse when stack-carved; the fibers reach
# these cells via the FiberFrame `user` side channel).  One cell per fiber;
# the event log cells are SHARED.
struct T25Payload(ImplicitlyCopyable, ImplicitlyDeletable):
    var log: UnsafePointer[Int, MutUntrackedOrigin]   # shared event log
    var n: UnsafePointer[Int, MutUntrackedOrigin]     # log slot count
    var marker: Int             # frame-local address recorded at first entry
    var phase: Int              # 0 fresh / 1 entered / 3 resumed / 4 done
    var resume_ok: Bool         # exact-resume evidence (ADR-007)

    def __init__(out self):
        self.log = UnsafePointer[Int, MutUntrackedOrigin](unsafe_from_address=1)
        self.n = UnsafePointer[Int, MutUntrackedOrigin](unsafe_from_address=1)
        self.marker = 0
        self.phase = 0
        self.resume_ok = True

    def __init__(
        out self,
        log: UnsafePointer[Int, MutUntrackedOrigin],
        n: UnsafePointer[Int, MutUntrackedOrigin],
    ):
        self.log = log
        self.n = n
        self.marker = 0
        self.phase = 0
        self.resume_ok = True


def _log(mut pl: T25Payload, ev: Int):
    var i = pl.n[]
    pl.log[i] = ev
    pl.n[] = i + 1

def _expect_ev(i: Int) -> Int:
    """Expected event at log slot i (t2 carry-over interleaving)."""
    if i == 0:
        return EV_A_ENTER
    if i == 1:
        return EV_A_PARK
    if i == 2:
        return EV_B_ENTER
    if i == 3:
        return EV_B_DONE
    if i == 4:
        return EV_A_RESUMED
    return EV_A_DONE


# --- task A: enters, parks MID-FRAME on its synthetic stack, resumes --------

@export("t25_entry_a")
def t25_entry_a(ud: BytePtr) abi("C"):
    var fr = ud.bitcast[FiberFrame]()
    var pl = fr[].user.bitcast[T25Payload]()

    # A stack-local whose ADDRESS must survive the suspend/resume round trip
    # (the exact-resume / ADR-007 evidence; t23/t4 shape).
    var local: Int = 0
    var local_p = UnsafePointer[Int, MutAnyOrigin](to=local)
    if pl[].phase == 0:
        pl[].marker = Int(local_p)
        pl[].phase = 1
        _log(pl[], EV_A_ENTER)
        _log(pl[], EV_A_PARK)
        # ---- SUSPEND: switch away; the worker is free the moment this
        # ---- returns (A is parked; B may run on this same worker now).
        ms_ctx_switch(fr[].self_ctx, fr[].caller_ctx)

        # ---- EXACT RESUME POINT: same frame, same synthetic stack --------
        if pl[].marker != Int(local_p):
            pl[].resume_ok = False
        pl[].phase = 3
        _log(pl[], EV_A_RESUMED)
    pl[].phase = 4
    _log(pl[], EV_A_DONE)


# --- task B: runs to COMPLETION inside A's park window (worker reuse) ------

@export("t25_entry_b")
def t25_entry_b(ud: BytePtr) abi("C"):
    var fr = ud.bitcast[FiberFrame]()
    var pl = fr[].user.bitcast[T25Payload]()
    var local: Int = 0
    var local_p = UnsafePointer[Int, MutAnyOrigin](to=local)
    pl[].marker = Int(local_p)
    _log(pl[], EV_B_ENTER)
    # B runs to completion on the same worker while A is parked.
    _log(pl[], EV_B_DONE)


# --- native-stack markers at DIFFERENT call depths --------------------------
# The worker's native stack pointer changes across the switches (the drive
# did real work — B ran — between them; the markers are captured at
# different depths), while A's frame-local on the SYNTHETIC stack stays
# identical: the task state lived in the fiber, not in the native frame.

def _native_marker() -> Int:
    """Address of a frame-local in THIS frame (native worker stack)."""
    var slot: Int = 0
    return Int(UnsafePointer[Int, MutAnyOrigin](to=slot))


def _resume_marked(
    mut f: Fiber,
    depth: Int,
    cell: UnsafePointer[Int, MutUntrackedOrigin],
) raises:
    """Build `depth` live nested native frames, record the native marker at
    the innermost point, then resume `f`.  Recursion keeps every level's
    frame genuinely live on the native stack (recursion cannot be inlined
    away), so a marker captured at depth 0 and one at depth 4 sit at
    GUARANTEED distinct native addresses.  (The driver asserts the two
    markers differ; if a future compiler tail-call-eliminated the recursion
    the proof would visibly break instead of silently passing.)"""
    if depth == 0:
        cell[0] = _native_marker()
        f.resume()
        return
    _resume_marked(f, depth - 1, cell)


comptime NATIVE_DEPTH_SHALLOW = Int(0)
comptime NATIVE_DEPTH_DEEP = Int(4)


def main() raises:
    var failures = List[String]()

    # --- heap-backed shared scratch (t2 shape) ------------------------------
    var log = UnsafePointer[Int, MutUntrackedOrigin](
        unsafe_from_address=Int(c_malloc((N_EVENTS + 2) * 8))
    )
    var ncell = UnsafePointer[Int, MutUntrackedOrigin](
        unsafe_from_address=Int(c_malloc(8))
    )
    var pa = UnsafePointer[T25Payload, MutUntrackedOrigin](
        unsafe_from_address=Int(c_malloc(64))
    )
    var pb = UnsafePointer[T25Payload, MutUntrackedOrigin](
        unsafe_from_address=Int(c_malloc(64))
    )
    var npre = UnsafePointer[Int, MutUntrackedOrigin](
        unsafe_from_address=Int(c_malloc(8))
    )
    var ndeep = UnsafePointer[Int, MutUntrackedOrigin](
        unsafe_from_address=Int(c_malloc(8))
    )
    ncell[0] = 0
    pa[0] = T25Payload(log, ncell)
    pb[0] = T25Payload(log, ncell)

    var ps = Int(ms_page_size())
    if ps <= 0:
        failures.append("ms_page_size non-positive")
    var stack_bytes = STACK_PAGES * ps

    # --- acquire synthetic stacks (pool-seam shape, issue #52) --------------
    var slots_a = UnsafePointer[BytePtr, MutUntrackedOrigin](
        unsafe_from_address=Int(c_malloc(16))
    )
    var slots_b = UnsafePointer[BytePtr, MutUntrackedOrigin](
        unsafe_from_address=Int(c_malloc(16))
    )
    if ms_stack_alloc(stack_bytes, slots_a, slots_a + 1) != 0:
        failures.append("ms_stack_alloc failed for A")
    if ms_stack_alloc(stack_bytes, slots_b, slots_b + 1) != 0:
        failures.append("ms_stack_alloc failed for B")
    var ns_a = NativeStack(slots_a[0], (slots_a + 1)[])
    var ns_b = NativeStack(slots_b[0], (slots_b + 1)[])
    var base_a = Int(ns_a.base)
    var top_a = Int(ns_a.top)
    var base_b = Int(ns_b.base)
    var top_b = Int(ns_b.top)
    if (top_a & ALIGN_MASK) != 0 or (top_b & ALIGN_MASK) != 0:
        failures.append("stack top not 16-aligned")

    # --- bind both fibers (batch Fiber seam, issue #49) ---------------------
    var f_a = make_fiber(
        UnsafePointer[NativeStack, MutAnyOrigin](to=ns_a),
        entry_pointer["t25_entry_a"](),
        pa.bitcast[Byte](),
    )
    var f_b = make_fiber(
        UnsafePointer[NativeStack, MutAnyOrigin](to=ns_b),
        entry_pointer["t25_entry_b"](),
        pb.bitcast[Byte](),
    )

    # --- A1.3 policy: unstarted fiber has no owner pinned -------------------
    if f_a.is_started():
        failures.append("is_started before first body entry")
    if f_a.owner_worker() != 0:
        failures.append("unstarted fiber reports a pinned owner")
    try:
        f_a.assert_never_relocated()
    except e:
        failures.append("assert_never_relocated raised on a bound fiber")

    # Pin the sole worker as owner BEFORE first entry (the EPIC #2 pool seam
    # call); observable policy keeps the owner hidden until STARTED.
    f_a.set_owner(WORKER_SELF)
    if f_a.owner_worker() != 0:
        failures.append("owner must not be observable before first entry")

    # Wake routing while unpinned: falls back to the local worker's queue.
    if wake_target_worker(f_a.owner_worker(), WORKER_SELF) != WORKER_SELF:
        failures.append("unpinned wake must stay on the local queue")

    # --- resume #1: FIRST BODY ENTRY — STARTED + owner pinned here ----------
    _resume_marked(f_a, NATIVE_DEPTH_SHALLOW, npre)

    if not f_a.is_started():
        failures.append("fiber not started after first body entry")
    if f_a.owner_worker() != WORKER_SELF:
        failures.append("owner worker not pinned at first body entry")
    if pa[].phase != 1:
        failures.append("entry A did not reach the park point (phase "
                        + String(pa[].phase) + ")")
    # ADR-007: the live stack reservation is unchanged across the entry.
    var sp_a1 = f_a.stack_ptr()
    if Int(sp_a1.base) != base_a or Int(sp_a1.top) != top_a:
        failures.append("stack reservation changed across first entry")

    # --- owner IMMUTABILITY once STARTED (ADR-006) --------------------------
    var rejected = False
    try:
        f_a.set_owner(WORKER_FOREIGN)
    except e:
        rejected = True
    if not rejected:
        failures.append("set_owner on a started fiber did NOT raise")
    if f_a.owner_worker() != WORKER_SELF:
        failures.append("started fiber's owner_worker changed")

    # --- wake routing stability (spec §19.2 / EPIC #2 E5 seam) --------------
    # Intra-worker wake (owner == local worker): the sole queue (spec §88 —
    # today's single-worker behavior, preserved).
    if wake_target_worker(f_a.owner_worker(), WORKER_SELF) != WORKER_SELF:
        failures.append("intra-worker wake must target the owner's (sole) queue")
    # A FOREIGN waker must route to the OWNER worker's remote-ready queue,
    # never the general stealable set.
    if wake_target_worker(f_a.owner_worker(), WORKER_FOREIGN) != WORKER_SELF:
        failures.append("foreign wake must target the owner worker's queue")

    # --- worker reuse: B runs to completion inside A's park window ---------
    f_b.resume()
    if not f_b.is_started():
        failures.append("B not started after its single resume")
    if pb[].phase != 0:
        failures.append("B entry did not run to completion (phase "
                        + String(pb[].phase) + ")")
    if pb[].marker < base_b or pb[].marker >= top_b:
        failures.append("B did not execute on its allocated synthetic stack")

    # --- resume #2: exact resume point on the SAME live stack ---------------
    _resume_marked(f_a, NATIVE_DEPTH_DEEP, ndeep)

    if pa[].phase != 4:
        failures.append("entry A did not complete after resume (phase "
                        + String(pa[].phase) + ")")
    if not pa[].resume_ok:
        failures.append("exact resume point lost across park/resume")
    if pa[].marker < base_a or pa[].marker >= top_a:
        failures.append("A did not execute on its allocated synthetic stack")
    if not f_a.is_started():
        failures.append("fiber lost started after resume")
    if f_a.owner_worker() != WORKER_SELF:
        failures.append("started fiber's owner_worker changed across park/resume")
    # Direct ADR-007 assertion (also run by the switch helpers in
    # fault-enabled builds).
    try:
        f_a.assert_never_relocated()
    except e:
        failures.append("assert_never_relocated raised on a live fiber")

    # ADR-007: the live stack address is IDENTICAL before and after the
    # suspend/resume (no relocation).
    var sp_a2 = f_a.stack_ptr()
    if Int(sp_a2.base) != base_a or Int(sp_a2.top) != top_a:
        failures.append("live stack moved across park/resume (ADR-007 violated)")

    # Native stack depth CHANGED across the switches while A's frame-local
    # stayed identical — the fiber, not the native frame, held the task.
    if npre[0] == ndeep[0]:
        failures.append("worker native stack depth did not change across the switches")

    # --- event order: B ran entirely inside A's park window -----------------
    if ncell[0] != N_EVENTS:
        failures.append("expected " + String(N_EVENTS) + " events, got "
                        + String(ncell[0]))
    else:
        for i in range(N_EVENTS):
            if log[i] != _expect_ev(i):
                failures.append("event order mismatch at " + String(i)
                                + ": got " + String(log[i]) + " want "
                                + String(_expect_ev(i)))

    # --- teardown: fibers own their reservations; destroy releases ----------
    f_a.destroy()
    f_b.destroy()
    if f_a.alive() or f_b.alive():
        failures.append("destroy left a fiber alive")

    c_free(UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=Int(log)))
    c_free(UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=Int(ncell)))
    c_free(UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=Int(pa)))
    c_free(UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=Int(pb)))
    c_free(UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=Int(npre)))
    c_free(UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=Int(ndeep)))
    c_free(UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=Int(slots_a)))
    c_free(UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=Int(slots_b)))

    if len(failures) == 0:
        print("T25 fiber affinity: PASS")
    else:
        print("T25 fiber affinity: FAIL (" + String(len(failures)) + ")")
        for m in failures:
            print("  - " + m)
        _iso_exit(1)