# mojito_async/test/stress/t27_concurrency_aot.mojo
#
# issue #101 — the A2.0 resumed-cap regression probe.
#
# RED (frozen substrate): the vendored mojito-sys bookkeeping keeps a FIXED
# 64-row process-global resume table with no eviction (aarch64_switch.S:
# _ms_resume_tab; the full path traps loudly, brk #0x67 -> SIGILL).  Every
# live fiber claims 2 rows (its own ctx + its caller ctx), so > 32 live
# fibers traps the process.  N_LIVE = 40 fibers (80 rows) exceeds the cap:
# the FIRST pass (every fiber suspended once) hits brk #0x67 at fiber #33
# and the process dies by SIGILL.  That is the RED evidence (a hard process
# trap, not an exit-1 verdict — documented in precommit/known-red.tsv).
#
# GREEN (a2/00-substrate): the resume table and _ms_last_* globals are gone;
# the return link is carried in the ctx itself (O(1), no scan, and no shared
# writable global on the switch path => thread-safe for M:N).  All 40 fibers
# then park/resume/complete with their exact frame-locals intact and the
# >32 cap is gone.
#
# Each fiber: FIRST ENTRY records a frame-local address and parks (switches
# back to the driver); on RE-ENTRY it verifies the frame-local survived the
# park/resume (exact resume / ADR-007) then returns (completion path — the
# trampoline tail-switches back to the driver).  The driver resumes all 40
# to park, resumes all 40 to completion, and asserts each resumed exactly
# its own frame.
#
# EXTERN DISCIPLINE (modular/modular#6971): only the extern-free fiber seam
# (make_fiber/resume/destroy) + vendor firewall defs; AOT (*_aot.mojo)
# exactly like t23/t25/t26.
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


@extern("_exit")
def _iso_exit(code: Int32) abi("C"): ...


comptime N_LIVE = Int(40)       # > 32: exceeds the frozen resume-table cap
comptime STACK_PAGES = 4
comptime ALIGN_MASK = 15


# One heap-backed cell per fiber, reached via the FiberFrame `user` side
# channel.  All scalars (t25 lesson: values that escape across a switch are
# not protected by stack reuse once the frame is gone — keep them heap-side).
struct T27Cell(ImplicitlyCopyable, ImplicitlyDeletable):
    var marker: Int             # frame-local addr recorded at first entry
    var resumed: Int            # exact-resume-point observations
    var ok: Int                 # 1 while every exact check held
    var base: Int               # this fiber's stack base (locality bound)
    var top: Int                # ... and top

    def __init__(out self):
        self.marker = 0
        self.resumed = 0
        self.ok = 1
        self.base = 0
        self.top = 0


# The single shared entry trampoline for every fiber (S0 demo form): record
# the frame-local at FIRST entry, park, then on re-entry verify it survived,
# and return (completion path back to the driver's resume()).
@export("t27_entry")
def t27_entry(ud: BytePtr) abi("C"):
    var fr = ud.bitcast[FiberFrame]()
    var cell = fr[].user.bitcast[T27Cell]()

    var local: Int = 0
    var local_p = UnsafePointer[Int, MutAnyOrigin](to=local)

    if cell[].marker == 0:
        cell[].marker = Int(local_p)          # FIRST ENTRY: record the frame-local
    # -- PARK: switch away; the driver is free the moment this returns. ------
    ms_ctx_switch(fr[].self_ctx, fr[].caller_ctx)

    # -- EXACT RESUME POINT (ADR-107): runs exactly once, on re-entry. ------
    if cell[].marker != Int(local_p):
        cell[].ok = 0
    cell[].resumed = cell[].resumed + 1
    # return: completion (the trampoline tail-switches back to the driver).


def red(what: String) raises -> None:
    print("T27 concurrency: RED (" + what + ")")
    raise Error(what)


def main() raises:
    var failures = List[String]()
    var ps = Int(ms_page_size())
    if ps <= 0:
        failures.append("ms_page_size non-positive")
    var stack_bytes = STACK_PAGES * ps

    # --- cell array (stable heap, never moves) ------------------------------
    var cells = UnsafePointer[T27Cell, MutAnyOrigin](
        unsafe_from_address=Int(c_malloc(N_LIVE * 64))
    )
    for k in range(N_LIVE):
        (cells + k)[0] = T27Cell()

    # One shared 2-slot BytePtr buffer per stack acquire; the base/top are
    # COPIED into fibers at make_fiber (t26/t25 proven ms_stack_alloc shape).
    var sbuf = UnsafePointer[BytePtr, MutUntrackedOrigin](
        unsafe_from_address=Int(c_malloc(N_LIVE * 2 * 8))
    )

    var fibs = List[Fiber]()
    for k in range(N_LIVE):
        var scell = sbuf + 2 * k
        if ms_stack_alloc(stack_bytes, scell, scell + 1) != 0:
            failures.append("stack alloc failed for fiber " + String(k))
            break
        var ns = NativeStack(scell[0], (scell + 1)[0])
        (cells + k)[].base = Int(scell[0])
        (cells + k)[].top = Int((scell + 1)[])
        if (cells + k)[].top & ALIGN_MASK != 0:
            failures.append("stack top not 16-aligned for fiber " + String(k))
        var cell_addr = UnsafePointer[Byte, MutAnyOrigin](
            unsafe_from_address=Int(cells + k)
        )
        fibs.append(make_fiber(
            UnsafePointer[NativeStack, MutAnyOrigin](to=ns),
            entry_pointer["t27_entry"](),
            cell_addr,
        ))

    var n_fibs = len(fibs)
    if n_fibs != N_LIVE:
        failures.append("expected " + String(N_LIVE) + " fibers, got "
                        + String(n_fibs))
    if len(failures) != 0:
        print("T27 concurrency: FAIL (setup)")
        for m in failures:
            print("  - " + m)
        _iso_exit(1)

    # ---- PASS 1: resume every fiber to its park point ---------------------
    # On the frozen substrate, fiber #33's first resume switch needs a 65th
    # resume-table row => brk #0x67 -> SIGILL; the process dies here.  That
    # is the RED evidence.  On a2/00 there is NO table, so all 40 suspend.
    for k in range(n_fibs):
        fibs[k].resume()

    # ---- PASS 2: resume every fiber to COMPLETION -------------------------
    for k in range(n_fibs):
        fibs[k].resume()

    # ---- verdicts -----------------------------------------------------------
    for k in range(n_fibs):
        var c = cells + k
        if c[].resumed != 1:
            failures.append("fiber " + String(k) + " resume-point "
                            + String(c[].resumed) + " != 1")
        if c[].ok != 1:
            failures.append("fiber " + String(k) + " lost its exact marker")
        if c[].marker == 0:
            failures.append("fiber " + String(k) + " never recorded a marker")
        if c[].marker < c[].base or c[].marker >= c[].top:
            failures.append("fiber " + String(k)
                            + " frame-local outside its stack")
    # ---- teardown -----------------------------------------------------------
    for k in range(n_fibs):
        fibs[k].destroy()
    c_free(UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=Int(cells)))
    c_free(UnsafePointer[Byte, MutAnyOrigin](unsafe_from_address=Int(sbuf)))

    if len(failures) == 0:
        print("T27 concurrency (" + String(n_fibs) + " fibers): PASS")
        return
    print("T27 concurrency: FAIL (" + String(len(failures)) + ")")
    for m in failures:
        print("  - " + m)
    _iso_exit(1)