# mojito_async/test/unit/t55_grant_marker_identity.mojo
#
# RED driver for issue #149 — every grant marker is Int(1), stamped into the
# same shared cell, so any primitive can claim another's grant.
#
#     sync/mutex.mojo:60       comptime WAITER_GRANTED = Int(1)
#     sync/semaphore.mojo:49   comptime PERMIT_GRANTED = Int(1)
#     sync/rwlock.mojo:80      comptime RW_GRANTED     = Int(1)
#
# All three stamp and claim the SAME constant in the SAME `WaitNode._next`
# cell, with no record of which primitive — or which instance — issued the
# grant.  `Mutex.holds_grant` taking `self` and then ignoring it is the tell
# that instance identity was meant to be there.
#
# WHY THIS IS ORDINARY AND NOT EXOTIC.  The runtime's re-entry model makes it
# the default shape: a resumed task re-runs its slice from the top, so the
# body below — acquire a permit, then take a lock — re-executes `acquire`
# every time a grant for the LOCK resumes it.  No phase flags, no tricks;
# this is how the tree's own drivers are written.
#
# THE SEQUENCE, all deterministic on one worker:
#
#   1. The task acquires a permit, then contends for a held mutex and parks.
#   2. `unlock()` pops it, stamps WAITER_GRANTED (= 1) into `_next`, wakes it.
#   3. The task is re-dispatched and re-runs from the top.
#   4. `Semaphore.acquire` runs FIRST, reads `_next` == 1 == PERMIT_GRANTED,
#      clears it, and returns True WITHOUT consuming a permit.
#   5. `Mutex.lock` then runs with the marker gone and `_locked` still True
#      (it is held across the handoff by design), so the task re-queues into
#      a FIFO nobody will drain again.
#
# Silent permit overcommit plus deadlock, with no diagnostic anywhere.
#
# ORACLE.  Permits are conserved: successful acquires must equal permits
# consumed.  The driver counts both and compares, so "acquire returned True
# having consumed nothing" is an arithmetic fact rather than a judgement
# call.  The deadlock half is read off the mutex directly.
#
# Verdict: exit 0 + "PASS"; any failure prints the details and raises.
from std.memory import stack_allocation
from mojito_async.integration.sys import BytePtr, IntResult
from mojito_async.runtime.runtime import Runtime, create
from mojito_async.runtime.scheduler import scheduler_loop
from mojito_async.runtime.task_control_block import TaskControlBlock
from mojito_async.sync import Mutex, Semaphore
from mojito_async.task import JoinHandle, claim_running, spawn


comptime TB = TaskControlBlock[IntResult]
comptime START_PERMITS = Int(2)


struct Scene(ImplicitlyCopyable, ImplicitlyDeletable):
    var sem: UnsafePointer[Semaphore, MutAnyOrigin]
    var mtx: UnsafePointer[Mutex[Int], MutAnyOrigin]
    var c: UnsafePointer[Int, MutAnyOrigin]   # 0 acquires_ok, 1 locks_ok

    def __init__(out self):
        self.sem = UnsafePointer[Semaphore, MutAnyOrigin](unsafe_from_address=1)
        self.mtx = UnsafePointer[Mutex[Int], MutAnyOrigin](unsafe_from_address=1)
        self.c = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=1)


def dispatch_body(
    mut rt: Runtime, tcb_addr: Int, tid: Int, ud: BytePtr
) raises -> Int:
    """The task body, written the plain way: acquire a permit, then take the
    lock.  No phase flag, because the re-entry model re-runs the slice from
    the top and the codebase's own docs say so ("the driver re-enters the
    task, which re-invokes send()/recv()")."""
    var sc = ud.bitcast[Scene]()
    var h = JoinHandle[IntResult](
        UnsafePointer[TB, MutAnyOrigin](unsafe_from_address=tcb_addr), tid
    )
    claim_running(h)
    if not sc[].sem[].acquire(rt, h, 1):
        return 1                       # parked on the semaphore
    sc[].c[0] += 1                     # this call REPORTED a permit acquired
    if not sc[].mtx[].lock(rt, h):
        return 1                       # parked on the mutex
    sc[].c[1] += 1
    return 1


def main() raises:
    var failures = List[String]()
    var rt = create()
    var sem = Semaphore(START_PERMITS)
    var mtx = Mutex[Int](0)
    var cells = stack_allocation[4, Int]()
    for i in range(4):
        cells[i] = 0

    var sc = Scene()
    sc.sem = UnsafePointer[Semaphore, MutAnyOrigin](to=sem)
    sc.mtx = UnsafePointer[Mutex[Int], MutAnyOrigin](to=mtx)
    sc.c = UnsafePointer[Int, MutAnyOrigin](unsafe_from_address=Int(cells))
    var scp = UnsafePointer[Scene, MutAnyOrigin](to=sc)
    var ud = scp.bitcast[Byte]()

    # Somebody else holds the lock, so the body's lock() takes the slow path.
    if not mtx.try_lock():
        print("T55 grant marker identity: RED (setup could not take the lock)")
        raise Error("setup")

    var tcb = TB.create()
    var h = spawn(rt, UnsafePointer[TB, MutAnyOrigin](to=tcb), 0)

    # --- first slice: permit taken, then parked on the mutex ---------------
    _ = scheduler_loop(rt, dispatch_body, ud)
    if cells[0] != 1:
        failures.append("setup: the body should have acquired one permit")
    if sem.available() != START_PERMITS - 1:
        failures.append("setup: one permit should be gone, available="
                        + String(sem.available()))
    if h.state() != TaskControlBlock.WAITING:
        failures.append("setup: the body should be parked on the mutex (state "
                        + String(h.state()) + ")")

    # --- the handoff: unlock() stamps WAITER_GRANTED into `_next` ----------
    var handed = mtx.unlock[IntResult](rt)
    if not handed:
        failures.append("setup: unlock() did not hand off to the waiter")
    if tcb.wait_node()[].next() != 1:
        failures.append("setup: the mutex grant marker should be stamped")

    # --- re-entry: the body re-runs from the top --------------------------
    var permits_before = sem.available()
    var acquires_before = cells[0]
    _ = scheduler_loop(rt, dispatch_body, ud)
    var acquires_after = cells[0]
    var permits_after = sem.available()

    var reported = acquires_after - acquires_before
    var consumed = permits_before - permits_after
    if reported != consumed:
        failures.append(
            "GRANT COLLISION — Semaphore.acquire returned True "
            + String(reported) + " time(s) on re-entry but consumed "
            + String(consumed) + " permit(s). It read the MUTEX's"
            + " WAITER_GRANTED marker (Int(1)) as its own PERMIT_GRANTED"
            + " (also Int(1), same WaitNode._next cell), cleared it, and"
            + " returned without touching the counter. available()="
            + String(permits_after) + " with " + String(acquires_after)
            + " successful acquires against " + String(START_PERMITS)
            + " starting permits: the semaphore is oversubscribed."
        )

    if tcb.wait_node()[].next() != 0:
        failures.append("the marker should have been consumed by somebody")

    # --- and the lock the marker was actually FOR -------------------------
    if cells[1] != 0:
        # The body took the lock, so the marker reached its owner after all.
        pass
    else:
        if h.state() == TaskControlBlock.WAITING and mtx.is_locked():
            failures.append(
                "DEADLOCK — the mutex grant was eaten by the semaphore, so"
                + " lock() found no marker, saw _locked still True (it is held"
                + " across the handoff by design) and re-queued: the task is"
                + " WAITING again with waiter_count="
                + String(mtx.waiter_count())
                + " and no owner left to unlock it."
            )

    if len(failures) == 0:
        print("T55 grant marker identity: PASS")
    else:
        print("T55 grant marker identity: RED (" + String(len(failures)) + ")")
        for m in failures:
            print("  - " + m)
        raise Error("T55 grant marker identity: RED")
