# mojito_async/runtime/park.mojo
#
# A1 follow-up (issue #39) — the ONE park/wake kernel over the TCB parked-
# state machinery (spec §24 "ParkingLot / cancellation"; see
# task_control_block.mojo's embedded WaitNode for the allocation-free wait
# cell).
#
# Single source of truth for the cooperative suspend choreography.  Before
# this module the SAME transition sequence was re-implemented in four places:
#   - scheduler._suspend_current / resume_current   (the A1.1 canonical seam);
#   - task.suspend_commit / wake (+ dead park_prepare / park_commit);
#   - parking_lot.park_current / unpark_current + the ParkingLot struct;
#   - sync/event.WaitEvent (a readiness cell nobody consumed).
# Those four spellings are now ONE kernel here; every consumer (mutex,
# semaphore, channel, timer, scheduler, the *_aot drivers and the lane test
# drivers) calls the primitives below.  The dead duplicates were
# deleted (task.park_prepare/park_commit/suspend_commit/wake, parking_lot,
# WaitEvent).
#
# Execution discipline (A2.5, issue #71 — the PARKING-LOT-ADAPTER at the
# bottom was PROMOTED to live code; see the two-phase section for the full
# protocol):
#   park_current       — the single-worker WAITING-side pair: RUNNING ->
#       PARKING -> WAITING, stamping the wait REASON on the embedded
#       WaitNode (spec §24/§25, generation-bumped epoch).  Raises
#       IllegalTransitionError if the task is not RUNNING.
#   park_prepare / park_validate / park_commit — the two-phase PREPARE /
#       VALIDATE / COMMIT promotion for the cross-worker wake (spike
#       event.mojo model, spec §23 / A0-T11 / A0-T12).
#   unpark_current    — deliver readiness ONCE per epoch: claim the waiter's
#       generation EXACTLY ONCE (A0-T12) and re-enqueue onto the OWNER
#       worker's REMOTE-ready queue (spec §19.2; started fibers never route
#       to injection or to a steal candidate).  A wake may come from ANY
#       worker; the owner is resolved from the TCB's owner_runtime stamp
#       (set at first dispatch), so no global worker registry is needed.
#       An already-RUNNABLE task is a no-op (enqueue-once); a COMPLETED /
#       CANCELLED task raises — a stale wake never silently double-enqueues
#       (t15 asserts this).
#
# No hidden allocation.  OS-thread synchronization is confined to the OWNER
# worker's remote-ready queue spinlock (issue #68's P0 queue guard — the one
# lock every wake path already serializes through), used to make the
# two-phase latch/claim and the parker's COMMIT atomic with respect to each
# other.  The worker owns no task storage: every TCB cell is caller-
# allocated and the caller passes its own JoinHandle.
#
# RACE-PROTOCOL NOTE (A0.7 two-phase parking, issues #16/#71): the spike's
# PREPARE/VALIDATE/COMMIT pipeline exists to close a lost-wakeup window
# between publishing a waiter and parking.  On the A1 SINGLE cooperative
# worker there is no interleaving inside a dispatcher slice: publish+park
# (register_* then park_current) is atomic with respect to other tasks, so a
# release always finds its waiter already parked, and the protocol's
# VALIDATE re-check is a no-op.  On the A2 M:N scheduler a wake may come
# from ANOTHER worker MID-PARK, so the two-phase protocol is LIVE (below);
# the single-worker path (owner_runtime == 0) keeps the exact A1 behavior.
from mojito_async.runtime.runtime import Runtime
from mojito_async.runtime.task_control_block import ResultValue, TaskControlBlock
from mojito_async.runtime.join_handle import JoinHandle, SuspendReason
from mojito_async.runtime.checkpoint import checkpoint
from mojito_async.cancellation import CancellationToken


def park_current[R: ResultValue](
    mut rt: Runtime,
    h: JoinHandle[R],
    reason: Int = SuspendReason.PARK,
) raises:
    """Park the CURRENT task on the single worker: RUNNING -> PARKING -->
    WAITING (fresh epoch; stale-wakeup defense built into the TCB).  The wait
    REASON is stamped on the embedded WaitNode so a later wake can inspect
    why the task waits.  The worker is free for other RUNNABLE tasks.  Resume
    later via unpark_current."""
    h.tcb()[].transition(TaskControlBlock.PARKING)
    h.tcb()[].wait_node()[].set_reason(reason)
    h.tcb()[].transition(TaskControlBlock.WAITING)


def unpark_current[R: ResultValue](
    mut rt: Runtime,
    h: JoinHandle[R],
    required_gen: Int = 0,
    win_reason: Int = SuspendReason.UNCHANGED,
) raises:
    """Deliver readiness ONCE per epoch — the two-phase WAKE leg
    (spec §23 / A0-T12), routed to the OWNER worker (spec §19.2).

    Under the OWNER's remote-ready queue guard (resolved from the TCB's
    owner_runtime stamp; the A1 single worker keeps `rt`):
      - a WAITING task at `required_gen` (or any epoch when 0) has its
        generation claimed EXACTLY ONCE — wake_claim — and the wake record
        is pushed onto the OWNER's REMOTE-ready queue: a STARTED fiber's
        wake NEVER lands in injection and is NEVER a steal candidate;
      - a RUNNING/PARKING task (the early-wake window, A0-T11) has its
        readiness LATCHED — the parker's VALIDATE/COMMIT consumes it and
        unwinds WITHOUT WAITING and WITHOUT a generation bump;
      - an already-RUNNABLE task is a no-op (enqueue-once, Q6);
      - a WAITING task at a STALE `required_gen` is rejected silently —
        nothing is claimed, nothing enqueued (fresh-generation guard);
      - a COMPLETED/CANCELLED task raises (the A1 loud surface) — but ONLY
        for a genuinely illegal NON-duplicate wake (H2, PR #109).

    Generation consumption (T5, issue #51): pass `required_gen` = the
    generation a producer captured at WAITING commit to REJECT a stale wake
    from a previous epoch.  Pass 0 (default) to always claim when WAITING
    (today's single worker: the epoch is trivially current).

    A4.4 (issue #58) — WINNER-REASON STAMP: pass `win_reason` (one of
    SuspendReason.READY/CANCEL/TIMER/CLOSED) to record WHICH cause won the
    claim on the WaitNode (spec §25/§29.2).  Stamped ONLY on a SUCCESSFUL
    claim, and INSIDE THE SAME owner remote-queue guard section that
    performs wake_claim — never as a separate step before/after this call
    — so the stamp is atomic with the claim: two causes racing for the
    same epoch can never have the LOSER's label clobber the WINNER's (the
    top-level RUNNABLE fast-return below makes every losing call a
    complete no-op, including its win_reason, before it reaches the guard).
    The default SuspendReason.UNCHANGED leaves `_reason` exactly as the
    park side stamped it — EVERY existing call site (mutex, semaphore,
    channel, timer_service) omits `win_reason` and is BYTE-FOR-BYTE
    unaffected by this parameter's existence.

    H2 (PR #109) — DUPLICATE/STALE claims are QUIET NO-OPs in EVERY task
    state: a wake whose `required_gen` no longer matches the current
    generation (stale) or whose epoch was already claimed (duplicate — the
    TCB's claimed-epoch marker) returns WITHOUT enqueueing and WITHOUT
    raising, no matter where the task is — RUNNING, PARKING, RUNNABLE,
    COMPLETED or CANCELLED.  A racing duplicate must never corrupt a later
    park via a spurious early latch, and must never raise against a task
    that legitimately completed after the winning claim.  The loud
    IllegalTransitionError surface is preserved ONLY for genuinely illegal
    NON-duplicate wakes: a fresh wake (no epoch claim, e.g. `required_gen`
    = 0, the A1 single-worker call form) delivered to a COMPLETED/CANCELLED
    task (t15/t27 assert this)."""
    if h.state() == TaskControlBlock.RUNNABLE:
        return
    var owner = _owner_rt(h, rt)
    # The latch/claim section: serialized against the parker's park_commit
    # under the SAME guard, so an epoch can never be both latched (early)
    # and claimed (WAITING) — exactly one winner (A0-T10).
    owner[].remote_queue()[]._guard.lock()
    var claimed = False
    var st = h.tcb()[].state()
    if st == TaskControlBlock.WAITING:
        claimed = h.tcb()[].wake_claim(required_gen)
        if claimed:
            h.tcb()[].clear_early_readiness()
            if win_reason != SuspendReason.UNCHANGED:
                h.tcb()[].wait_node()[].set_reason(win_reason)
    elif st == TaskControlBlock.PARKING or st == TaskControlBlock.RUNNING:
        # Early-wake window: latch readiness for the parker's VALIDATE
        # re-check (A0-T11).  No claim, no transition, no enqueue here —
        # the parker's COMMIT decides the unwind (Q6).  A STALE or
        # DUPLICATE claim (H2) must NOT latch: the epoch it references is
        # already consumed, so latching would fabricate a phantom early
        # wake for the task's NEXT park.
        if not _stale_or_duplicate(h, required_gen):
            h.tcb()[].set_early_readiness()
    owner[].remote_queue()[]._guard.unlock()
    if claimed:
        # STARTED with a known owner: deliver to the OWNER's REMOTE-ready
        # queue (spec §19.2) — never injection, never a steal candidate.
        owner[].push_remote(Int(h.tcb()), h.id())
        return
    if st == TaskControlBlock.WAITING or st == TaskControlBlock.PARKING or st == TaskControlBlock.RUNNING:
        return  # stale/duplicate rejected, or early latch delivered
    # COMPLETED / CANCELLED / NEW: the loud A1 surface applies ONLY to
    # genuinely illegal NON-duplicate wakes (a stale wake never silently
    # enqueues twice).  A stale/duplicate generation claim is a quiet no-op
    # here too — the racing duplicate that lands after the task completed
    # must not raise (H2).
    if _stale_or_duplicate(h, required_gen):
        return
    h.tcb()[].transition(TaskControlBlock.RUNNABLE)


def _stale_or_duplicate[R: ResultValue](
    h: JoinHandle[R],
    required_gen: Int,
) -> Bool:
    """H2 (PR #109): True when the wake carries an epoch that is NOT a live
    first delivery — `required_gen` != 0 AND either the task's generation
    has moved past it (STALE: the epoch is gone, superseded by a re-park or
    consumed) or the epoch was already claimed by a previous wake
    (DUPLICATE: the TCB's claimed-epoch marker matches).  Such a wake is a
    QUIET NO-OP in every task state — never a claim, never an early latch,
    never a transition, never a raise.  Only the caller-visible state under
    the OWNER's remote-ready queue guard feeds this (the guard serializes
    it against park_commit and all other wake legs)."""
    if required_gen == 0:
        return False
    var t = h.tcb()[]
    return t.generation() != required_gen or t.claimed_epoch() == required_gen


def _owner_rt[R: ResultValue](
    h: JoinHandle[R],
    mut rt: Runtime,
) -> UnsafePointer[Runtime, MutAnyOrigin]:
    """Resolve the wake target Runtime for task `h`: the OWNER worker's
    runtime (TCB owner_runtime stamp, set at first dispatch — A2.5 issue
    #71), or the caller's `rt` on the A1 single worker (owner_runtime == 0:
    no pool, the sole worker IS the owner; the A1 behavior is preserved
    exactly)."""
    var addr = h.tcb()[].owner_runtime()
    if addr == 0:
        return UnsafePointer[Runtime, MutAnyOrigin](to=rt)
    return UnsafePointer[Runtime, MutAnyOrigin](unsafe_from_address=addr)


# ---------------------------------------------------------------------------
# TWO-PHASE PARK (A2.5, issue #71 — promoted from the PARKING-LOT-ADAPTER
# at the bottom to LIVE code; spike event.mojo PREPARE/VALIDATE/COMMIT
# calling convention restored).
# ---------------------------------------------------------------------------
#
# On the A2 M:N scheduler a wake may come from ANOTHER worker, so the A0.7
# two-phase protocol applies.  The primitive set:
#
#   park_prepare(h)   — RUNNING -> PARKING, open the early-wake window.  The
#                        embedded WaitNode + waiter id are ALREADY published
#                        in the TCB (spec §24: no allocation, no re-
#                        registration — the wake side reaches them through
#                        the handle); PREPARE neither sets nor clears the
#                        epoch readiness latch (a producer that latched
#                        while the task was still RUNNING — the release-
#                        before-park case — must stay observable).
#   park_validate(h)  — re-check readiness inside the window (the lost-
#                        wakeup window, spec §23.2 / A0-T11), under the
#                        owner's remote-ready queue guard (the same lock the
#                        WAKE leg latches through, so a concurrent cross-
#                        worker wake is never missed).  READY => the caller
#                        closes with park_commit, which unwinds PARKING ->
#                        RUNNABLE WITHOUT a generation bump and WITHOUT
#                        entering WAITING (the task never slept; no enqueue
#                        — Q6).
#   park_commit(h, r)  — close the window under the same guard: readiness
#                        latched => consume the latch and unwind PARKING ->
#                        RUNNABLE (WAITING never entered, no gen bump);
#                        else PARKING -> WAITING, FRESH generation, wait
#                        reason `r` stamped (spec §25).  Exactly ONE winner
#                        (A0-T10) and enqueue-once (Q6) by construction:
#                        the commit's readiness check and the WAKE leg's
#                        latch/claim are serialized under one guard.
#   unpark_current     — the WAKE leg above, claiming the waiter's
#                        generation EXACTLY ONCE per epoch (A0-T12).
#
# Cancellation integration seam: the settle policy (cancellation FIRST, then
# readiness, spike race_hooks.settle) is the caller's pre-check — the A1
# cancellation surface (CancellationToken / CancellationError at the next
# checkpoint) already precedes every park on the consumer side; the commit's
# readiness unwind covers the no-cancel geometry this lane owns.
# ---------------------------------------------------------------------------

def park_prepare[R: ResultValue](h: JoinHandle[R]) raises:
    """Two-phase park step 1 — PREPARE (spec §23 / A0-T11): RUNNING ->
    PARKING opens the early-wake window.  The embedded WaitNode + this
    task's scheduler id are already published in the TCB (spec §24 —
    allocation-free; the wake side reaches them through the handle).  The
    epoch readiness latch is consumed at COMMIT; PREPARE does not clear it
    (a producer that latched while the task was still RUNNING — the release-
    before-park case — must stay observable for the VALIDATE re-check)."""
    h.tcb()[].transition(TaskControlBlock.PARKING)


def park_validate[R: ResultValue](h: JoinHandle[R]) -> Bool:
    """Two-phase park step 2 — VALIDATE (A0-T11): re-check readiness inside
    the early-wake window (the lost-wakeup window, spec §23.2).  Returns
    True when a wake was delivered before the check; the caller then closes
    the window with park_commit, which unwinds to RUNNABLE WITHOUT a
    generation bump and WITHOUT entering WAITING (the task never slept; no
    enqueue — Q6: the record was already dequeued, the slice continues).
    Reads the latch under the OWNER's remote-ready queue guard — the same
    lock the WAKE leg latches through — so a concurrent cross-worker wake is
    never missed (PREPARE then VALIDATE re-check catches readiness-before-
    COMMIT) and never observed twice."""
    var addr = h.tcb()[].owner_runtime()
    if addr == 0:
        # A1 single worker: no cross-worker interleaving — plain read.
        return h.tcb()[].early_readiness()
    var owner = UnsafePointer[Runtime, MutAnyOrigin](unsafe_from_address=addr)
    owner[].remote_queue()[]._guard.lock()
    var rdy = h.tcb()[].early_readiness()
    owner[].remote_queue()[]._guard.unlock()
    return rdy


def park_commit[R: ResultValue](
    h: JoinHandle[R],
    reason: Int = SuspendReason.PARK,
) raises -> Bool:
    """Two-phase park step 3 — COMMIT: close the early-wake window.

    Under the OWNER's remote-ready queue guard (the same lock the WAKE leg
    latches/claims through):
      - readiness latched (an early wake was delivered in the window):
        consume the latch and unwind PARKING -> RUNNABLE — WAITING is NEVER
        entered and the wait generation is NEVER bumped (A0-T11).  No
        enqueue here (Q6): the record was already dequeued; the caller keeps
        running the task in this slice.  Returns False.
      - else: PARKING -> WAITING — FRESH wait epoch (generation bump) and
        the wait reason `r` stamped on the embedded node (spec §25).  The
        wake producer claims the new generation exactly once.  Returns True.

    Returns True when the task committed to WAITING (genuinely parked);
    False when it unwound PARKING -> RUNNABLE because an early wake was
    consumed.  Callers MUST handle False: the task is RUNNABLE, in NO queue,
    and the caller's dispatcher is still live on the OS-thread — the caller
    must either claim_running and continue in the same slice (mutex/channel
    style) or push_remote to re-enqueue for a later slice (fiber-seam style).

    Exactly ONE winner (A0-T10): the commit's readiness check and the WAKE
    leg's latch/claim are serialized under the same guard, so a wake can
    never both latch (early) and claim (WAITING) for the same epoch.  Raises
    unless the task is PARKING (the window must be open)."""
    if h.state() != TaskControlBlock.PARKING:
        raise Error("park_commit: task must be PARKING to commit the park")
    var addr = h.tcb()[].owner_runtime()
    if addr == 0:
        # A1 single worker: no cross-worker interleaving — lock-free commit.
        if h.tcb()[].early_readiness():
            h.tcb()[].clear_early_readiness()
            h.tcb()[].transition(TaskControlBlock.RUNNABLE)
            return False
        h.tcb()[].wait_node()[].set_reason(reason)
        h.tcb()[].transition(TaskControlBlock.WAITING)
        return True
    var owner = UnsafePointer[Runtime, MutAnyOrigin](unsafe_from_address=addr)
    owner[].remote_queue()[]._guard.lock()
    if h.tcb()[].early_readiness():
        h.tcb()[].clear_early_readiness()
        h.tcb()[].transition(TaskControlBlock.RUNNABLE)
        owner[].remote_queue()[]._guard.unlock()
        return False
    h.tcb()[].wait_node()[].set_reason(reason)
    h.tcb()[].transition(TaskControlBlock.WAITING)
    owner[].remote_queue()[]._guard.unlock()
    return True

# ---------------------------------------------------------------------------
# PARKING-LOT-ADAPTER (A2 seam history — the two-phase protocol above is the
# PROMOTED live code; this block is retained as the design record).
# ---------------------------------------------------------------------------
#
# The A0.7 spike model (event.mojo) the promotion follows:
#
#   PREPARE  — publish the waiter as DATA: task id + embedded WaitNode;
#              RUNNING -> PARKING opens the early-wake window
#   VALIDATE — recheck readiness — the lost-wakeup window; readiness here
#              returns WITHOUT sleeping and WITHOUT a generation bump (the
#              task never left RUNNING)                              [A0-T11]
#   COMMIT   — close the window: readiness/cancel unwinds via PARKING ->
#              RUNNABLE (WAITING never entered); otherwise PARKING ->
#              WAITING bumps the generation and stamps the node
#   WAKE     — unpark_current claims the waiter's generation EXACTLY ONCE,
#              records ONE enqueue, and resumes the waiter via the WAITING
#              -> RUNNABLE claim edge                                [A0-T12]
#
# A2.1 (issue #67) NAME RESERVATIONS (kept):
#   - park_current / unpark_current stay the WAITING-side pair; their
#     signatures are unchanged and remain the single park/wake kernel.
#   - the A2 worker loop (thread_entry.pool_worker_loop, E2-OWNED seam) is
#     issue #68's; the NativeEvent idle path is E6 (#72).
#   - cross-worker wakes route through wake_target_worker (scheduler.mojo,
#     A1.3 affinity seam) + the owner-routing in unpark_current above.


# ---------------------------------------------------------------------------
# TASK-AWARE CANCELLATION (A4.3, issue #57) — token-aware park + push-cancel
# ---------------------------------------------------------------------------
#
# The two-phase COMMIT above documents its own cancellation seam: "the
# settle policy (cancellation FIRST, then readiness) is the caller's
# pre-check ... the commit's readiness unwind covers the no-cancel
# geometry".  This section is that pre-check PLUS the receive-side half the
# seam defers: a park that already observed a request never parks
# (park_cancellable), and a WAITING task can be reached from OUTSIDE by a
# holder of its JoinHandle who decided its CancellationToken lost patience
# (wake_cancelled) — matching how a readiness wake (unpark_current) already
# reaches a WAITING task from outside today.
#
# C6 winner rule (readiness vs cancel): wake_cancelled is a THIN wrapper
# over the unchanged unpark_current — it inherits the SAME exactly-once
# generation claim (wake_claim) unpark_current already guarantees, so
# "exactly one winner" falls out of the existing kernel for free.  The
# WaitNode's `_reason` field (stamped CANCEL only by the call that actually
# WINS the claim) is what lets the resumed primitive tell readiness and
# cancellation apart: unpark_current's own no-op branches (already-
# RUNNABLE, stale/duplicate generation) never touch `_reason`, so a losing
# wake_cancelled call leaves the WINNING wake's stamp untouched.  Mutex /
# Semaphore / Channel own the OTHER half of "exactly one winner, coherent
# state": their `cancel_*_wait` methods (sync/mutex.mojo, sync/semaphore.
# mojo, channel/channel.mojo) remove the waiter from their OWN FIFO before
# calling wake_cancelled, so a readiness handoff that already popped the
# same waiter leaves nothing for a racing cancel to find (no ghost queue
# entry, no leaked permit/lock/slot — spec: "cancel unblocks ... leaving
# the primitive in a coherent state").
#
# `_reason` re-stamp safety: park_current/park_commit stamp `_reason` FRESH
# on every new park, so a CANCEL left over from a resumed-and-consumed wait
# can never leak into a LATER, unrelated wait — but the consuming side
# (raise_if_cancel_wake / with_cancel) MUST clear it back to NONE the
# moment it is observed (own doing, not park's), for the same reason.
def park_cancellable[R: ResultValue](
    mut rt: Runtime,
    h: JoinHandle[R],
    token: CancellationToken,
    reason: Int = SuspendReason.PARK,
) raises:
    """Token-aware park (issue #57): the caller-side pre-check the COMMIT
    docs above defer to.  Raises CancellationError-as-Error (reusing
    cancellation.mojo's checkpoint naming verbatim, deliverable #2) WITHOUT
    parking when `token` already requested cancellation — a task is never
    parked with no live path back except an external wake it cannot
    guarantee.  Otherwise delegates to `park_current` unchanged; a later
    `wake_cancelled` against this same handle is what delivers a MID-wait
    cancel."""
    checkpoint(token)
    park_current(rt, h, reason)


def wake_cancelled[R: ResultValue](mut rt: Runtime, h: JoinHandle[R]) raises:
    """Push-cancel a WAITING task (issue #57): stamp the wait reason CANCEL
    then deliver the SAME wake `unpark_current` would for readiness.

    Callers that own a primitive-specific wait queue (Mutex/Semaphore/
    Channel) MUST remove `h` from their own queue FIRST (see module header)
    — this function only performs the state transition, exactly like
    unpark_current's contract.  If readiness already claimed the wake (the
    task is no longer WAITING), unpark_current's existing no-op path fires
    and `_reason` is left untouched — the earlier winner's stamp (or lack
    of one) stands; this call never overwrites a settled outcome."""
    h.tcb()[].wait_node()[].set_reason(SuspendReason.CANCEL)
    unpark_current(rt, h)


def is_cancel_wake[R: ResultValue](h: JoinHandle[R]) -> Bool:
    """True when this waiter's most recent wake was the CANCEL winner
    (wake_cancelled's stamp survived unpark_current's claim) — the C6
    post-resume winner read.  Non-consuming query; pair with
    raise_if_cancel_wake / with_cancel to also clear the stamp."""
    return h.tcb()[].wait_node()[].reason() == SuspendReason.CANCEL


def raise_if_cancel_wake[R: ResultValue](h: JoinHandle[R]) raises:
    """Post-resume winner check + raise (issue #57 C6): if wake_cancelled
    won this wait, clear the stamp (module-header re-stamp-safety) and
    raise CancellationError-as-Error; a no-op when readiness won."""
    if is_cancel_wake(h):
        h.tcb()[].wait_node()[].set_reason(SuspendReason.NONE)
        raise Error("CancellationError: park/wait cancelled")


def with_cancel[R: ResultValue](
    mut rt: Runtime,
    h: JoinHandle[R],
    token: CancellationToken,
    reason: Int = SuspendReason.PARK,
) raises -> Bool:
    """Cancellable park, UNBLOCKS-WITHOUT-RAISE form (issue #57 deliverable
    4): the with_cancel counterpart to park_cancellable's raise, for the
    PRE-park check only.  True = parked normally (proceed exactly as
    park_current would — the caller is free to drive other tasks); False =
    the token was ALREADY requested — the wait was never entered (same
    "commit aborts the wait" contract as park_cancellable), returned
    instead of raised so a caller with its own error surface can decide how
    to report it.

    Cooperative re-entrant model (spec §88: no blocking call, no fiber):
    park_current never suspends the call stack — it stamps WAITING and
    RETURNS to the driver immediately, so a resume is a SEPARATE later
    re-entry, never observable inside this same call.  with_cancel therefore
    covers ONLY the pre-park half; the POST-resume winner check is the
    caller's job on its OWN next re-entry via is_cancel_wake /
    raise_if_cancel_wake — the exact two-call shape Mutex.lock_cancellable /
    Semaphore.acquire_cancellable / Channel.send_cancellable/recv_cancellable
    already use (raise_if_cancel_wake at the TOP of the re-entrant method,
    the park attempt at the bottom)."""
    if token.is_cancellation_requested():
        return False
    park_current(rt, h, reason)
    return True