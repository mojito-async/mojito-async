# mojito_async/time/timeout_scope.mojo
#
# A6.1 timer lane (issue #84) — deadline-scoped operation with auto-cancel +
# typed timeout (EPIC #6 dependency F4, spec §30 `with_timeout`/`timeout`).
#
# Built as a CHILD SCOPE with a deadline, not a separate timeout-specific
# task model (per the issue's design note): `open_timeout` nests a fresh
# `mojito_async.scope.Scope` under a caller-owned parent scope (the A3
# cancellation tree, issue #54, already merged) and arms ONE monotonic
# deadline on the shared `TimerHeap` (A1.4, issue #36).  At expiry the
# `timeout_scope_driver` service hook — plugged into the SAME expiry pass
# `service_timers`/`drive_step` already drive (issue #39 single-source
# park/wake: no second wake path) — descends the scope tree via the
# existing `request_cancel_all()` (spec §29.1: every child, including
# nested subscopes, cancelled recursively) and resumes the parked owner
# EXACTLY ONCE via the canonical `unpark_current`, its TCB stamped with a
# `TimeoutError` through the EXISTING erased failure slot (`mark_failed` /
# `is_failed` / `error()` on `TCB_Prefix`, #42) — no new ResultValue type,
# matching the house error-taxonomy convention (message-prefix decode, see
# `is_timeout_error` below): a deadline-cancelled child/owner reads as a
# deterministic timeout, distinct from a user failure.
#
# Registry (alloc-free wake path, per the issue's implementation plan):
# `TimeoutRegistry` maps a scope-deadline arm's timer id -> (live gen, the
# nested Scope's FINAL address, the owner's TCB address + task id) in
# caller-owned parallel Lists (the CancelFlagRegistry/Scope-registry house
# pattern).  `timeout_scope_driver` consults it once per popped entry to
# tell "this arm is a scope deadline" from "a plain sleep" cheaply — an
# entry NOT in the registry is serviced exactly like `service_timers`
# (this hook SUBSUMES service_timers' plain-wake path so one hook can
# drive a heap that mixes scope-deadlines and ordinary sleeps).
#
# Result precedence (spec §32 / issue #84 acceptance):
#   - a child (or the scope) already CANCELLED by an earlier event (e.g. a
#     failure-driven `request_cancel_all()`) makes a LATER expiry pop of
#     the SAME scope a no-op — `Scope.is_cancelled()` is the idempotency
#     gate — "child failure beats a later timeout";
#   - an owner already COMPLETED is never touched (never resurrect a
#     settled result) — "child [the owner's own task] completes before
#     the deadline ... no cancellation is applied";
#   - nested timeouts: the min-heap pops the inner (earlier) deadline
#     first; `request_cancel_all()`'s existing child->parent propagation
#     (#54) marks the OUTER scope cancelled too, so the outer arm's LATER
#     pop hits the same idempotency gate — "generation tokens stop a stale
#     outer wake from double-canceling" (no second owner mark/wake).
#
# `refresh_timeout` re-arms (issue #84 plan point 5): `TimerHeap.arm`
# already grants a fresh generation per id, so the prior arm becomes STALE
# (superseded, matches A1.4's generation-token discipline exactly, see
# timer_heap.mojo); `refresh_timeout` also opportunistically
# `cancel_token`s the OLD (id, gen) pair — a stale token can never cancel a
# NEWER arm (`cancel_token`'s exact-match contract), so "cancel-by-refresh
# cannot disturb a live deadline" holds even under repeat refreshes.
#
# Mojo 1.0.0b2 dialect (matches scope.mojo / timer_heap.mojo headers):
# `def` only; no module globals; no function-typed fields — `TimeoutScope`
# is constructed EXCLUSIVELY through the OUT-PARAM factory `open_timeout`
# (never `TimeoutScope(...)` directly), which places the embedded `Scope`
# field at the caller's FINAL binding address before any parent-registry
# pointer is taken (the same discipline `scope.make_nested_scope`
# documents: a return-by-value factory would register a temporary's
# address that dangles after the move).
#
# COMPILER QUIRK (found + fixed this session, issue #84): chaining
# `self = TimeoutScope(...)` -> `self._scope = make_nested_scope(...)`
# inside `TimeoutScope.__init__` — i.e. assigning the result of ONE
# out-param factory to a FIELD of ANOTHER out-param — does NOT reliably
# place the embedded Scope at its final address under b2: a cancel-tree
# descent reaching it through the PARENT's `_child_scope_ptrs` (recorded
# during that assignment) crashes inside `Scope._mark_cancelled_with_
# children`, dereferencing the pre-move temporary's stale address
# (reproduced with a two-level TimeoutScope-then-nested-scope tree driven
# through `timeout_scope_driver`; a direct in-frame `request_cancel_all()`
# call on the SAME tree does not crash — only the address stored via the
# chained assignment is wrong).  FIX: `TimeoutScope.__init__` constructs
# `self._scope = Scope(...)` DIRECTLY (`Scope.__init__` captures no
# self-pointer, so a temporary-then-move is harmless) and only AFTER that
# settles does it capture `UnsafePointer(to=self._scope)` — by then `self`
# (this constructor's own out-param) is already the caller's stable final
# binding, so the field address is stable too.  `scope.validate_nest` /
# `scope.attach_child_scope` factor the shared validation/registration
# out of `make_nested_scope` so both paths share one source of truth.
from std.collections import List
from mojito_async.runtime.runtime import Nil, Runtime
from mojito_async.runtime.join_handle import JoinHandle
from mojito_async.runtime.park import unpark_current
from mojito_async.runtime.scheduler import scheduler_loop
from mojito_async.runtime.task_control_block import ResultValue, TaskControlBlock
from mojito_async.integration.sys import BytePtr
from mojito_async.scope import Scope, attach_child_scope, validate_nest
from mojito_async.time.timer_heap import TimerHeap


# ---------------------------------------------------------------------------
# TimeoutError (error model — the message-prefix taxonomy convention)
# ---------------------------------------------------------------------------


struct TimeoutError:
    """Named error model for a scope-deadline expiry (issue #84).  Not a
    ResultValue: like ChildrenStillLive/ScopeCancelled/CancellationError
    elsewhere in this codebase, `raise` only ever carries `Error`, so the
    taxonomy is the documented "TimeoutError:" message-prefix, decoded by
    `is_timeout_error` below.  Carried on the owner's erased TCB_Prefix
    failure stamp (`mark_failed`/`error()`), not thrown directly — the
    resumed owner's OWN dispatch checkpoint reads it and re-raises."""

    var message: String

    def __init__(out self, msg: String):
        self.message = msg


def is_timeout_error(e: Error) -> Bool:
    """True when `e` is a TimeoutError (message begins with the stable
    "TimeoutError:" prefix)."""
    return "TimeoutError:" in String(e)


# ---------------------------------------------------------------------------
# TimeoutRegistry — caller-owned (timer id -> scope-deadline arm) mapping
# ---------------------------------------------------------------------------


struct TimeoutRegistry(Movable, ImplicitlyDeletable):
    """Maps a scope-deadline arm's timer id to its live generation, the
    nested Scope's FINAL address, and the owner's (TCB address, task id).

    Backed by parallel Lists (allocates only on register growth — the
    CancelFlagRegistry/Scope registry house pattern), so
    `timeout_scope_driver`'s hot-path lookup (`is_registered`) is O(n) over
    the LIVE scope-deadline arms only (not every timer), never the whole
    heap.  The registry does NOT own the Scope cell: `open_timeout`
    allocates it (embedded in the caller's `TimeoutScope` binding) and
    passes its pointer in."""

    var _ids: List[Int]
    var _gens: List[Int]
    var _scope_ptrs: List[UnsafePointer[Scope, MutAnyOrigin]]
    var _owner_addrs: List[Int]
    var _owner_ids: List[Int]

    def __init__(out self):
        self._ids = List[Int]()
        self._gens = List[Int]()
        self._scope_ptrs = List[UnsafePointer[Scope, MutAnyOrigin]]()
        self._owner_addrs = List[Int]()
        self._owner_ids = List[Int]()

    def _index_of(self, id: Int) -> Int:
        for i in range(len(self._ids)):
            if self._ids[i] == id:
                return i
        return -1

    def is_registered(self, id: Int) -> Bool:
        return self._index_of(id) >= 0

    def register(
        mut self,
        id: Int,
        gen: Int,
        scope_ptr: UnsafePointer[Scope, MutAnyOrigin],
        owner_addr: Int,
        owner_id: Int,
    ) raises:
        """Bind a fresh scope-deadline arm.  Refuses a duplicate timer id
        (timer ids come from the caller's monotonic `rt.next_id()`
        allocator, matching Scope's own handle convention, so a duplicate
        signals a caller bug, not a legitimate re-arm — re-arms go through
        `update_gen` instead)."""
        if self.is_registered(id):
            raise Error("TimeoutRegistry: duplicate timer id " + String(id))
        self._ids.append(id)
        self._gens.append(gen)
        self._scope_ptrs.append(scope_ptr)
        self._owner_addrs.append(owner_addr)
        self._owner_ids.append(owner_id)

    def unregister(mut self, id: Int) raises:
        """Drop a registration (swap-remove).  Refuses an unknown id."""
        var i = self._index_of(id)
        if i < 0:
            raise Error("TimeoutRegistry: unknown timer id " + String(id))
        var last = len(self._ids) - 1
        self._ids[i] = self._ids[last]
        self._gens[i] = self._gens[last]
        self._scope_ptrs[i] = self._scope_ptrs[last]
        self._owner_addrs[i] = self._owner_addrs[last]
        self._owner_ids[i] = self._owner_ids[last]
        _ = self._ids.pop(last)
        _ = self._gens.pop(last)
        _ = self._scope_ptrs.pop(last)
        _ = self._owner_addrs.pop(last)
        _ = self._owner_ids.pop(last)

    def gen_for(self, id: Int) raises -> Int:
        var i = self._index_of(id)
        if i < 0:
            raise Error("TimeoutRegistry: unknown timer id " + String(id))
        return self._gens[i]

    def update_gen(mut self, id: Int, gen: Int) raises:
        """Re-point the LIVE generation for `id` (refresh_timeout's re-arm:
        the heap already supersedes the old entry via a fresh `arm` gen —
        this keeps the registry's bookkeeping in lockstep so a later
        `timeout_scope_driver` pop of the NEW arm is recognized live)."""
        var i = self._index_of(id)
        if i < 0:
            raise Error("TimeoutRegistry: unknown timer id " + String(id))
        self._gens[i] = gen

    def scope_ptr_for(self, id: Int) raises -> UnsafePointer[Scope, MutAnyOrigin]:
        var i = self._index_of(id)
        if i < 0:
            raise Error("TimeoutRegistry: unknown timer id " + String(id))
        return self._scope_ptrs[i]

    def owner_addr_for(self, id: Int) raises -> Int:
        var i = self._index_of(id)
        if i < 0:
            raise Error("TimeoutRegistry: unknown timer id " + String(id))
        return self._owner_addrs[i]

    def owner_id_for(self, id: Int) raises -> Int:
        var i = self._index_of(id)
        if i < 0:
            raise Error("TimeoutRegistry: unknown timer id " + String(id))
        return self._owner_ids[i]


def make_timeout_registry() -> TimeoutRegistry:
    """Fresh, empty, caller-owned (timer id -> scope-deadline arm) registry."""
    return TimeoutRegistry()


# ---------------------------------------------------------------------------
# TimeoutScope — a nested Scope + its one armed monotonic deadline
# ---------------------------------------------------------------------------


struct TimeoutScope(Movable, ImplicitlyDeletable):
    """A child `Scope` (spec §29.1 cancellation-tree member, nested under a
    caller's parent scope) paired with the ONE monotonic deadline armed on
    the shared `TimerHeap` that auto-cancels it.  Movable, NOT implicitly
    copyable (it embeds a Scope, which is Movable-only itself) — construct
    exclusively via the module factory `open_timeout` below, never via a
    bare `TimeoutScope(...)` call (see module header: the embedded Scope's
    address must be the caller's FINAL binding before the parent scope's
    cancellation-tree registry captures a pointer to it)."""

    var _scope: Scope
    # The armed timer's id (== this TimeoutScope's own scope handle — one
    # id space, no extra allocation: scope handles already come from the
    # caller's monotonic `rt.next_id()`, so reusing it as the timer id is
    # collision-free with every OTHER timer/task id in the same heap).
    var _timer_id: Int
    # The CURRENT live generation token for `_timer_id` (bumped by every
    # `refresh_timeout`; stale pops of a superseded gen are skipped by the
    # driver exactly like a plain sleep's stale-generation defense).
    var _gen: Int

    def __init__(
        out self,
        handle: Int,
        parent: UnsafePointer[Scope, MutAnyOrigin],
        owner_addr: Int,
        owner_id: Int,
        deadline: UInt64,
        mut heap: TimerHeap,
        mut registry: TimeoutRegistry,
        order_log: UnsafePointer[List[Int], MutAnyOrigin],
        has_log: Bool,
    ) raises:
        """Nest the child scope under `parent` (refuses a closed/cancelled
        parent or a duplicate handle — `scope.validate_nest`'s shared
        checks, issue #54), arm the ONE deadline (`heap.arm` grants the
        fresh generation), and register the arm so the expiry hook can
        recognize it.  `owner_addr`/`owner_id` are the parked owner's TCB
        address and task id (plain Ints, not a typed JoinHandle: this
        constructor stays non-generic; `open_timeout[R]` below is the
        typed, generic public factory).

        Constructs `self._scope` DIRECTLY (`Scope(...)`, module header
        COMPILER QUIRK note) rather than chaining through `make_nested_
        scope`'s own out-param — `attach_child_scope`/`registry.register`
        only capture `UnsafePointer(to=self._scope)` AFTER the field has
        settled at `self`'s (this constructor's out-param) stable final
        address."""
        validate_nest(parent, handle)
        var with_log: Optional[UnsafePointer[List[Int], MutAnyOrigin]]
        if has_log:
            with_log = Optional[UnsafePointer[List[Int], MutAnyOrigin]](
                order_log
            )
        else:
            with_log = Optional[UnsafePointer[List[Int], MutAnyOrigin]]()
        var with_parent = Optional[UnsafePointer[Scope, MutAnyOrigin]](
            parent
        )
        self._scope = Scope(handle, with_log, with_parent)
        self._timer_id = handle
        self._gen = heap.arm(handle, owner_addr, deadline)
        var self_scope_ptr = UnsafePointer[Scope, MutAnyOrigin](
            to=self._scope
        )
        attach_child_scope(parent, handle, self_scope_ptr)
        registry.register(
            handle, self._gen, self_scope_ptr, owner_addr, owner_id
        )

    # --- queries -------------------------------------------------------

    def handle(self) -> Int:
        return self._scope.handle()

    def timer_id(self) -> Int:
        return self._timer_id

    def gen(self) -> Int:
        return self._gen

    def scope_ptr(mut self) -> UnsafePointer[Scope, MutAnyOrigin]:
        """The nested Scope BY POINTER — spawn/register/close call sites."""
        return UnsafePointer[Scope, MutAnyOrigin](to=self._scope)

    def is_timed_out(self) -> Bool:
        """True once the deadline scope has been cancelled — by ITS OWN
        expiry, by an ancestor's cancel, or by a sibling failure's cancel
        cascade (spec §29.1): `Scope.is_cancelled()` is the single source
        of truth for "this deadline scope is done being useful"."""
        return self._scope.is_cancelled()


def open_timeout[R: ResultValue](
    handle: Int,
    parent: UnsafePointer[Scope, MutAnyOrigin],
    owner: JoinHandle[R],
    deadline: UInt64,
    mut heap: TimerHeap,
    mut registry: TimeoutRegistry,
    order_log: UnsafePointer[List[Int], MutAnyOrigin],
    has_log: Bool,
    out self: TimeoutScope,
) raises:
    """PUBLIC typed factory (issue #84): create a child Scope nested under
    `parent`, arm one monotonic deadline for `owner` (the task to resume —
    TimeoutError on expiry, its own result on early completion), and
    register the arm in `registry`.  `handle` is a caller-allocated id
    (`rt.next_id()`, matching `make_scope`/`make_nested_scope`'s own
    convention — this factory does not itself touch `Runtime`).

    OUT-PARAM factory: `var ts = open_timeout(...)` constructs `TimeoutScope`
    IN PLACE at the caller's binding (chains into `TimeoutScope.__init__`,
    which places the embedded Scope there too — see module header)."""
    self = TimeoutScope(
        handle,
        parent,
        Int(owner.tcb()),
        owner.id(),
        deadline,
        heap,
        registry,
        order_log,
        has_log,
    )


def refresh_timeout(
    mut ts: TimeoutScope,
    mut heap: TimerHeap,
    mut registry: TimeoutRegistry,
    new_deadline: UInt64,
) raises:
    """Re-arm `ts`'s deadline (issue #84 plan point 5): opportunistically
    `cancel_token`s the OLD (timer id, gen) pair — a no-op if it already
    lost its liveness to an earlier refresh or expiry, and INCAPABLE of
    touching a NEWER arm even if this call races one (cancel_token's exact
    (id, gen) match) — then arms a FRESH deadline (a new generation,
    superseding any surviving stale entry regardless) and updates both
    `ts`'s own generation and the registry's bookkeeping so the driver
    recognizes the new arm as live.  "Cancel-by-refresh cannot disturb a
    live deadline; a stale arm never releases the scope" (issue #84
    acceptance)."""
    var owner_addr = registry.owner_addr_for(ts.timer_id())
    _ = heap.cancel_token(ts.timer_id(), ts.gen())
    var fresh_gen = heap.arm(ts.timer_id(), owner_addr, new_deadline)
    ts._gen = fresh_gen
    registry.update_gen(ts.timer_id(), fresh_gen)


# ---------------------------------------------------------------------------
# timeout_scope_driver — the expiry-pass service hook (issue #39 parity)
# ---------------------------------------------------------------------------


def timeout_scope_driver[R: ResultValue](
    mut rt: Runtime,
    mut heap: TimerHeap,
    mut registry: TimeoutRegistry,
    now: UInt64,
) raises -> Int:
    """Deadline-integration hook (issue #84): pops every timer due at `now`
    in deadline order — SAME expiry pass `service_timers` drives (issue #39
    single-source park/wake: this hook SUBSUMES it, so a heap mixing plain
    sleeps and scope-deadline arms needs only ONE service call).  Returns
    the number of tasks woken.

    Per popped entry:
      - a STALE generation (superseded by a refresh, or already consumed)
        is dropped, exactly like `service_timers` — never wakes, never
        touches the registry;
      - a REGISTERED scope-deadline arm whose scope is not yet cancelled
        AND whose owner is not yet COMPLETED: cancels every child under the
        scope (recursive, spec §29.1 — nested subscopes included, and the
        cancel cascades UPWARD to any ancestor scope too), stamps the
        owner's erased TCB_Prefix failure slot with a `TimeoutError`
        (`mark_failed` — the EXISTING slot, no new ResultValue type), and
        wakes the owner EXACTLY ONCE (`unpark_current`) if it is WAITING;
      - a REGISTERED arm whose scope is ALREADY cancelled (an earlier
        failure's cancel, or an ancestor/sibling deadline's cascade — see
        module header) or whose owner already COMPLETED is a NO-OP: the
        timeout LOSES to whatever already resolved it — never a double
        mark, never a double wake, never clobbers a settled result;
      - an UNREGISTERED entry is a plain (non-scope) timer: the canonical
        park/wake (`service_timers`'s own behavior, reproduced here so one
        hook drives the whole heap)."""
    var woke = 0
    while heap.has_due(now):
        var e = heap.pop_min()
        if heap.live_gen(e.id) != e.gen:
            continue  # stale generation — superseded/cancelled arm, drop
        if registry.is_registered(e.id):
            var sp = registry.scope_ptr_for(e.id)
            var owner_id = registry.owner_id_for(e.id)
            var h = JoinHandle[R](
                UnsafePointer[TaskControlBlock[R], MutAnyOrigin](
                    unsafe_from_address=e.tcbaddr
                ),
                owner_id,
            )
            if sp[].is_cancelled() or h.tcb()[].is_completed():
                continue  # already resolved — the timeout loses, no-op
            sp[].request_cancel_all()
            h.tcb()[].mark_failed(
                "TimeoutError: deadline scope "
                + String(sp[].handle())
                + " expired"
            )
            var gen = h.tcb()[].wait_node()[].generation()
            var was_waiting = h.tcb()[].state() == TaskControlBlock.WAITING
            unpark_current(rt, h, required_gen=gen)
            if was_waiting and h.tcb()[].state() == TaskControlBlock.RUNNABLE:
                woke += 1
            continue
        # plain (non-scope) timer: service_timers' own wake, reproduced so
        # this hook is a drop-in replacement over a mixed heap.
        var h2 = JoinHandle[R](
            UnsafePointer[TaskControlBlock[R], MutAnyOrigin](
                unsafe_from_address=e.tcbaddr
            ),
            e.id,
        )
        var gen2 = h2.tcb()[].wait_node()[].generation()
        var was_waiting2 = h2.tcb()[].state() == TaskControlBlock.WAITING
        unpark_current(rt, h2, required_gen=gen2)
        if was_waiting2 and h2.tcb()[].state() == TaskControlBlock.RUNNABLE:
            woke += 1
    return woke


def timeout_drive_step[
    F: def(mut Runtime, Int, Int, BytePtr) raises -> Int,
    R: ResultValue = Nil,
](
    mut rt: Runtime,
    dispatcher: F,
    ud: BytePtr,
    mut heap: TimerHeap,
    mut registry: TimeoutRegistry,
    now: UInt64,
) raises -> Int:
    """One scheduler service step over a timeout-aware heap (issue #84,
    `time.drive_step` parity): drive all READY tasks (A1.1 scheduler_loop)
    then service due timers/scope-deadlines via `timeout_scope_driver`.
    Returns total records served + waiters woken."""
    var served = scheduler_loop(rt, dispatcher, ud)
    var woke = timeout_scope_driver[R](rt, heap, registry, now)
    return served + woke
