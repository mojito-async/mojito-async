# mojito_async/test/unit/t19_timer_heap.mojo
#
# A1.4 (issue #36) — the min-heap deliverable: insert/arm, expiry ordering
# (deadline-ordered pops), cancel (whole-id and exact-generation-token),
# min-peek, and generation-token stale-timer suppression.
#
# Acceptance (spec §31):
#   - arm() registers timers; pop_min()/collect_due() yield them in
#     MONOTONIC DEADLINE order (min-heap invariant);
#   - min_deadline() peeks the earliest without removing; NO_DEADLINE on an
#     empty heap;
#   - cancel(id) removes the whole timer (never fires) and clears the live
#     registration;
#   - generation tokens: a re-armed id gets a FRESH gen; BOTH physical
#     entries stay in the heap and expiry pops both — the service must
#     flag the superseded (stale) one via is_stale_against(live_gen);
#   - gen-checked cancel: a token for an OLD gen cannot cancel or disturb
#     the LIVE timer; the live-gen token removes its own timer.
#
# Verdict: exit 0 + "PASS"; any RED prints + raises (exit 1).
from mojito_async.time.timer_heap import (
    NO_DEADLINE,
    TimerEntry,
    TimerHeap,
)


def red(what: String) raises -> None:
    print("T19 timer heap: RED (" + what + ")")
    raise Error(what)


def main() raises:
    # ---- min-heap invariant: expiry order = deadline order ----------------
    var h = TimerHeap()
    if not h.is_empty():
        red("fresh heap not empty")
    if h.min_deadline() != NO_DEADLINE:
        red("empty heap min_deadline is not NO_DEADLINE")

    var g1 = h.arm(1, 0x100, 1000)
    var g2 = h.arm(2, 0x200, 300)
    var g3 = h.arm(3, 0x300, 700)
    if g1 == g2 or g2 == g3:
        red("arm did not grant distinct generation tokens")
    if h.size() != 3:
        red("heap size != 3 after arms")
    if h.min_deadline() != 300:
        red("min_deadline is not the earliest (300)")

    var e1 = h.pop_min()
    var e2 = h.pop_min()
    var e3 = h.pop_min()
    if e1.deadline != 300 or e2.deadline != 700 or e3.deadline != 1000:
        red("pop order is not monotonic deadline order")
    if e1.id != 2 or e2.id != 3 or e3.id != 1:
        red("pop order id mismatch")
    if not h.is_empty():
        red("heap not empty after draining")

    # ---- collect_due: ordered expiry pass ----------------------------------
    var h2 = TimerHeap()
    _ = h2.arm(10, 0xAA, 5000)
    _ = h2.arm(11, 0xBB, 1000)
    _ = h2.arm(12, 0xCC, 3000)
    var early = h2.collect_due(1500)
    if len(early) != 1 or early[0].id != 11:
        red("collect_due(1500) should yield exactly timer 11")
    var rest = h2.collect_due(5000)
    if len(rest) != 2 or rest[0].id != 12 or rest[1].id != 10:
        red("collect_due(5000) order wrong")
    if not h2.is_empty():
        red("collect_due did not drain")

    # ---- cancel(id): whole-timer removal -----------------------------------
    var h3 = TimerHeap()
    _ = h3.arm(20, 0x01, 100)
    _ = h3.arm(21, 0x02, 200)
    if not h3.cancel(20):
        red("cancel(20) reported no removal")
    if h3.size() != 1:
        red("cancel(20) left wrong size")
    if h3.live_gen(20) != 0:
        red("cancel(20) did not clear live registration")
    var d = h3.collect_due(10_000)
    if len(d) != 1 or d[0].id != 21:
        red("cancelled timer 20 still fired")

    # ---- generation suppression + gen-checked cancel -----------------------
    # Scenario A: re-arm of the same id leaves both physical entries; expiry
    # pops both and the (superseded) first must be flagged stale.
    var h4 = TimerHeap()
    var old_gen = h4.arm(9, 0x09, 100)   # superseded arm (stale later)
    var new_gen = h4.arm(9, 0x09, 900)  # fresh arm of the same id
    if h4.live_gen(9) != new_gen:
        red("live gen is not the most recent arm")
    if h4.size() != 2:
        red("expected both physical entries after re-arm")
    var due = h4.collect_due(1000)
    if len(due) != 2:
        red("expected 2 popped entries (stale + live)")
    if not due[0].is_stale_against(new_gen):
        red("superseded entry not flagged stale against live gen")
    if due[1].is_stale_against(new_gen):
        red("live entry wrongly flagged stale")

    # Scenario B: a bogus gen token cannot cancel the live timer; the live
    # gen token removes it.
    var h5 = TimerHeap()
    _ = h5.arm(7, 0x07, 50)
    var live = h5.live_gen(7)
    if h5.cancel_token(7, live - 1):
        red("bogus gen token cancelled the live timer")
    if h5.size() != 1:
        red("bogus token removed the live timer")
    if not h5.cancel_token(7, live):
        red("cancel_token with live gen failed to remove")
    if h5.size() != 0:
        red("live-token cancel left entries")

    # Scenario C: a stale token MAY remove its own superseded physical entry
    # but the live timer keeps firing (no cross-cancel).
    var h6 = TimerHeap()
    var ga = h6.arm(8, 0x08, 100)
    var gb = h6.arm(8, 0x08, 800)
    if not h6.cancel_token(8, ga):
        red("stale token could not remove its own superseded entry")
    if h6.size() != 1:
        red("stale-token removal disturbed the live timer")
    if h6.live_gen(8) != gb:
        red("stale-token removal poisoned the live gen")
    var rest2 = h6.collect_due(1000)
    if len(rest2) != 1 or rest2[0].gen != gb:
        red("live timer did not survive stale-token removal")
    if rest2[0].is_stale_against(gb):
        red("surviving timer wrongly flagged stale")

    print("T19 timer heap: PASS")