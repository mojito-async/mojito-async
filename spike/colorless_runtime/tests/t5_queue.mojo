# A0-T5 (issue #14) — pure-Mojo FIFO runnable queue (A0.5 lane)
#
# Spec §100 A0.5: struct FifoQueue[T] with push/pop/len/is_empty/clear on a
# single worker (NO lock-free structures, NO work stealing, NO scheduler).
# The queue is payload-neutral (review fold): equal values enqueue freely;
# identity enqueue-once belongs to the scheduler/Event claim (A0.6/A0.7).
#
# This file is TDD-RED first (compile/run reflects an absent queue.mojo),
# then green once queue.mojo lands. Pure `mojo run`, no dylib.

from queue import FifoQueue


# Small struct element: exercises the generic over a non-scalar Movable,
# Equatable, ImplicitlyCopyable payload while staying allocation-cheap.
struct Item(Movable, Equatable, ImplicitlyCopyable):
    var a: Int
    var b: Int

    def __init__(out self, a: Int, b: Int):
        self.a = a
        self.b = b


def main() raises:
    var failures = List[String]()
    var n_fail = 0

    # ---- FIFO order: push 1..N, pop 1..N ---------------------------
    var q = FifoQueue[Int]()
    for i in range(5):
        q.push(i)
    var order_ok = True
    for i in range(5):
        var v = q.pop()
        if v != i:
            order_ok = False
    if not order_ok:
        failures.append("FIFO order violated")
        n_fail += 1

    # ---- empty pop raises ------------------------------------------
    var empty_ok = False
    try:
        _ = q.pop()
    except:
        empty_ok = True
    if not empty_ok:
        failures.append("empty pop did not raise")
        n_fail += 1

    # ---- len / is_empty ---------------------------------------------
    var q2 = FifoQueue[Int]()
    var len_ok = q2.is_empty() and len(q2) == 0
    q2.push(1)
    len_ok = len_ok and (not q2.is_empty()) and len(q2) == 1
    q2.push(2)
    len_ok = len_ok and len(q2) == 2
    if not len_ok:
        failures.append("len/is_empty wrong")
        n_fail += 1

    # ---- dup values queue fine (queue is payload-neutral; identity
    # ---- enqueue-once belongs to the scheduler/Event claim, A0.6/A0.7)
    var qd = FifoQueue[Int]()
    qd.push(7)
    qd.push(7)
    var dup_ok = len(qd) == 2
    qd.pop()
    var distinct_ok = len(qd) == 1
    if not dup_ok:
        failures.append("equal values did not both queue")
        n_fail += 1
    if not distinct_ok:
        failures.append("pop after equal-value push wrong")
        n_fail += 1
    # Re-enqueue after pop is legal (drained element reusable).
    qd.push(7)
    if len(qd) != 2:
        failures.append("re-enqueue after pop failed")
        n_fail += 1

    # ---- struct elements round trip -----------------------------------
    var qm = FifoQueue[Item]()
    for i in range(3):
        qm.push(Item(i, i * 2))
    var mok = True
    for i in range(3):
        var mres = qm.pop()
        if mres.a != i or mres.b != i * 2:
            mok = False
    if not mok:
        failures.append("struct element order corrupted")
        n_fail += 1

    # ---- interleaved push/pop wrap-around ------------------------------
    var qw = FifoQueue[Item]()
    for i in range(64):
        qw.push(Item(i, i))
    var wrap_ok = True
    for i in range(200000):
        var v = qw.pop()
        if v.a != i:
            wrap_ok = False
        qw.push(Item(i + 64, 0))
    if not wrap_ok:
        failures.append("interleaved push/pop wrap corrupted order")
        n_fail += 1

    # ---- 100k push/pop stress of small structs ----------------------------
    var N = 100000
    var qs = FifoQueue[Item]()
    for i in range(N):
        qs.push(Item(i, i + 1))
    var stress_ok = True
    for i in range(N):
        var v = qs.pop()
        if v.a != i or v.b != i + 1:
            stress_ok = False
    if not stress_ok:
        failures.append("100k stress round-trip corrupted")
        n_fail += 1
    if len(qs) != 0:
        failures.append("100k stress left residue")
        n_fail += 1

    # ---- pop after clear re-raises (queue stays usable) -------------------
    var qc = FifoQueue[Int]()
    qc.push(1)
    qc.push(2)
    qc.clear()
    var after_clear_ok = False
    try:
        _ = qc.pop()
    except:
        after_clear_ok = True
    # And it accepts fresh pushes after clear.
    qc.push(3)
    var after_clear_push = (len(qc) == 1) and (qc.pop() == 3)
    if not after_clear_ok:
        failures.append("pop after clear did not raise")
        n_fail += 1
    if not after_clear_push:
        failures.append("push after clear failed")
        n_fail += 1

    if n_fail == 0:
        print("T5 FIFO queue: PASS")
    else:
        print("T5 FIFO queue: FAIL (" + String(n_fail) + ")")
        for f in failures:
            print("  - " + f)