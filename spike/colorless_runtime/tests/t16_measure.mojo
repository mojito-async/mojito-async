# A0-T16 (issue #19) — allocation + latency measurement infrastructure
#
# Spec A0-T16 + §A0.7 table: the measurement lane provides
#   - LatencyTimer:     start/stop around a region; ns deltas kept in a
#                       FIXED InlineArray capacity (documented cap, no heap
#                       growth); statistical helpers min/p50/p95/max.
#   - AllocAccounter:   explicit account(event, bytes) API accumulating
#                       (event, bytes, count) rows in a fixed-size table;
#                       deterministic report(); totals_by_event();
#                       assert_zero_allocs(accounter, event).
#   - JSONL emitter:    emit(path, rows) — one JSON object per line,
#                       hand-rolled string building incl. quote/backslash
#                       escaping; exercised with an in-test round-trip
#                       parse (hand-rolled, no JSON lib).
#   - ClockSource:      monotonic ns. NOTE (spike limitation): Mojo
#                       1.0.0b2's std.time exposes only sleep(); there is
#                       no std.time clock surface, so ClockSource reads
#                       CLOCK_MONOTONIC through @extern("clock_gettime").
#
# Pure `mojo run`, no dylib. Timing assertions use generous bounds only
# (stop >= start, min <= p50 <= p95 <= max, elapsed >= floor after sleep)
# so the driver stays deterministic. This file is TDD-RED first: it does
# not compile until measure.mojo lands.

from std.io import FileHandle
from std.time import sleep

from measure import (
    AllocAccounter,
    ClockSource,
    LatencyTimer,
    assert_zero_allocs,
    emit,
    jsonl_escape,
    jsonl_row,
    make_alloc_accounter,
    make_clock_source,
    make_latency_timer,
)


@extern("exit")
def _c_exit(code: Int32) abi("C"):
    ...


@extern("remove")
def _c_remove(path: UnsafePointer[Byte, ImmutAnyOrigin]) abi("C") -> Int32:
    ...


# Hand-rolled line splitter for the JSONL round-trip parse: cut on '\n',
# drop a trailing empty piece after the last newline.
def split_lines(s: String) -> List[String]:
    var out = List[String]()
    var start = 0
    var i = 0
    var n = s.byte_length()
    while i < n:
        if s[byte=i] == "\n":  # newline byte
            out.append(String(s[byte=start:i]))
            start = i + 1
        i += 1
    if start < n:
        out.append(String(s[byte=start:]))
    return out^


# Hand-rolled extraction of the "event" value from a JSONL line produced
# by jsonl_row(): locate '"event":"', copy until the closing quote.
def extract_event_value(line: String) -> String:
    var key = "\"event\":\""
    var klen = key.byte_length()
    var n = line.byte_length()
    var pos = -1
    var i = 0
    while i + klen <= n:
        if String(line[byte=i:i+klen]) == key:
            pos = i + klen
            break
        i += 1
    if pos == -1:
        return String("")
    var end = pos
    while end < n:
        # closing unescaped quote
        if line[byte=end] == "\"" and not (line[byte=end - 1] == "\\"):
            break
        end += 1
    return String(line[byte=pos:end])


def main() raises:
    var failures = List[String]()
    var n_fail = 0

    # ---- ClockSource: monotonic non-decreasing, strictly advancing ----
    var clock = make_clock_source()
    var t0 = clock.now()
    var t1 = clock.now()
    if t1 < t0:
        failures.append("clock went backwards")
        n_fail += 1
    sleep(0.005)
    var t2 = clock.now()
    # Generous floor: a 5 ms sleep must yield at least ~1 ms of progress.
    if not (t2 > t1 and (t2 - t1) >= 1_000_000):
        failures.append("clock did not advance across sleep")
        n_fail += 1

    # ---- LatencyTimer: region timing + statistics ----------------------
    var timer = make_latency_timer()
    var acc = 0
    var last_delta_ok = True
    for i in range(8):
        timer.start()
        for j in range(1000):
            acc += j * i
        var d = timer.stop()
        if d < 0:
            last_delta_ok = False
    _ = acc
    if not last_delta_ok:
        failures.append("negative latency sample")
        n_fail += 1
    if timer.count() != 8:
        failures.append("sample count wrong")
        n_fail += 1

    var mn = timer.min_ns()
    var p50 = timer.p50_ns()
    var p95 = timer.p95_ns()
    var mx = timer.max_ns()
    if not (mn <= p50 and p50 <= p95 and p95 <= mx):
        failures.append("statistics ordering violated")
        n_fail += 1
    if mn < 0 or mx < mn:
        failures.append("statistics bounds violated")
        n_fail += 1

    # Fixed capacity: filling past the documented cap must raise, never grow.
    var cap_ok = False
    try:
        while True:
            timer.start()
            _ = timer.stop()
    except:
        cap_ok = True
    if not cap_ok:
        failures.append("capacity overflow did not raise")
        n_fail += 1
    if timer.count() != timer.capacity():
        failures.append("capacity bookkeeping wrong after overflow")
        n_fail += 1

    # stop() without start() raises.
    var nostart_ok = False
    var t_errless = make_latency_timer()
    try:
        _ = t_errless.stop()
    except:
        nostart_ok = True
    if not nostart_ok:
        failures.append("stop without start did not raise")
        n_fail += 1

    # ---- AllocAccounter: explicit accounting API -----------------------
    var accounter = make_alloc_accounter()
    accounter.account("queue.push", 64)
    accounter.account("queue.push", 32)
    accounter.account("timer.stop", 8)

    var totals_ok = (
        accounter.total_bytes() == 104
        and accounter.total_for("queue.push") == 96
        and accounter.count_for("queue.push") == 2
        and accounter.total_for("timer.stop") == 8
        and accounter.total_for("never.accounted") == 0
        and accounter.event_count() == 2
    )
    if not totals_ok:
        failures.append("alloc totals wrong")
        n_fail += 1

    # Deterministic report: identical across calls, names present.
    var rep1 = accounter.report()
    var rep2 = accounter.report()
    if rep1 != rep2:
        failures.append("report not deterministic")
        n_fail += 1
    if not (rep1.find("queue.push") != -1 and rep1.find("96") != -1):
        failures.append("report missing rows/totals")
        n_fail += 1

    var by_event = accounter.totals_by_event()
    if not (by_event.find("queue.push") != -1 and by_event.find("96") != -1):
        failures.append("totals_by_event missing rows/totals")
        n_fail += 1

    # Zero-allocation assertion helper.
    if not assert_zero_allocs(accounter, "never.accounted"):
        failures.append("assert_zero_allocs false negative")
        n_fail += 1
    if assert_zero_allocs(accounter, "queue.push"):
        failures.append("assert_zero_allocs false positive")
        n_fail += 1

    # Fixed-size table: more distinct events than MAX_EVENTS raises.
    var full_ok = False
    try:
        for i in range(4096):
            accounter.account("evt_" + String(i), 1)
    except:
        full_ok = True
    if not full_ok:
        failures.append("table overflow did not raise")
        n_fail += 1

    # ---- JSONL emitter: escape + emit + round-trip parse ---------------
    if jsonl_escape("plain") != "plain":
        failures.append("escape mangled plain text")
        n_fail += 1
    if jsonl_escape("a\"b\\c\nd") != "a\\\"b\\\\c\\nd":
        failures.append("escape missed quote/backslash/newline")
        n_fail += 1
    # Control bytes must become \u00XX (RFC 8259), not raw bytes.
    var ctl_in = String(Codepoint(0x01))
    var ctl = jsonl_escape(ctl_in)
    if ctl != "\\u0001":
        failures.append("control char not \\u-escaped")
        n_fail += 1
    if jsonl_escape(String(Codepoint(0x1B))) != "\\u001b":
        failures.append("ESC not \\u-escaped")
        n_fail += 1

    var row = jsonl_row("ev1", 10, 2)
    if row != "{\"event\":\"ev1\",\"bytes\":10,\"count\":2}":
        failures.append("jsonl_row layout wrong")
        n_fail += 1

    var rows = List[String]()
    rows.append(jsonl_row("ev1", 10, 2))
    rows.append(jsonl_row("tricky\"name\\\nline", 7, 3))
    rows.append(jsonl_row("ev3", 0, 1))

    # Emit inside the repo tests dir during the run only; removed below.
    var out_path = "spike/colorless_runtime/tests/t16_measure_out.jsonl"
    var emitted = False
    try:
        emit(out_path, rows)
        emitted = True
    except:
        pass
    if not emitted:
        # Fallback when cwd differs (e.g. invoked from inside tests/).
        out_path = "t16_measure_out.jsonl"
        emit(out_path, rows)

    # Round-trip: read back, hand-parse, validate structure + content.
    var rh = FileHandle(out_path, "r")
    var blob = rh.read(65536)
    _ = rh.close()

    var lines = split_lines(blob)
    if len(lines) != 3:
        failures.append("round-trip line count wrong")
        n_fail += 1
    else:
        if lines[0] != row:
            failures.append("round-trip line 0 mismatch")
            n_fail += 1
        if lines[2] != "{\"event\":\"ev3\",\"bytes\":0,\"count\":1}":
            failures.append("round-trip line 2 mismatch")
            n_fail += 1
        # Structural hand-parse: object envelope + event field round-trips.
        var l1 = lines[1]
        if not (l1.startswith("{\"event\":\"") and l1.endswith("}")):
            failures.append("round-trip line 1 envelope wrong")
            n_fail += 1
        var ev_back = extract_event_value(l1)
        if ev_back != jsonl_escape("tricky\"name\\\nline"):
            failures.append("round-trip escaped event mismatch")
            n_fail += 1

    _ = _c_remove(out_path.unsafe_ptr())

    if n_fail == 0:
        print("T16 measurement infra: PASS")
    else:
        print("T16 measurement infra: FAIL (" + String(n_fail) + ")")
        for f in failures:
            print("  * " + f)
        _c_exit(1)
