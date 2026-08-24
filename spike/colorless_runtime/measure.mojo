# A0.10 (issue #19) — allocation + latency measurement infrastructure.
#
# Pure-Mojo measurement kit for the colorless runtime spike (spec A0-T16,
# §A0.7 table). Everything lives in structs — there are deliberately NO
# module-level mutable globals: later lanes EMBED an AllocAccounter per
# subsystem (scheduler, fiber pool, queue, ...) instead of sharing one
# global counter, so accounting stays allocation-free and single-worker.
#
# Components:
#   ClockSource     monotonic nanoseconds. SPIKE LIMITATION: Mojo
#                   1.0.0b2's std.time exposes only sleep(); it has no
#                   clock surface (no now/instant/MonotonicClock), so
#                   monotonic time is read through @extern("clock_gettime")
#                   with Darwin's CLOCK_MONOTONIC (= 6). Wall-clock-only
#                   would be a documented fallback on non-Darwin hosts;
#                   this lane targets the darwin/arm64 spike host.
#   LatencyTimer    start()/stop() around a region; ns deltas accumulate
#                   into a FIXED InlineArray capacity (no heap growth);
#                   min/p50/p95/max helpers over recorded samples.
#   AllocAccounter  explicit account(event, bytes); fixed-size table of
#                   (event, bytes_total, count) rows keyed by event name in
#                   insertion order; deterministic report(); totals_by_event();
#                   assert_zero_allocs(accounter, event).
#   JSONL emitter   jsonl_escape/jsonl_row build lines by hand (quote,
#                   backslash, \n, \r, \t escaping); emit(path, rows)
#                   writes one JSON object per line via std.io FileHandle.
#                   Files are written by TESTS only, inside the repo tests
#                   dir, and removed afterwards.
#
# Mojo 1.0.0b2 dialect notes:
#   - `def` only; mutable instance methods take `mut self`; constructors
#     take `out self`. NO static methods in structs -> construction goes
#     through module-level factories (make_* below).
#   - C symbols: @extern("<sym>") + abi("C") between parameter list and
#     result type + `...` body; UnsafePointer origins must be concrete in
#     extern signatures (ImmutAnyOrigin accepts literal + heap strings).
from std.io import FileHandle


# ---------------------------------------------------------------------------
# ClockSource
# ---------------------------------------------------------------------------

struct Timespec(ImplicitlyCopyable, ImplicitlyDeletable):
    """Layout-compatible with C struct timespec (tv_sec, tv_nsec)."""
    var tv_sec: Int64
    var tv_nsec: Int64

    def __init__(out self):
        self.tv_sec = 0
        self.tv_nsec = 0


@extern("clock_gettime")
def _clock_gettime(
    clk_id: Int32, tp: UnsafePointer[Timespec, MutAnyOrigin]
) abi("C") -> Int32:
    ...


struct ClockSource(ImplicitlyCopyable, ImplicitlyDeletable):
    """Monotonic nanosecond clock (spike: Darwin CLOCK_MONOTONIC_RAW via libc).

    std.time in 1.0.0b2 has no clock API, hence the extern. now() never
    goes backwards within a boot. CLOCK_MONOTONIC_RAW (=4) ticks ~42ns on
    darwin/arm64 (mach_absolute_time timebase, unslewable); plain
    CLOCK_MONOTONIC (=6) only advances in ~1us steps on this host.
    Non-Darwin hosts would need the platform's monotonic id instead.
    """

    comptime CLOCK_MONOTONIC = Int32(4)
    def __init__(out self):
        pass

    def now(self) raises -> Int:
        var ts = Timespec()
        var rc = _clock_gettime(
            Self.CLOCK_MONOTONIC, UnsafePointer[Timespec, MutAnyOrigin](to=ts)
        )
        if rc != 0:
            raise Error("ClockSource.now: clock_gettime failed")
        return Int(ts.tv_sec) * 1_000_000_000 + Int(ts.tv_nsec)


def make_clock_source() -> ClockSource:
    """Factory (b2 structs have no static methods)."""
    return ClockSource()


# ---------------------------------------------------------------------------
# LatencyTimer
# ---------------------------------------------------------------------------

struct LatencyTimer(ImplicitlyCopyable, ImplicitlyDeletable):
    """Region latency sampler with FIXED sample capacity.

    Samples are Int ns deltas stored in a stack/inline InlineArray of
    comptime capacity SAMPLE_CAPACITY — recording NEVER grows the heap;
    stop() past capacity raises instead. Statistics are insertion-sort
    based over a copy (fine at spike scale, deterministic).

    p50/p95 use index (n * pct) // 100 clamped to n-1 over ascending sort.

    Overhead note: each start/stop pair performs two extern clock reads
    plus bookkeeping inside the measured envelope (~100-200ns). For
    sub-microsecond regions subtract this fixed cost or batch K iterations
    per sample; raw p50/p95 are upper bounds for tiny regions.
    """

    comptime SAMPLE_CAPACITY = 256

    var _clock: ClockSource
    var _samples: InlineArray[Int, Self.SAMPLE_CAPACITY]
    var _n: Int
    var _start_ns: Int

    def __init__(out self):
        self._clock = make_clock_source()
        self._samples = InlineArray[Int, Self.SAMPLE_CAPACITY](fill=0)
        self._n = 0
        self._start_ns = -1

    def capacity(self) -> Int:
        return Self.SAMPLE_CAPACITY

    def count(self) -> Int:
        return self._n

    def is_running(self) -> Bool:
        return self._start_ns >= 0

    def start(mut self) raises:
        """Open a timing region."""
        self._start_ns = self._clock.now()

    def stop(mut self) raises -> Int:
        """Close the region, record the delta, return it in ns.

        Raises when no region is open or the fixed sample buffer is full.
        """
        if self._start_ns < 0:
            raise Error("LatencyTimer.stop: stop without start")
        var delta = self._clock.now() - self._start_ns
        self._start_ns = -1
        if self._n >= Self.SAMPLE_CAPACITY:
            raise Error("LatencyTimer.stop: sample capacity exceeded")
        self._samples[self._n] = delta
        self._n += 1
        return delta

    def _sorted(self) -> InlineArray[Int, Self.SAMPLE_CAPACITY]:
        """Ascending copy of recorded samples (insertion sort)."""
        var out = self._samples  # value copy of the inline storage
        var i = 1
        while i < self._n:
            var key = out[i]
            var j = i - 1
            while j >= 0 and out[j] > key:
                out[j + 1] = out[j]
                j -= 1
            out[j + 1] = key
            i += 1
        return out

    def _stat(self, pct: Int) raises -> Int:
        if self._n == 0:
            raise Error("LatencyTimer: no samples recorded")
        var s = self._sorted()
        var idx = (self._n * pct) // 100
        if idx > self._n - 1:
            idx = self._n - 1
        return s[idx]

    def min_ns(self) raises -> Int:
        return self._stat(0)

    def p50_ns(self) raises -> Int:
        return self._stat(50)

    def p95_ns(self) raises -> Int:
        return self._stat(95)

    def max_ns(self) raises -> Int:
        return self._stat(100)


def make_latency_timer() -> LatencyTimer:
    """Factory (b2 structs have no static methods)."""
    return LatencyTimer()


# ---------------------------------------------------------------------------
# AllocAccounter
# ---------------------------------------------------------------------------

struct AllocEventRow(ImplicitlyCopyable, ImplicitlyDeletable):
    """One aggregated accounting row: distinct event -> bytes total, count."""
    var name: String
    var bytes_total: Int
    var count: Int

    def __init__(out self):
        self.name = String("")
        self.bytes_total = 0
        self.count = 0


struct AllocAccounter(ImplicitlyCopyable, ImplicitlyDeletable):
    """Explicit byte/count accounting table (fixed size, insertion order).

    Rows aggregate BY EVENT NAME: repeated account(event, bytes) calls fold
    into the existing row, so the table holds up to MAX_EVENTS DISTINCT
    events and account() raises once full — no heap growth, ever. Later
    lanes embed one AllocAccounter per subsystem rather than sharing a
    global; report() emits deterministic lines because iteration follows
    first-accounted order.
    """

    comptime MAX_EVENTS = 64

    var _rows: InlineArray[AllocEventRow, Self.MAX_EVENTS]
    var _n: Int
    var _total_bytes: Int

    def __init__(out self):
        self._rows = InlineArray[AllocEventRow, Self.MAX_EVENTS](
            fill=AllocEventRow()
        )
        self._n = 0
        self._total_bytes = 0

    def event_count(self) -> Int:
        return self._n

    def total_bytes(self) -> Int:
        return self._total_bytes

    def account(mut self, event: String, bytes: Int) raises:
        """Record an allocation event; folds into the row for `event`."""
        var i = 0
        while i < self._n:
            if self._rows[i].name == event:
                self._rows[i].bytes_total += bytes
                self._rows[i].count += 1
                self._total_bytes += bytes
                return
            i += 1
        if self._n >= Self.MAX_EVENTS:
            raise Error("AllocAccounter.account: table capacity exceeded")
        self._rows[self._n].name = String(event)
        self._rows[self._n].bytes_total = bytes
        self._rows[self._n].count = 1
        self._n += 1
        self._total_bytes += bytes

    def _find(self, event: String) -> Int:
        var i = 0
        while i < self._n:
            if self._rows[i].name == event:
                return i
            i += 1
        return -1

    def total_for(self, event: String) -> Int:
        var i = self._find(event)
        if i == -1:
            return 0
        return self._rows[i].bytes_total

    def count_for(self, event: String) -> Int:
        var i = self._find(event)
        if i == -1:
            return 0
        return self._rows[i].count

    def report(self) -> String:
        """Deterministic multi-line report, one line per known event."""
        var out = String("")
        var i = 0
        while i < self._n:
            var r = self._rows[i]
            out = (
                out + "event=" + r.name + " bytes=" + String(r.bytes_total)
                + " count=" + String(r.count) + "\n"
            )
            i += 1
        return out

    def totals_by_event(self) -> String:
        """Deterministic event->total-bytes map, one '<event> <B>' per line."""
        var out = String("")
        var i = 0
        while i < self._n:
            var r = self._rows[i]
            out = out + r.name + " " + String(r.bytes_total) + "\n"
            i += 1
        return out


def assert_zero_allocs(accounter: AllocAccounter, event: String) -> Bool:
    """Zero-allocation assertion helper: True iff `event` recorded nothing."""
    return accounter.total_for(event) == 0


def make_alloc_accounter() -> AllocAccounter:
    return AllocAccounter()


# ---------------------------------------------------------------------------
# JSONL emitter
# ---------------------------------------------------------------------------

def jsonl_escape(s: String) raises -> String:
    """Hand-rolled JSON string escaping: backslash, quote, control chars.

    Codepoint-wise scan; ASCII control bytes outside the named escapes are
    emitted as the 6-char sequence backslash-u-00XX per RFC 8259.
    """
    var out = String("")
    var hexdigits = "0123456789abcdef"
    for cp in s.codepoints():
        var v = Int(cp)
        if v == 92:
            out = out + "\\\\"
        elif v == 34:
            out = out + "\\\""
        elif v == 10:
            out = out + "\\n"
        elif v == 13:
            out = out + "\\r"
        elif v == 9:
            out = out + "\\t"
        elif v < 0x20:
            out = out + "\\u00" + hexdigits[byte=v >> 4] + hexdigits[byte=v & 0xF]
        else:
            out = out + String(Codepoint(v))
    return out

def jsonl_row(event: String, bytes: Int, count: Int) raises -> String:
    """One JSONL measurement object: {"event":..,"bytes":N,"count":N}."""
    return (
        "{\"event\":\"" + jsonl_escape(event) + "\",\"bytes\":"
        + String(bytes) + ",\"count\":" + String(count) + "}"
    )


def emit(jsonl_path: String, rows: List[String]) raises:
    """Write rows to `jsonl_path`, one JSON object per line.

    Tests only: callers write inside the repo tests dir during a test run
    and remove the file afterwards. Raises on I/O failure.
    """
    var fh = FileHandle(jsonl_path, "w")
    for row in rows:
        fh.write(row + "\n")
    # b2 FileHandle.write/close return None and raise on I/O failure.
    fh.close()
