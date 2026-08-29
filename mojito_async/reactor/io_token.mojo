# mojito_async/reactor/io_token.mojo
#
# A7.2 reactor lane (issue #76) — operation tokens/generations.
#
# Every in-flight reactor operation gets an opaque `IoToken` carrying a
# slot index and a generation, so a stale readiness/completion delivery
# can never wake a recycled task or op slot — directly extending the C6
# exactly-one-winner park rule (task_control_block.wake_claim /
# runtime/park.mojo's required_gen) into the I/O domain (spec §27.1/§31).
#
# `IoToken.encode()`/`decode_token()` round-trip the (slot, generation)
# pair through the ONE UInt64 opaque `token` field the native poller
# preserves EXACTLY (spec §31; vendor/mojito_sys_io/poller.mojo's
# IoEvent.token) — packed generation-high/slot-low so a native delivery is
# ENTIRELY self-describing: `io_op_table.IoOpTable.deliver`-equivalent
# (`reactor/poller.mojo`'s `drain_ready`) never needs a side table to
# validate a wire token, only the op-table slot it names.
#
# IoOpKind is an OPEN SET of caller-defined operation tags (mirrors
# `SuspendReason`, `TCB_Prefix` state constants and every other "b2 has no
# enum sugar" comptime-Int family in this codebase): the reactor stores
# whatever kind the caller registered with and returns it UNCHANGED from
# drain — the network lanes (connect/accept/read/write, issues #77-#80)
# read it back to know which operation just became ready without a second
# table lookup.
struct IoOpKind:
    """Operation-kind tags stamped on an `IoToken`/`IoOpEntry` (open set —
    add more constants here as new lanes need them; never renumber an
    existing one, callers persist these across a register()/drain_ready()
    round trip)."""

    comptime NONE = Int(0)
    comptime READ = Int(1)
    comptime WRITE = Int(2)
    comptime CONNECT = Int(3)
    comptime ACCEPT = Int(4)


# ---------------------------------------------------------------------------
# IoToken — the opaque (slot, generation, op_kind) handle.
# ---------------------------------------------------------------------------

struct IoToken(ImplicitlyCopyable, ImplicitlyDeletable):
    """Opaque handle to one live registration in an `IoOpTable` (issue
    #76).  `slot` is the table index; `generation` is the epoch counter
    for that slot, bumped by every `IoOpTable.allocate()` — the table's
    per-slot counter starts at 0, so a slot's first-ever use yields
    generation 1 (a trivial "reuse" with no prior consumer to protect
    against) and every SUBSEQUENT reuse after a `release()` bumps it
    again, so a late delivery naming an earlier generation can never be
    confused with the slot's current occupant; `op_kind` is the caller-
    supplied tag from `IoOpKind`, carried for the caller's convenience
    (`register_op`'s return value and `drain_ready`'s delivered entries
    both stamp it).

    A token whose `generation` no longer matches `table[slot].generation`
    — because the slot was unregistered and reused for a DIFFERENT
    registration in the meantime — is STALE: every table operation
    (`IoOpTable.is_live`/`release`/`Reactor.unregister`/delivery decode)
    treats a stale token as an inert no-op, never as an error, matching
    the native poller's own "already-closed handle degrades to a no-op"
    contract (vendor/mojito_sys_io/platform/kqueue.mojo's unregister)."""

    var slot: Int
    var generation: Int
    var op_kind: Int

    def __init__(out self, slot: Int, generation: Int, op_kind: Int):
        self.slot = slot
        self.generation = generation
        self.op_kind = op_kind

    def __copyinit__(out self, existing: Self):
        self.slot = existing.slot
        self.generation = existing.generation
        self.op_kind = existing.op_kind

    def is_valid(self) -> Bool:
        """False for the sentinel `invalid_token()` (slot < 0); does NOT
        check table liveness — use `IoOpTable.is_live(token)` for that."""
        return self.slot >= 0

    def encode(self) -> UInt64:
        """Pack (generation, slot) into the ONE opaque UInt64 the native
        poller preserves verbatim (spec §31): generation in the high 32
        bits, slot in the low 32 bits.  Both are truncated to UInt32 —
        `IoOpTable.CAPACITY` bounds `slot` far under 2**32, and a
        generation wrapping past 2**32 reuses of the SAME slot is outside
        any plausible process lifetime."""
        return (UInt64(UInt32(self.generation)) << 32) | UInt64(
            UInt32(self.slot)
        )


def invalid_token() -> IoToken:
    """The sentinel invalid token (slot -1); `is_valid()` is False."""
    return IoToken(-1, 0, IoOpKind.NONE)


def decode_token(bits: UInt64) -> IoToken:
    """Unpack a raw wire token (an `IoEvent.token` delivered by the native
    poller) back into (slot, generation).  `op_kind` is NOT recoverable
    from the wire bits alone (it never crosses the C ABI) — decode_token()
    always returns `IoOpKind.NONE`; callers resolve the real op_kind from
    the op-table entry at `slot` (see `io_op_table.IoOpTable`, consumed by
    `poller.drain_ready`)."""
    var slot = Int(UInt32(bits & UInt64(0xFFFFFFFF)))
    var generation = Int(UInt32(bits >> 32))
    return IoToken(slot, generation, IoOpKind.NONE)
