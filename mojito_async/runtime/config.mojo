# mojito_async/runtime/config.mojo
#
# A2.1 (issue #67) — runtime configuration (spec §89).
#
# RuntimeConfig carries the EXPLICIT optional scheduler knobs the pool
# accepts; everything else stays implicit.  The four fields are the spec §89
# surface for the M:N worker pool:
#
#   worker_count              — how many mojito-sys.NativeThread workers the
#                               pool spawns.  Default: cpu_logical_count()
#                               (sysctl hw.logicalcpu), so an unconfigured
#                               run sizes itself to the machine and N=1 on
#                               single-core hosts preserves the A1
#                               single-worker invariant.
#   stack_reserve_bytes       — reserved native stack per worker (the
#                               pthread default reservation is a generous
#                               floor; worker stacks are OS-thread stacks,
#                               not ms_stack reservations — the fiber stacks
#                               the scheduler drives are a separate pool,
#                               issue #52).
#   stack_initial_commit_bytes— eagerly committed stack for workers whose
#                               first task touches deep call chains (0 =
#                               commit on demand).
#   enable_tracing            — scheduler/tracing instrumentation toggle
#                               (off by default; the E-lanes consume it).
#
# Extern discipline: cpu_logical_count() is an ms_* dylib symbol (AOT-only,
# modular/modular#6971), so THIS module is only linked into pool consumers
# (the pool spawns native threads and is likewise AOT-only).  JIT unit
# drivers never import the pool.
from mojito_async.vendor.mojito_sys import cpu_logical_count


comptime DEFAULT_STACK_RESERVE = Int(1048576)  # 1 MiB, pthread's default
comptime DEFAULT_STACK_COMMIT = Int(0)


struct RuntimeConfig(ImplicitlyCopyable, ImplicitlyDeletable, Movable):
    """Scheduler sizing knobs (spec §89).  Defaults use detected hardware /
    runtime guidance: worker_count = cpu_logical_count(), 1 MiB reserved
    stack, no eager commit, tracing off."""

    var worker_count: Int
    var stack_reserve_bytes: Int
    var stack_initial_commit_bytes: Int
    var enable_tracing: Bool

    def __init__(out self):
        self.worker_count = cpu_logical_count()
        self.stack_reserve_bytes = DEFAULT_STACK_RESERVE
        self.stack_initial_commit_bytes = DEFAULT_STACK_COMMIT
        self.enable_tracing = False

    def __init__(
        out self,
        worker_count: Int,
        stack_reserve_bytes: Int = DEFAULT_STACK_RESERVE,
        stack_initial_commit_bytes: Int = DEFAULT_STACK_COMMIT,
        enable_tracing: Bool = False,
    ):
        self.worker_count = worker_count
        self.stack_reserve_bytes = stack_reserve_bytes
        self.stack_initial_commit_bytes = stack_initial_commit_bytes
        self.enable_tracing = enable_tracing

    def validate(self) raises:
        """Refuse nonsensical configurations at construction-like boundaries
        (start() validates again after the driver may have mutated)."""
        if self.worker_count < 1:
            raise Error(
                "RuntimeConfig.validate: worker_count must be >= 1"
            )
        if self.stack_reserve_bytes < 1:
            raise Error(
                "RuntimeConfig.validate: stack_reserve_bytes must be >= 1"
            )
        if self.stack_initial_commit_bytes < 0:
            raise Error(
                "RuntimeConfig.validate: stack_initial_commit_bytes must be >= 0"
            )


def make_pool_config() -> RuntimeConfig:
    """Module-level factory (b2 has no static methods): the DEFAULT config —
    worker_count from cpu_logical_count(), 1 MiB reserve, no eager commit,
    tracing off."""
    return RuntimeConfig()


def make_pool_config(
    worker_count: Int,
    stack_reserve_bytes: Int = DEFAULT_STACK_RESERVE,
    stack_initial_commit_bytes: Int = DEFAULT_STACK_COMMIT,
    enable_tracing: Bool = False,
) -> RuntimeConfig:
    """Explicit-config factory (b2 has no static methods)."""
    return RuntimeConfig(
        worker_count,
        stack_reserve_bytes,
        stack_initial_commit_bytes,
        enable_tracing,
    )