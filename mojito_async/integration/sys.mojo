# mojito_async/integration/sys.mojo
#
# A1.1 runtime (issue #33) — the ONLY low-level adapter to mojito-sys.
#
# Per the spec §6 layout, integration/sys.mojo is the single place that names
# mojito-sys bindings.  For A1.1 the runtime needs no raw symbol access: every
# module is deliberately EXTERN-FREE (spike discipline, modular/modular#6971
# miscompiles imported-module extern calls; context switching stays in the
# *_aot driver modules).  This module therefore re-defines the mojito-sys
# TYPE SURFACE that drivers need — `BytePtr` — plus the concrete result-slot
# types (Nil, IntResult) that the singleton generic TaskControlBlock
# instantiations use.  No extern `@extern` symbol lives here.
#
# These are the same shapes as spike/colorless_runtime/vendor/mojito-sys/
# mojito_spike.mojo (BytePtr) and the concrete result wrappers used across
# the spike (Nil in runtime.mojo, IntResult in event.mojo), kept name-for-name
# so sibling lanes and the A0 test lineage stay coherent.

from mojito_async.runtime.task_control_block import ScopeChild


comptime BytePtr = UnsafePointer[Byte, MutAnyOrigin]
"""Wild pointer used when passing raw byte payloads / userdata channels."""


# ---------------------------------------------------------------------------
# Concrete ResultValue slots (single-instantiation targets for the tests)
# ---------------------------------------------------------------------------

struct IntResult(ScopeChild):
    """Concrete Int result slot for unit-style tasks (A3.2/#61: also a
    ScopeChild — homogeneous scopes across the suite register IntResult
    children under one shared tag)."""

    var v: Int

    def __init__(out self):
        self.v = 0

    def __init__(out self, x: Int):
        self.v = x

    comptime TAG = Int(0)