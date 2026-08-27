# mojito_async/runtime/checkpoint.mojo
#
# A1.1 runtime (issue #33) — module-level cooperative `checkpoint` surface.
#
# spec §27/§28 expose `checkpoint()` as a root concurrency primitive.  b2 has
# no TLS, so A1.1 threads the token explicitly: `checkpoint(token)` raises
# CancellationError-as-Error iff the token's flag (or an ancestor) requested
# cancellation.  Kept as its own module so the free function never collides
# with `CancellationToken.checkpoint()` (a b2 name-resolution hazard inside
# cancellation.mojo).
from mojito_async.cancellation import CancellationToken


def checkpoint(token: CancellationToken) raises:
    """Cooperative cancellation point: raise CancellationError-as-Error when
    `token` is requested (here or through an ancestor).  Lighter than a full
    check for reads; a later lane may add a scheduler fairness read."""
    token.flag()[].checkpoint()