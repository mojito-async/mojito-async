/*
 * mojito_async/vendor/mojito-sys/atomic_helpers.c
 *
 * Issue #143 — cross-thread TCB visibility.  Thin C wrappers around the
 * compiler atomic builtins (__atomic_load_n / __atomic_store_n).
 *
 * Motivation: the Mojo b2 optimizer can LICM-hoist reads even when the Mojo
 * source uses Atomic[DType.int64].load[ordering=ACQUIRE] or inlined_assembly
 * ldar, because the MLIR-level LICM pass runs before the operations are fully
 * lowered to LLVM atomic IR.  An @extern abi("C") function call is opaque
 * to the Mojo optimizer — LICM cannot hoist an external function call across
 * a loop boundary.  The C compiler inlines these wrappers to the platform's
 * native atomic instruction (LDAR / STLR on arm64) with the requested memory
 * order, so the semantics are identical to a true load atomic / store atomic.
 *
 * Memory-order constants (GCC __atomic model, matching C11 memory_order):
 *   __ATOMIC_RELAXED = 0
 *   __ATOMIC_ACQUIRE = 2   (reads; prevents later loads/stores moving before)
 *   __ATOMIC_RELEASE = 3   (writes; prevents earlier loads/stores moving after)
 *   __ATOMIC_SEQ_CST = 5   (full sequential consistency)
 *
 * Both JIT and AOT Mojo can resolve these symbols: the dylib (libmojito_spike)
 * is always linked by both execution modes.
 */
#include <stdint.h>

int64_t mojito_ato_load_i64(const int64_t *ptr, int order)
{
    return __atomic_load_n(ptr, order);
}

void mojito_ato_store_i64(int64_t *ptr, int64_t val, int order)
{
    __atomic_store_n(ptr, val, order);
}
