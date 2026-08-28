#ifndef MOJITO_SPIKE_H
#define MOJITO_SPIKE_H
#include <stddef.h>
#include <stdint.h>

typedef void (*ms_entry_fn)(void *userdata);

/* Fixed-layout v3 save area consumed by aarch64_switch.S: 22 x 8 = 176 bytes
 * (regs[12] = x19..x30 @0..95; fps[8] = d8..d15 low halves @96..159;
 * sp @160; return_to @168). v2 per issue #19 added the FP lows; v3 per
 * issue #101 (A2.0 M:N rework) adds `return_to` — the O(1) in-ctx return
 * link that replaces the removed process-global resume table, so a switch
 * touches NO shared writable state and the substrate is thread-safe. */
typedef struct ms_ctx {
    uint64_t regs[12]; /* x19..x30 (x30=lr); slot i => reg x(19+i) */
    uint64_t fps[8];   /* low 64 bits of v8..v15, callee-saved     */
    uint64_t sp;
    uint64_t return_to; /* ctx to switch back to on suspend/exit (owned by
                           aarch64_switch.S via _ms_ctx_switch)          */
} ms_ctx_t;

int      ms_page_size(void);
/* Reserve `bytes` (rounded up to page multiple) + one PROT_NONE guard page.
 * Out: *out_base (allocation base, guard at [base, base+ps)), *out_top
 * (initial SP = highest usable address, 16-byte aligned). Non-moving. */
int      ms_stack_alloc(size_t bytes, void **out_base, void **out_top);
int      ms_live_stack_count(void); /* live reservations (oversubscription
                                       guard: 64-row resume tab / 2 per fiber
                                       = 32 fibers; EPIC #2/#101 removes cap) */

/* Prepare ctx so ms_ctx_switch resumes at entry(userdata) on stack_top,
 * with AAPCS64 prologue assumptions (sp 16-aligned at entry). */
void     ms_ctx_make(ms_ctx_t *ctx, void *stack_top, ms_entry_fn entry, void *userdata);
/* Save current callee-saved state into *from; resume *to. */
void     ms_ctx_switch(ms_ctx_t *from, ms_ctx_t *to);
#endif
