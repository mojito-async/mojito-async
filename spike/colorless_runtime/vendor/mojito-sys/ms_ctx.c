/*
 * ms_ctx.c — compile-time guards for the frozen v2 ms_ctx_t layout consumed by
 * aarch64_switch.S, plus a committed sentinel-register probe.
 * Issue mojito-async/mojito-sys#9; layout v2 per issue #19.
 *
 * The context switch itself is pure assembly (aarch64_switch.S); this unit
 * pins the C-side layout the asm hardcodes as immediates so any header drift
 * fails at compile time instead of corrupting saved registers at runtime.
 */

#include "include/mojito_spike.h"

#include <stddef.h>

/* 12 GPRs + 8 FP lows + sp, all uint64_t = 21 x 8 = 168 bytes (v2). */
_Static_assert(sizeof(ms_ctx_t) == 168, "ms_ctx_t must be 12 regs + 8 fps + sp = 168 bytes");
_Static_assert(_Alignof(ms_ctx_t) == 8, "ms_ctx_t must be 8-byte aligned");
_Static_assert(offsetof(ms_ctx_t, regs) == 0, "regs[] must be first: asm uses base+0..88");
_Static_assert(offsetof(ms_ctx_t, fps) == 96, "fps[] must be at +96: asm stp/ldp d8-d15");
_Static_assert(offsetof(ms_ctx_t, sp) == 160, "sp slot must be at +160: asm immediate");

/* reg slot i => x(19+i): fp is regs[10] @80, lr is regs[11] @88.
 * fps[i] => low half of v(8+i) (d8..d15). */

/* ----------------------------------------------------------------------- */
/* Committed sentinel probe (panel evidence for #9/#19).
 *
 * Build & run:
 *   clang -arch arm64 -DMS_CTX_SENTINEL_PROBE -DMS_CTX_SENTINEL_PROBE_MAIN \
 *     -O2 spike/context_switch/ms_ctx.c spike/context_switch/aarch64_switch.S \
 *     -I spike/context_switch -o /tmp/probe && /tmp/probe
 *
 * Two synthetic contexts each tag x19-x28 and d8-d15 with distinct magics,
 * ping-pong through each other (so BOTH contexts' full callee-saved state
 * crosses a real ms_ctx_switch in both directions), re-read and compare,
 * then hand control back to the probe driver through the exit trampoline.
 * Default build (no define) compiles the layout guards only; probe symbols
 * are .private_extern so the contract surface stays exactly
 * {ms_ctx_make, ms_ctx_switch}.
 * ----------------------------------------------------------------------- */
#ifdef MS_CTX_SENTINEL_PROBE

#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <unistd.h>

#define P_USABLE (64 * 1024)

static ms_ctx_t p_main, p_a, p_b;

typedef struct {
    long counter;          /* +0  */
    long left;             /* +8  */
    long regs_ok;          /* +16 */
    long sp_ok;            /* +24 */
    unsigned long tag;     /* +32 */
    ms_ctx_t *self;        /* +40 */
    ms_ctx_t *peer;        /* +48 */
    ms_ctx_t *sched;       /* +56 */
} p_arg_t;

static p_arg_t pa = { 0, 2, 1, 1, 0x11111000UL, &p_a, &p_b, &p_main };
static p_arg_t pb = { 0, 2, 1, 1, 0x22222000UL, &p_b, &p_a, &p_main };

__asm__(
".text\n"
".align 4\n"

/* x0 = tag: x19..x28 <- tag+{0x000..0x900}; d8..d15 mirror x19..x26 */
".private_extern _probe_fill\n"
"_probe_fill:\n"
"       mov     x19, x0\n"
"       add     x20, x0, #0x100\n"
"       add     x21, x0, #0x200\n"
"       add     x22, x0, #0x300\n"
"       add     x23, x0, #0x400\n"
"       add     x24, x0, #0x500\n"
"       add     x25, x0, #0x600\n"
"       add     x26, x0, #0x700\n"
"       add     x27, x0, #0x800\n"
"       add     x28, x0, #0x900\n"
"       fmov    d8,  x19\n"
"       fmov    d9,  x20\n"
"       fmov    d10, x21\n"
"       fmov    d11, x22\n"
"       fmov    d12, x23\n"
"       fmov    d13, x24\n"
"       fmov    d14, x25\n"
"       fmov    d15, x26\n"
"       ret\n"

/* x0 = tag, x1 = long *ok: clear *ok if any tagged register drifted */
".private_extern _probe_check\n"
"_probe_check:\n"
"       sub     sp, sp, #64\n"
"       stp     d8,  d9,  [sp]\n"
"       stp     d10, d11, [sp, #16]\n"
"       stp     d12, d13, [sp, #32]\n"
"       stp     d14, d15, [sp, #48]\n"
"       mov     x9, x0\n"
"       cmp     x19, x9\n"
"       b.ne    9f\n"
"       add     x11, x0, #0x100\n"
"       cmp     x20, x11\n"
"       b.ne    9f\n"
"       add     x11, x0, #0x200\n"
"       cmp     x21, x11\n"
"       b.ne    9f\n"
"       add     x11, x0, #0x300\n"
"       cmp     x22, x11\n"
"       b.ne    9f\n"
"       add     x11, x0, #0x400\n"
"       cmp     x23, x11\n"
"       b.ne    9f\n"
"       add     x11, x0, #0x500\n"
"       cmp     x24, x11\n"
"       b.ne    9f\n"
"       add     x11, x0, #0x600\n"
"       cmp     x25, x11\n"
"       b.ne    9f\n"
"       add     x11, x0, #0x700\n"
"       cmp     x26, x11\n"
"       b.ne    9f\n"
"       add     x11, x0, #0x800\n"
"       cmp     x27, x11\n"
"       b.ne    9f\n"
"       add     x11, x0, #0x900\n"
"       cmp     x28, x11\n"
"       b.ne    9f\n"
"       ldr     x12, [sp]\n"
"       cmp     x12, x9\n"
"       b.ne    9f\n"
"       ldr     x12, [sp, #8]\n"
"       add     x11, x0, #0x100\n"
"       cmp     x12, x11\n"
"       b.ne    9f\n"
"       ldr     x12, [sp, #16]\n"
"       add     x11, x0, #0x200\n"
"       cmp     x12, x11\n"
"       b.ne    9f\n"
"       ldr     x12, [sp, #24]\n"
"       add     x11, x0, #0x300\n"
"       cmp     x12, x11\n"
"       b.ne    9f\n"
"       ldr     x12, [sp, #32]\n"
"       add     x11, x0, #0x400\n"
"       cmp     x12, x11\n"
"       b.ne    9f\n"
"       ldr     x12, [sp, #40]\n"
"       add     x11, x0, #0x500\n"
"       cmp     x12, x11\n"
"       b.ne    9f\n"
"       ldr     x12, [sp, #48]\n"
"       add     x11, x0, #0x600\n"
"       cmp     x12, x11\n"
"       b.ne    9f\n"
"       ldr     x12, [sp, #56]\n"
"       add     x11, x0, #0x700\n"
"       cmp     x12, x11\n"
"       b.ne    9f\n"
"       add     sp, sp, #64\n"
"       ret\n"
"9:\n"
"       str     xzr, [x1]\n"
"       add     sp, sp, #64\n"
"       ret\n"

/* generic worker body shared by both probe entries; arg offsets in comments */
".private_extern _pworker\n"
"_pworker:\n"                                  /* x0 = p_arg_t *arg        */
"       stp     x29, x30, [sp, #-32]!\n"
"       mov     x29, sp\n"
"       str     x0, [sp, #16]\n"               /* arg on our own frame     */
"       ldr     x0, [x0, #32]\n"               /* tag                      */
"       bl      _probe_fill\n"                 /* tags x19-x28, d8-d15     */ 
"       mov     x12, sp\n"
"       tst     x12, #15\n"
"       b.eq    1f\n"                          /* entry-time sp alignment  */
"       ldr     x8, [sp, #16]\n"
"       ldr     x9, [x8, #24]\n"
"       str     xzr, [x9]\n"
"1:\n"
"2:\n"
"       ldr     x8, [sp, #16]\n"
"       ldr     x9, [x8, #0]\n"                /* counter++                */
"       add     x9, x9, #1\n"
"       str     x9, [x8, #0]\n"
"       ldr     x10, [x8, #8]\n"               /* --left                   */
"       sub     x10, x10, #1\n"
"       str     x10, [x8, #8]\n"
"       cbz     x10, 3f\n"
"       ldr     x0, [x8, #40]\n"               /* self                     */
"       ldr     x1, [x8, #48]\n"               /* peer                     */
"       bl      _ms_ctx_switch\n"
"       b       2b\n"                          /* resumed here             */
"3:\n"
"       ldr     x8, [sp, #16]\n"
"       ldr     x0, [x8, #32]\n"               /* tag                      */
"       ldr     x1, [x8, #16]\n"               /* &regs_ok                 */
"       bl      _probe_check\n"                /* x19-x28+d8-d15 intact?   */
"       mov     x12, sp\n"                     /* post-resume sp alignment */
"       tst     x12, #15\n"
"       b.eq    4f\n"
"       ldr     x8, [sp, #16]\n"
"       ldr     x9, [x8, #24]\n"
"       str     xzr, [x9]\n"
"4:\n"
"       ldr     x8, [sp, #16]\n"
"       ldr     x0, [x8, #40]\n"               /* self                     */
"       ldr     x1, [x8, #56]\n"               /* scheduler (probe driver) */
"       bl      _ms_ctx_switch\n"
"       brk     #0x70\n"                       /* resumed after exit = bug */

"\n.private_extern _pa_entry\n"
"_pa_entry:\n"                                 /* x0 = &pa                 */
"       b       _pworker\n"
"\n.private_extern _pb_entry\n"
"_pb_entry:\n"                                 /* x0 = &pb                 */
"       b       _pworker\n"
);

void pa_entry(void *);
void pb_entry(void *);

static void *p_stack(void)
{
    long ps = sysconf(_SC_PAGESIZE);
    size_t total = (size_t)ps + P_USABLE;
    char *p = mmap(NULL, total, PROT_READ | PROT_WRITE,
                   MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (p == MAP_FAILED) { perror("mmap"); exit(2); }
    mprotect(p, (size_t)ps, PROT_NONE);        /* guard below usable region */
    return p + ps + P_USABLE;
}

int ms_ctx_sentinel_probe(void)
{
    int failures = 0;
    ms_ctx_make(&p_a, p_stack(), (ms_entry_fn)pa_entry, &pa);
    ms_ctx_make(&p_b, p_stack(), (ms_entry_fn)pb_entry, &pb);

    printf("sentinel probe: A<->B cross-switch; both verify x19-x28 + d8-d15\n");
    ms_ctx_switch(&p_main, &p_a);   /* A fills -> B fills -> back to A -> A exits */
    if (!(pa.regs_ok == 1 && pa.sp_ok == 1)) failures++;
    printf("  A: x19-x28 + d8-d15 preserved, sp aligned      %s\n",
           (pa.regs_ok && pa.sp_ok) ? "OK" : "FAIL");

    ms_ctx_switch(&p_main, &p_b);   /* B resumes post-switch, verifies, exits */
    if (!(pb.regs_ok == 1 && pb.sp_ok == 1)) failures++;
    printf("  B: x19-x28 + d8-d15 preserved, sp aligned      %s\n",
           (pb.regs_ok && pb.sp_ok) ? "OK" : "FAIL");
    printf("RESULT: %s\n", failures ? "FAILED" : "ALL SENTINELS PRESERVED");
    return failures ? 1 : 0;
}

#ifdef MS_CTX_SENTINEL_PROBE_MAIN
int main(void) { return ms_ctx_sentinel_probe(); }
#endif

#endif /* MS_CTX_SENTINEL_PROBE */
