/*
 * aarch64_switch.S — ms_ctx_make / ms_ctx_switch for Apple arm64 (AAPCS64).
 * Issue mojito-async/mojito-sys#9; layout v3 per issue #101 (A2.0 M:N rework).
 *
 * ms_ctx_t layout (header v3 — pinned by _Static_asserts in ms_ctx.c):
 *   regs[0..11]    @   0..88 => x19..x30 (slot i => x(19+i); regs[10]=fp, regs[11]=lr)
 *   fps[0..7]      @  96..159 => low 64 bits of v8..v15 (d8..d15)
 *   sp             @ 160
 *   return_to      @ 168 => the context to which THIS ctx returns when it
 *                           suspends/exits (i.e. its most recent switcher).
 *
 * Rules honored here:
 *   - Callee-saved x19-x28, fp(x29), lr(x30), d8-d15, sp are saved/restored;
 *     nothing else. Only temporaries x15-x17 are used inside the routines.
 *   - x18 is NEVER written (platform register, reserved on Apple platforms).
 *
 * Exit-trampoline design (CONTRACT.md: userdata passes through unmodified;
 * the trampoline tail-switches back to its recorded return context):
 *   A context created by ms_ctx_make starts on a trampoline that recovers
 *   its OWN ms_ctx_t* WITHOUT any global, calls entry(userdata), and on
 *   return tail-jumps to ms_ctx_switch(self, return_to).
 *
 * ISSUE #101 — M:N thread-safety rework (what changed vs the frozen S0):
 *   - REMOVED: the process-global 64-row `_ms_resume_tab` and the
 *     `_ms_last_from` / `_ms_last_to` globals, and the O(64) linear upsert
 *     scan on every switch.  These made the substrate (a) single-OS-thread
 *     only (`_ms_last_to` was clobbered the moment a 2nd thread switched)
 *     and (b) capped at 32 live fibers (2 rows/fiber, never evicted;
 *     `brk #0x67` on exhaustion).
 *   - REPLACED with: the return link rides INSIDE the context as
 *     `to->return_to` written by `_ctx_switch(from,to)` (O(1), no table,
 *     no scan).  The trampoline derives its own ctx WITHOUT a global by
 *     reading a marker `ms_ctx_make` stashes at [stack_top-8] on the
 *     fiber's own stack (reachable via SP / 16-aligned), so no process-
 *     shared writable state lives on the switch path => safe for M:N
 *     worker threads.
 *   - Loud diagnostics are KEPT for genuinely broken re-entry: unaligned
 *     stack_top (brk #0x64), and a resumed-after-exit double entry (the
 *     trampoline plants lr=brk #0x66 into its own saved slot so a later
 *     resume of an exited ctx traps).  A fabric-fresh resume and a valid
 *     exit are NEVER trapped.  There is no ceil on live fibers (no table).
 *
 * PAC/BTI note: unchanged from the frozen spike — this substrate assumes
 * unsigned return addresses and no BTI landing pads (NOT built with
 * -mbranch-protection=pac-ret,bti); productionization (epic #5) must add
 * pacibsp/paciblr or compile downstream Mojo frames with PAC disabled.
 */

#if !defined(__APPLE__)
#error "aarch64_switch.S targets Apple arm64 (Mach-O) only"
#endif

.section __TEXT,__text,regular,pure_instructions

.set MS_CTX_FPS_OFF, 96
.set MS_CTX_SP_OFF, 160
.set MS_CTX_RETURN_TO_OFF, 168   /* v3: the O(1) in-ctx return link */

/* ------------------------------------------------------------------------ */
/* void ms_ctx_switch(ms_ctx_t *from [x0], ms_ctx_t *to [x1])                */
/* Saves current callee-saved state into *from; writes in to->return_to =   */
/* `from` (the most recent switcher of `to`); resumes *to.                   */
/* No shared writable global on this path (issue #101 -> M:N safe).          */
/* ------------------------------------------------------------------------ */
.align 4
.globl _ms_ctx_switch
_ms_ctx_switch:
        /* save callee-saved GPRs + fp/lr into from->regs[0..11] */
        stp     x19, x20, [x0, #0]
        stp     x21, x22, [x0, #16]
        stp     x23, x24, [x0, #32]
        stp     x25, x26, [x0, #48]
        stp     x27, x28, [x0, #64]
        stp     x29, x30, [x0, #80]             /* fp, lr */
        /* save callee-saved FP/SIMD low halves d8-d15 into from->fps[0..7] */
        stp     d8, d9, [x0, #MS_CTX_FPS_OFF]
        stp     d10, d11, [x0, #(MS_CTX_FPS_OFF+16)]
        stp     d12, d13, [x0, #(MS_CTX_FPS_OFF+32)]
        stp     d14, d15, [x0, #(MS_CTX_FPS_OFF+48)]
        mov     x15, sp
        str     x15, [x0, #MS_CTX_SP_OFF]       /* sp saved last */

        /* to->return_to = from  (O(1); the most recent switcher of `to`) */
        str     x0, [x1, #MS_CTX_RETURN_TO_OFF]

        /* restore `to` */
        ldp     d14, d15, [x1, #(MS_CTX_FPS_OFF+48)]
        ldp     d12, d13, [x1, #(MS_CTX_FPS_OFF+32)]
        ldp     d10, d11, [x1, #(MS_CTX_FPS_OFF+16)]
        ldp     d8, d9, [x1, #MS_CTX_FPS_OFF]
        ldp     x27, x28, [x1, #64]
        ldp     x25, x26, [x1, #48]
        ldp     x23, x24, [x1, #32]
        ldp     x21, x22, [x1, #16]
        ldp     x19, x20, [x1, #0]
        ldr     x16, [x1, #MS_CTX_SP_OFF]
        mov     sp, x16
        ldp     x29, x30, [x1, #80]             /* fp, lr restored last */
        ret                                     /* br x30 semantics */

/* ------------------------------------------------------------------------ */
/* void ms_ctx_make(ms_ctx_t *ctx [x0], void *stack_top [x1],                */
/*                  ms_entry_fn entry [x2], void *userdata [x3])             */
/* Synthesizes a resumed state: first switch lands on the exit trampoline    */
/* with sp == stack_top (must be 16-byte aligned; misalignment traps).       */
/* Also stashes `ctx` at [stack_top-8] (the fiber's own stack) so the        */
/* trampoline can recover its own identity without any global (issue #101;   */
/* keeps the switch path free of shared writable state -> thread-safe).      */
/* ------------------------------------------------------------------------ */
.align 4
.globl _ms_ctx_make
_ms_ctx_make:
        tst     x1, #15
        b.ne    3f                              /* stack_top misaligned */
        stp     x2, x3, [x0, #0]                /* x19 = entry, x20 = userdata */
        mov     x15, xzr
        stp     x15, x15, [x0, #16]             /* x21, x22 */
        stp     x15, x15, [x0, #32]             /* x23, x24 */
        stp     x15, x15, [x0, #48]             /* x25, x26 */
        stp     x15, x15, [x0, #64]             /* x27, x28 */
        str     x15, [x0, #80]                  /* fp = 0 */
        stp     x15, x15, [x0, #(MS_CTX_FPS_OFF)]      /* d8, d9 = 0 */
        stp     x15, x15, [x0, #(MS_CTX_FPS_OFF+16)]   /* d10, d11 = 0 */
        stp     x15, x15, [x0, #(MS_CTX_FPS_OFF+32)]   /* d12, d13 = 0 */
        stp     x15, x15, [x0, #(MS_CTX_FPS_OFF+48)]   /* d14, d15 = 0 */
        str     x1, [x0, #MS_CTX_SP_OFF]        /* sp = stack_top */
        /* return_to is zero (fresh ctx, not yet switched); the trampoline's
           first entry does not need it.  Zero it for safety. */
        str     xzr, [x0, #MS_CTX_RETURN_TO_OFF]
        adrp    x16, _ms_ctx_trampoline@PAGE
        add     x16, x16, _ms_ctx_trampoline@PAGEOFF
        str     x16, [x0, #88]                  /* lr = trampoline */
        str     x0, [x1, #-8]                   /* self marker @ [stack_top-8] */
        ret
3:      brk     #0x64                           /* stack_top not 16-aligned */

/* ------------------------------------------------------------------------ */
/* Exit trampoline (private): entered via ms_ctx_switch `ret` with           */
/*   x19 = entry, x20 = userdata, sp = stack_top (16-aligned).               */
/* Recovers its own ctx from the [stack_top-8] marker, calls entry(userdata);*/
/* on return switches back to the most recent switcher (ctx->return_to).     */
/* Resuming an already-exited context traps (brx #0x66).                     */
/* ------------------------------------------------------------------------ */
.align 4
.private_extern _ms_ctx_trampoline
_ms_ctx_trampoline:
        sub     sp, sp, #16                     /* [sp+8] = marker slot (stack_top-8) */
        ldr     x15, [sp, #8]                   /* self = [stack_top-8] (issue #101) */
        str     x15, [sp]                       /* keep self below entry frames */

        mov     x0, x20
        blr     x19                             /* entry(userdata) */

        /* self from our frame; return_to = self->return_to (O(1) in-ctx link) */
        ldr     x0, [sp]
        ldr     x1, [x0, #MS_CTX_RETURN_TO_OFF]
        add     sp, sp, #16

        adr     x17, 6f
        mov     x30, x17                        /* re-resume after exit traps */
        b       _ms_ctx_switch                  /* tail call: switch(self, return_to) */
6:      brk     #0x66