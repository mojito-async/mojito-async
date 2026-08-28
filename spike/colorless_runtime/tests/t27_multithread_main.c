/*
 * spike/colorless_runtime/tests/t27_multithread_main.c
 *
 * issue #101 — substrate M:N thread-safety probe.
 *
 * Drives real ms_ctx switches from TWO pthread'd worker threads CONCURRENTLY,
 * each switching its OWN set of fibers, asserting every fiber resumes exactly
 * its own frame (per-thread, per-fiber exact-resume markers + per-thread tags).
 * This is the regression probe for the cross-OS-thread clobber that makes the
 * frozen single-threaded spike unsafe under M:N (the S2.2 2-worker recipe).
 *
 * RED (frozen substrate): ms_ctx_switch() writes process-global
 * _ms_last_from/_ms_last_to on every switch, and the exit trampoline reads
 * _ms_last_to to learn its own ctx (aarch64_switch.S).  That read is on a
 * fiber's FIRST entry only; only a second thread's switch that lands between
 * a thread's own switch-write and that trampoline read clobbers it.  To make
 * the window observable in practice we create MANY fibers per thread: every
 * fiber's first entry fires the window once, so with FIBERS per thread
 * running concurrently the clobber is reached on a typical schedule (a
 * worker reads the OTHER thread's _ms_last_to, stashes the wrong self, and
 * later resumes the WRONG context -> cross-wired markers/tags or a resume-
 * table miss, brk #0x65, process crash).  The driver reports RED/FAIL.
 * LIMITATION (documented): the precise interleaving is scheduler-dependent,
 * so on a single, benign schedule the probe may not observe a clobber; it is
 * re-run and the many-first-entry storm makes it reliably red on frozen.
 * The rework removes the shared writable globals entirely, so the probe is
 * deterministically GREEN there (every run).
 *
 * GREEN (a2/00-substrate): the resume table and _ms_last_* globals are
 * REMOVED; the return link rides in the ctx and the trampoline derives its
 * own ctx from a STACK marker (ms_ctx_make stashes it); no shared writable
 * global on the switch path.  Two OS threads are fully independent: every
 * run passes with exact per-fiber markers/tags.
 *
 * Each worker builds FIBERS fibers over its own buffers and runs each fiber
 * once: enter (first entry -> trampoline), park, resume to completion.  The
 * per-fiber marker + the per-thread tag must survive each park/resume.
 * A go-barrier releases both workers together for overlap.
 *
 * Verdict: prints "PASS" only when every worker reports every fiber exact
 * (exit 0); else "FAIL" (exit 1); a trap/crash is a FAIL (no verdict).
 */

#include "mojito_spike.h"

#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define N_WORKERS  2
#define FIBERS     128                       /* first-entry windows / thread */

#define STACK_BYTES (64 * 1024)              /* per fiber */

static uint8_t stacks[N_WORKERS][FIBERS][STACK_BYTES]
    __attribute__((aligned(16)));

static ms_ctx_t driver_ctx[N_WORKERS][FIBERS];
static ms_ctx_t fiber_ctx[N_WORKERS][FIBERS];

typedef struct {
    int         id;
    uintptr_t   markers[FIBERS];   /* frame-local addr per fiber  */
    int         ok[FIBERS];        /* exact-resume evidence        */
    unsigned    tag;               /* per-thread magic             */
    int         done;              /* completed fibers              */
} runner_t;

static runner_t R[N_WORKERS];

static volatile int go = 0;

/* The fiber entry: record a frame-local on FIRST entry, park, verify the
 * exact resume point (marker + per-thread tag), then return (completion). */
static void fiber_entry(void *ud)
{
    unsigned *tri = (unsigned *)ud;   /* (wid, fk, tag) side channel */
    int wid = (int)tri[0];
    int fk  = (int)tri[1];
    runner_t *r = &R[wid];
    int local_slot = 0;
    uintptr_t lp = (uintptr_t)&local_slot;

    if (r->markers[fk] == 0)
        r->markers[fk] = lp;

    /* park once: switch back to this worker's driver context */
    ms_ctx_switch(&fiber_ctx[wid][fk], &driver_ctx[wid][fk]);

    /* -- exact resume point -- same frame-local, same per-thread tag */
    if (r->markers[fk] != lp || r->tag != (0xABCDu + (unsigned)wid))
        r->ok[fk] = 0;
    r->done++;
}

static void *worker(void *arg)
{
    runner_t *r = (runner_t *)arg;
    int wid = r->id;

    r->tag = 0xABCDu + (unsigned)wid;

    unsigned *tri = (unsigned *)calloc(FIBERS, 3 * sizeof(unsigned));
    if (!tri)
        return NULL;

    for (int fk = 0; fk < FIBERS; fk++) {
        void *top = stacks[wid][fk] + sizeof(stacks[wid][fk]);
        tri[fk * 3 + 0] = (unsigned)wid;
        tri[fk * 3 + 1] = (unsigned)fk;
        tri[fk * 3 + 2] = r->tag;
        r->ok[fk] = 1;   /* initialize before the fiber ever runs */
        ms_ctx_make(&fiber_ctx[wid][fk], top, fiber_entry, &tri[fk * 3]);
    }

    while (!go)
        ;

    for (int fk = 0; fk < FIBERS; fk++) {
        ms_ctx_switch(&driver_ctx[wid][fk], &fiber_ctx[wid][fk]);  /* enter  */
        ms_ctx_switch(&driver_ctx[wid][fk], &fiber_ctx[wid][fk]);  /* resume */
    }

    free(tri);
    return NULL;
}

int main(void)
{
    pthread_t t[N_WORKERS];
    int failures = 0;

    memset(R, 0, sizeof(R));
    for (int i = 0; i < N_WORKERS; i++)
        R[i].id = i;

    for (int i = 0; i < N_WORKERS; i++) {
        if (pthread_create(&t[i], NULL, worker, &R[i]) != 0) {
            fprintf(stderr, "T27 multithread: pthread_create %d failed\n", i);
            return 1;
        }
    }
    go = 1;

    for (int i = 0; i < N_WORKERS; i++)
        pthread_join(t[i], NULL);

    for (int i = 0; i < N_WORKERS; i++) {
        if (R[i].done != FIBERS) {
            fprintf(stderr, "worker %d: completed %d/%d fibers\n",
                    i, R[i].done, FIBERS);
            failures++;
        }
        for (int fk = 0; fk < FIBERS; fk++) {
            if (R[i].markers[fk] == 0) {
                fprintf(stderr, "worker %d fiber %d: no marker\n", i, fk);
                failures++;
                break;
            }
            if (!R[i].ok[fk]) {
                fprintf(stderr, "worker %d fiber %d: exact resume lost\n",
                        i, fk);
                failures++;
                break;
            }
        }
    }

    if (failures == 0) {
        printf("T27 multithread: workers=%d fibers/worker=%d exact PASS\n",
               N_WORKERS, FIBERS);
        return 0;
    }
    printf("T27 multithread: FAIL (%d)\n", failures);
    return 1;
}