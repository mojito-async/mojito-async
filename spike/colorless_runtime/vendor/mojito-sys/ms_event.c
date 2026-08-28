/*
 * ms_event.c — S3.5 NativeEvent: an efficient OS-worker sleep/wake primitive
 * (mojito-async A2.6 idle-worker park, issue #72 / spec #5166, Appendix A
 * "NativeEvent — efficient OS-worker sleep/wake").
 *
 * Semantics (issue #72, S3.5 — the "wake one parked worker" primitive of
 * spec §17):
 *   - AUTO-RESET, BREADTH-ONE: a signal releases AT MOST ONE parked waiter.
 *     The waiter that consumes the token returns from wait as the only one
 *     woken by that signal (no broadcast / no thundering herd).
 *   - AT MOST ONE TOKEN PENDING: the event holds a single boolean token.
 *     signal() while a token is already pending coalesces — the token stays
 *     one (a burst of N signals delivers at most one token).
 *   - STICKY WHEN NOBODY WAITS: signal() with zero waiters leaves the token
 *     pending; the NEXT waiter that calls wait consumes it and returns
 *     immediately without blocking (closes the lost-wakeup race).
 *   - COALESCING: at most one token, so redundant signals are dropped.
 *   - CLOCK_MONOTONIC wait_until: wait_until(e, deadline_ns) blocks until an
 *     ABSOLUTE CLOCK_MONOTONIC deadline (ns) and returns
 *       1 iff a token was CONSUMED (predicate loop fully inside C — the
 *         spec §17 win: a spurious wakeup never surfaces as a fake "ok"),
 *       0 iff the deadline elapsed with no token.
 *
 * The predicate loop lives here, NOT in Mojo: wait_until re-checks the token
 * under the mutex after every condvar wake (spurious or real) and only
 * returns 1 on an actual token consumption.
 *
 * Implementation: one pthread mutex + condvar + an int token + a waiter
 * count.  Darwin does NOT expose pthread_condattr_setclock, so the condvar
 * is default-CLOCK_REALTIME; wait_until converts the caller's absolute
 * CLOCK_MONOTONIC deadline to CLOCK_REALTIME (remaining-relative → realtime
 * now + relative) just before timedwait, so the public contract stays
 * "block until the monotonic deadline".
 *
 * Handle model: ms_event_create() returns an opaque handle (address of a
 * heap ms_event_t, 0 on failure); ms_event_destroy() frees it.  Coordinates
 * are process-wide pthread primitives (libc) — the C-ABI firewall the rest
 * of mojito-sys hits for thread/TLS.
 */
#define _DARWIN_C_SOURCE 1
#include <errno.h>
#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <time.h>

#ifndef CLOCK_MONOTONIC
#error "CLOCK_MONOTONIC required (POSIX)"
#endif

/* One NativeEvent: a single sticky token + the waiter count. */
typedef struct ms_event {
    pthread_mutex_t mutex;
    pthread_cond_t  cond;
    int             pending; /* 1 => a token is pending (sticky/available) */
    int             waiters; /* threads currently blocked in wait_until     */
} ms_event_t;

static uint64_t ms_clock_ns(clockid_t clk) {
    struct timespec ts;
    clock_gettime(clk, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static uint64_t ms_mono_now_ns(void) {
    return ms_clock_ns(CLOCK_MONOTONIC);
}

/* Convert an absolute CLOCK_MONOTONIC deadline into an absolute
 * CLOCK_REALTIME deadline (same remaining relative duration).  This lets us
 * use Darwin's default-realtime condvar while honoring a monotonic wait_until
 * contract. */
static struct timespec ms_mono_deadline_to_realtime(uint64_t mono_deadline_ns) {
    uint64_t now_mono = ms_mono_now_ns();
    uint64_t rel_ns = (mono_deadline_ns > now_mono) ? (mono_deadline_ns - now_mono) : 0;
    uint64_t rt = ms_clock_ns(CLOCK_REALTIME) + rel_ns;
    struct timespec abs;
    abs.tv_sec = (time_t)(rt / 1000000000ULL);
    abs.tv_nsec = (long)(rt % 1000000000ULL);
    return abs;
}

/* Create one NativeEvent; returns its handle (0 == allocation failure). */
uintptr_t ms_event_create(void) {
    ms_event_t *e = (ms_event_t *)calloc(1, sizeof(ms_event_t));
    if (!e) return 0;
    if (pthread_mutex_init(&e->mutex, NULL) != 0) {
        free(e);
        return 0;
    }
    if (pthread_cond_init(&e->cond, NULL) != 0) {
        pthread_mutex_destroy(&e->mutex);
        free(e);
        return 0;
    }
    e->pending = 0;
    e->waiters = 0;
    return (uintptr_t)e;
}

void ms_event_destroy(uintptr_t h) {
    if (h == 0) return;
    ms_event_t *e = (ms_event_t *)h;
    /* No waiters may still be blocked: destroy only after the pool joined
     * every worker (the lifecycle guarantees this). */
    pthread_mutex_destroy(&e->mutex);
    pthread_cond_destroy(&e->cond);
    free(e);
}

/* Signal: set the sticky token; if a waiter exists, wake at most ONE
 * (breadth-one).  Coalesces (a pending token stays pending — N signals
 * deliver at most one). */
void ms_event_signal(uintptr_t h) {
    if (h == 0) return;
    ms_event_t *e = (ms_event_t *)h;
    pthread_mutex_lock(&e->mutex);
    if (!e->pending) {
        e->pending = 1;
        if (e->waiters > 0) {
            pthread_cond_signal(&e->cond); /* wake exactly one */
        }
    }
    pthread_mutex_unlock(&e->mutex);
}

/* Block WITHOUT a deadline until a token is consumed; returns 1. */
int ms_event_wait(uintptr_t h) {
    if (h == 0) return 0;
    ms_event_t *e = (ms_event_t *)h;
    pthread_mutex_lock(&e->mutex);
    if (e->pending) {
        e->pending = 0;
        pthread_mutex_unlock(&e->mutex);
        return 1;
    }
    e->waiters++;
    while (1) {
        if (e->pending) { /* re-check under the lock: spurious/coalesced */
            e->pending = 0;
            e->waiters--;
            pthread_mutex_unlock(&e->mutex);
            return 1;
        }
        pthread_cond_wait(&e->cond, &e->mutex);
    }
}

/* Block until an ABSOLUTE CLOCK_MONOTONIC deadline_ns; returns 1 iff a token
 * was consumed, 0 iff the deadline elapsed with no token.  The predicate
 * loop is entirely inside C (spec §17): a real token is the ONLY path to a
 * return of 1. */
int ms_event_wait_until(uintptr_t h, uint64_t deadline_ns) {
    if (h == 0) return 0;
    ms_event_t *e = (ms_event_t *)h;
    pthread_mutex_lock(&e->mutex);
    if (e->pending) {
        e->pending = 0;
        pthread_mutex_unlock(&e->mutex);
        return 1;
    }
    e->waiters++;
    while (1) {
        if (e->pending) {
            e->pending = 0;
            e->waiters--;
            pthread_mutex_unlock(&e->mutex);
            return 1;
        }
        if (ms_mono_now_ns() >= deadline_ns) {
            e->waiters--;
            pthread_mutex_unlock(&e->mutex);
            return 0;
        }
        struct timespec abs = ms_mono_deadline_to_realtime(deadline_ns);
        int rc = pthread_cond_timedwait(&e->cond, &e->mutex, &abs);
        if (rc == ETIMEDOUT) {
            e->waiters--;
            pthread_mutex_unlock(&e->mutex);
            return 0;
        }
        /* woken (signal, spurious, or lost-wakeup guard): loop re-checks. */
    }
}

/* Current CLOCK_MONOTONIC time in nanoseconds (for deadline computation). */
uint64_t ms_monotonic_ns(void) {
    return ms_mono_now_ns();
}
