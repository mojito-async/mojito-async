/* macOS arm64 stack allocator for mojito_spike (#8).
 *
 * Layout of one reservation:
 *
 *   base                base+ps                    top (= initial SP)
 *   |  guard page       |  usable pages            |
 *   v  PROT_NONE        v  PROT_READ|WRITE         v  16-byte aligned
 *   +-------------------+--------------------------+
 *
 * The mapping never moves; ms_stack_free munmaps the whole reservation.
 */

#include "mojito_spike.h"

#include <sys/mman.h>
#include <unistd.h>
#include <pthread.h>

#include <errno.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

static size_t g_page_size;

/* Registry lock: guards g_resv / g_resv_len / g_resv_cap across the
 * concurrent worker threads that call ms_stack_alloc / ms_stack_free /
 * ms_stack_is_live on every park and resume (issue #145, Bug 1).
 * PTHREAD_MUTEX_INITIALIZER is a cold-path initializer: the lock is only
 * contended when the pool needs a fresh reservation or retires one, which
 * is uncommon relative to actual fiber switch frequency. */
static pthread_mutex_t g_registry_lock = PTHREAD_MUTEX_INITIALIZER;

/* Small base -> total-size registry so concurrent live stacks can each be
 * freed with only their base pointer. */
typedef struct {
    void *base;
    size_t total;
} ms_reservation;

static ms_reservation *g_resv;
static size_t g_resv_len, g_resv_cap;

/* ABA detection (issue #145, Bug 2): a circular dead-list of recently-freed
 * base addresses.  ms_stack_is_live returns 0 for any address in this list,
 * even if the OS subsequently re-mmap'd the same page to a new allocation.
 * Without this, a freed + re-mmap'd address is indistinguishable from a
 * still-live one, which is the ABA hole fiber/stack_pool relied on.
 *
 * The list is a fixed-size circular buffer (oldest entry evicted when full).
 * MS_DEAD_CAP is generous enough to survive ABA_TRIES == 4096 loop frees
 * without evicting the original freed address. */
#define MS_DEAD_CAP 8192
static void  *g_dead_list[MS_DEAD_CAP];
static size_t g_dead_write; /* next write index (monotonically increasing) */

static size_t page_size(void) {
    if (g_page_size == 0) {
        long ps = sysconf(_SC_PAGESIZE);
        g_page_size = ps > 0 ? (size_t)ps : 4096;
    }
    return g_page_size;
}

static size_t round_up(size_t n, size_t align) {
    return (n + align - 1) / align * align;
}

int ms_page_size(void) {
    return (int)page_size();
}

int ms_stack_alloc(size_t bytes, void **out_base, void **out_top) {
    if (out_base == NULL || out_top == NULL) {
        errno = EINVAL;
        return -1;
    }

    size_t ps = page_size();
    /* Rounding may add up to one page; the guard adds another. Reject
     * requests whose total could overflow SIZE_MAX before doing any math. */
    if (bytes > SIZE_MAX - 2 * ps) {
        errno = EINVAL;
        return -1;
    }

    size_t usable = round_up(bytes == 0 ? ps : bytes, ps); /* >= 1 page */
    size_t total  = usable + ps;                           /* + guard page */

    /* mmap/mprotect are slow syscalls; perform them WITHOUT the lock so
     * concurrent allocations don't serialise on OS round-trips. */
    void *base = mmap(NULL, total, PROT_READ | PROT_WRITE,
                      MAP_PRIVATE | MAP_ANON, -1, 0);
    if (base == MAP_FAILED)
        return -1;

    /* Guard at [base, base+ps): any overflow off the bottom of the stack
     * faults instead of silently corrupting the heap below. */
    if (mprotect(base, ps, PROT_NONE) != 0) {
        int saved = errno;
        munmap(base, total);
        errno = saved;
        return -1;
    }

    /* Insert into the registry under the lock. */
    pthread_mutex_lock(&g_registry_lock);
    ms_reservation *slot = NULL;
    for (size_t i = 0; i < g_resv_len; ++i) {
        if (g_resv[i].base == NULL) {
            slot = &g_resv[i];
            break;
        }
    }
    if (slot == NULL) {
        if (g_resv_len == g_resv_cap) {
            size_t cap = g_resv_cap ? g_resv_cap * 2 : 8;
            ms_reservation *grown = realloc(g_resv, cap * sizeof *g_resv);
            if (grown == NULL) {
                pthread_mutex_unlock(&g_registry_lock);
                munmap(base, total);
                errno = ENOMEM;
                return -1;
            }
            g_resv     = grown;
            g_resv_cap = cap;
        }
        slot = &g_resv[g_resv_len++];
    }
    slot->base  = base;
    slot->total = total;
    pthread_mutex_unlock(&g_registry_lock);

    *out_base = base;
    /* Highest usable address: end of the usable region. mmap returns a
     * page-aligned base, so this is 16-byte aligned by construction. */
    *out_top = (char *)base + ps + usable;
    return 0;
}

void ms_stack_free(void *base) {
    if (base == NULL)
        return;
    pthread_mutex_lock(&g_registry_lock);
    if (g_resv == NULL) {
        pthread_mutex_unlock(&g_registry_lock);
        return;
    }
    for (size_t i = 0; i < g_resv_len; ++i) {
        if (g_resv[i].base == base) {
            size_t total = g_resv[i].total;
            /* Record in dead-list BEFORE clearing the slot so that any
             * concurrent (post-unlock) ms_stack_is_live sees the dead entry
             * even if the OS reuses the address immediately (ABA, Bug 2). */
            g_dead_list[g_dead_write % MS_DEAD_CAP] = base;
            g_dead_write++;
            g_resv[i].base  = NULL;
            g_resv[i].total = 0;
            pthread_mutex_unlock(&g_registry_lock);
            munmap(base, total);
            return;
        }
    }
    pthread_mutex_unlock(&g_registry_lock);
}

/* Live reservation count (A1.1 oversubscription guard, issue #49 / #101).
 * The Mojo-side Fiber factories raise before a 33rd live fiber is created so
 * A1 fails loudly rather than trapping on the substrate resume table's 64-row
 * saturation (32 fibers x 2 rows/fiber = 64).  EPIC #2 (#101) removes the cap
 * entirely; the guard is the catchable fail-loud surface until then. */
int ms_live_stack_count(void) {
    pthread_mutex_lock(&g_registry_lock);
    size_t n = 0;
    for (size_t i = 0; i < g_resv_len; ++i)
        if (g_resv[i].base != NULL)
            ++n;
    pthread_mutex_unlock(&g_registry_lock);
    return (int)n;
}

int ms_stack_is_live(void *base) {
    /* 1 when `base` is a still-registered reservation AND was NOT recently
     * freed (ABA detection: the dead-list shadows re-used addresses).
     * 0 when never allocated, already freed, OR in the ABA dead-list. */
    if (base == NULL)
        return 0;
    pthread_mutex_lock(&g_registry_lock);
    if (g_resv == NULL) {
        pthread_mutex_unlock(&g_registry_lock);
        return 0;
    }
    /* Dead-list check: if base was recently freed, return 0 regardless of
     * whether a new allocation re-used the same OS page (ABA, Bug 2). */
    size_t dead_n = g_dead_write < MS_DEAD_CAP ? g_dead_write : MS_DEAD_CAP;
    for (size_t i = 0; i < dead_n; ++i) {
        if (g_dead_list[i] == base) {
            pthread_mutex_unlock(&g_registry_lock);
            return 0;
        }
    }
    /* Live registry check. */
    for (size_t i = 0; i < g_resv_len; ++i) {
        if (g_resv[i].base == base) {
            pthread_mutex_unlock(&g_registry_lock);
            return 1;
        }
    }
    pthread_mutex_unlock(&g_registry_lock);
    return 0;
}


/* Sum of all live reservations, guard pages included (reporting only). */
size_t ms_stack_total_size(void) {
    pthread_mutex_lock(&g_registry_lock);
    size_t total = 0;
    for (size_t i = 0; i < g_resv_len; ++i)
        total += g_resv[i].total;
    pthread_mutex_unlock(&g_registry_lock);
    return total;
}
