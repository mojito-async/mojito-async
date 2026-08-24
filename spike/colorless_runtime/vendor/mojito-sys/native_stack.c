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

#include <errno.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

static size_t g_page_size;

/* Small base -> total-size registry so concurrent live stacks can each be
 * freed with only their base pointer. */
typedef struct {
    void *base;
    size_t total;
} ms_reservation;

static ms_reservation *g_resv;
static size_t g_resv_len, g_resv_cap;

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

    *out_base = base;
    /* Highest usable address: end of the usable region. mmap returns a
     * page-aligned base, so this is 16-byte aligned by construction. */
    *out_top = (char *)base + ps + usable;
    return 0;
}

void ms_stack_free(void *base) {
    if (base == NULL || g_resv == NULL)
        return;

    for (size_t i = 0; i < g_resv_len; ++i) {
        if (g_resv[i].base == base) {
            munmap(base, g_resv[i].total);
            g_resv[i].base  = NULL;
            g_resv[i].total = 0;
            return;
        }
    }
}

size_t ms_stack_total_size(void) {
    /* Sum of all live reservations, guard pages included. */
    size_t total = 0;
    for (size_t i = 0; i < g_resv_len; ++i)
        if (g_resv[i].base != NULL)
            total += g_resv[i].total;
    return total;
}
