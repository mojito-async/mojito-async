/* mojito-sys S6.4 — readiness poller, epoll backend (issue #76).
 *
 * Frozen-ABI entry points (native/include/mojito_sys.h, s6-epoll block):
 *   mjs_epoll_{create,register,modify,unregister,wait,wake,close}
 *
 * Return contract mirrors mjs_poller.c exactly: 0 == success; negative ==
 * -errno; out-params untouched on failure (NULL out-slot => -EFAULT before
 * anything else happens). epoll is LINUX-ONLY; hosts without it (darwin,
 * BSD, Windows) return exactly -ENOSYS from every entry point
 * (detect-and-exclude, mirroring the s6-poller kqueue contract) so
 * `make libmojito_sys.dylib` stays green on macOS.
 *
 * Semantics (normative in the header block; implemented here):
 *   - register/modify are UPSERTS: epoll_ctl EPOLL_CTL_ADD/EPOLL_CTL_MOD on
 *     one entry per registration, so the last interests+token win;
 *   - LEVEL-triggered default (no EPOLLET in the frozen ABI): a ready fd is
 *     reported on every wait until drained — this is the platform-neutral
 *     default per spec §29/§38.7 (the kqueue lane uses EV_CLEAR edge below
 *     the wrapper; epoll deliberately exposes the level default and keeps
 *     edge BELOW the wrapper too, just unselected).
 *   - wake is an internal eventfd registered for EPOLLIN: a wake write
 *     (counter += 1) makes at most one blocked wait return promptly; with
 *     no waiter it STICKS (the counter remains) so exactly one later wait
 *     returns promptly. The eventfd is DRAINED (read to 0) in wait so each
 *     wake releases at most one wait (coalescing N wakes -> one release).
 *   - wait maps epoll_events into the neutral 16-byte mjs_poll_event:
 *     EPOLLIN -> READABLE, EPOLLOUT -> WRITABLE, EPOLLRDHUP|EPOLLHUP ->
 *     EOF, EPOLLERR -> ERROR (union with the readiness bit when both).
 *     The wake eventfd is filtered out before it reaches callers.
 *   - -EINTR passes through RAW for caller retry (§38.11); timeout expiry
 *     is success with zero events; wake deliveries never occupy an out
 *     slot but DO end the wait early (possibly with *out_n == 0).
 *
 * Blocking (SYS-5): only mjs_epoll_wait parks its caller (bounded by
 * timeout_ns when given). register/modify/unregister/wake/close never
 * block. Allocation (SYS-4): one fixed-size handle at create, none
 * afterwards — the epoll_event batch lives on this file's stack.
 */

#include <errno.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>

#include "mojito_sys.h"

#if defined(__linux__)
#define MJS_HAVE_EPOLL 1
#endif

#if defined(MJS_HAVE_EPOLL)

#include <sys/epoll.h>
#include <sys/eventfd.h>

/* Wake eventfd value written on mjs_epoll_wake (arbitrary; only the
 * transition 0 -> nonzero matters for the EPOLLIN wake). */
static const uint64_t MJS_EPOLL_WAKE_VALUE = 1ull;

/* Fixed-capacity open-addressed table mapping the ready DESCRIPTOR ->
 * its token. epoll_event.data is ONE 64-bit slot, so the fd rides in
 * data (recovered on wait) and the token lives in this table only
 * (mirroring mjs_iouring's r_fds). The wake fd is never registered
 * here, so a post-recovery compare to p->wfd discriminates the wake
 * entry. Power of two so `& (CAP - 1)` is a legal wrap mask. */
#define MJS_EPOLL_TABLE_CAP 256u

struct mjs_epoller {
    int epfd;   /* epoll_create1 fd */
    int wfd;    /* eventfd wake source */
    int table_fd[MJS_EPOLL_TABLE_CAP];      /* -1 = empty slot */
    uint64_t table_tok[MJS_EPOLL_TABLE_CAP];
};

/* Match-address slot for `fd` (or the first empty slot if absent). */
static unsigned mjs_epoll_slot(const mjs_epoller *p, int fd)
{
    unsigned h = (unsigned)(((uint64_t)fd * 2654435761u) &
                            (MJS_EPOLL_TABLE_CAP - 1u));
    while (p->table_fd[h] != -1 && p->table_fd[h] != fd)
        h = (h + 1u) & (MJS_EPOLL_TABLE_CAP - 1u);
    return h;
}

/* Validate interests: only the two registerable bits may be set. */
static int mjs_epoll_check_interests(uint32_t interests)
{
    if ((interests & ~(MJS_POLL_READABLE | MJS_POLL_WRITABLE)) != 0u)
        return -EINVAL;
    if (interests == 0u)
        return -EINVAL;
    return 0;
}

/* Add or replace ONE registration. op is EPOLL_CTL_ADD or EPOLL_CTL_MOD;
 * unregister uses EPOLL_CTL_DEL tolerating ENOENT (never-registered /
 * racing unregister degrade to no-ops per the header contract). */
static int mjs_epoll_ctl(mjs_epoller *p, int op, int fd, uint32_t interests)
{
    struct epoll_event ev;

    if (op == EPOLL_CTL_DEL) {
        /* The `events` field is ignored on DELETE but must be valid. */
        memset(&ev, 0, sizeof(ev));
        if (epoll_ctl(p->epfd, EPOLL_CTL_DEL, fd, &ev) == -1 &&
            errno != ENOENT)
            return -errno;
        return 0;
    }

    memset(&ev, 0, sizeof(ev));
    ev.data.fd = fd; /* the ready descriptor rides in epoll_event.data */
    if ((interests & MJS_POLL_READABLE) != 0u)
        ev.events |= EPOLLIN;
    if ((interests & MJS_POLL_WRITABLE) != 0u)
        ev.events |= EPOLLOUT;
    /* EPOLLRDHUP / EPOLLHUP / EPOLLERR are always watched so EOF and error
     * can be reported even when only one readiness half is registered. */
    ev.events |= EPOLLRDHUP;

    if (epoll_ctl(p->epfd, op, fd, &ev) == -1)
        return -errno;
    return 0;
}

static int mjs_epoll_set(mjs_epoller *p, int fd, uint32_t interests,
                         uint64_t token, int is_add)
{
    int rc;
    unsigned slot;

    if (p == NULL)
        return -EINVAL;
    rc = mjs_epoll_check_interests(interests);
    if (rc != 0)
        return rc;
    if (fd < 0)
        return -EBADF;

    slot = mjs_epoll_slot(p, fd);
    if (p->table_fd[slot] != -1 && p->table_fd[slot] != fd)
        return -ENOSPC; /* table full */

    rc = mjs_epoll_ctl(p, is_add ? EPOLL_CTL_ADD : EPOLL_CTL_MOD,
                       fd, interests);
    if (is_add && rc == -EEXIST) {
        /* Upsert parity with kqueue EV_ADD: replace the existing entry
         * (newest interests+token win). */
        rc = mjs_epoll_ctl(p, EPOLL_CTL_MOD, fd, interests);
    }
    if (rc != 0)
        return rc;

    /* Record fd -> token only after the kernel accepted the entry. */
    p->table_fd[slot] = fd;
    p->table_tok[slot] = token;
    return 0;
}

int mjs_epoll_create(mjs_epoller **out)
{
    struct epoll_event ev;
    mjs_epoller *p;
    int epfd;
    int wfd;
    unsigned t;

    if (out == NULL)
        return -EFAULT;
    epfd = epoll_create1(EPOLL_CLOEXEC);
    if (epfd == -1)
        return -errno;
    wfd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    if (wfd == -1) {
        int saved = errno;
        close(epfd);
        return -saved;
    }

    /* Pre-register the sticky wake source: level EPOLLIN on the eventfd.
     * Writes raise the 64-bit counter; the epoll entry fires whenever it
     * is nonzero; wait drains it so each wake releases at most one wait. */
    memset(&ev, 0, sizeof(ev));
    ev.events = EPOLLIN;
    ev.data.fd = wfd;
    if (epoll_ctl(epfd, EPOLL_CTL_ADD, wfd, &ev) == -1) {
        int saved = errno;
        close(wfd);
        close(epfd);
        return -saved;
    }

    p = (mjs_epoller *)malloc(sizeof(*p));
    if (p == NULL) {
        close(wfd);
        close(epfd);
        return -ENOMEM;
    }
    p->epfd = epfd;
    p->wfd = wfd;
    for (t = 0; t < MJS_EPOLL_TABLE_CAP; t++)
        p->table_fd[t] = -1;
    *out = p;
    return 0;
}

int mjs_epoll_register(mjs_epoller *p, int fd, uint32_t interests,
                       uint64_t token)
{
    return mjs_epoll_set(p, fd, interests, token, 1);
}

int mjs_epoll_modify(mjs_epoller *p, int fd, uint32_t interests,
                     uint64_t token)
{
    return mjs_epoll_set(p, fd, interests, token, 0);
}

int mjs_epoll_unregister(mjs_epoller *p, int fd)
{
    unsigned slot;

    if (p == NULL)
        return -EINVAL;
    if (fd < 0)
        return -EBADF;

    slot = mjs_epoll_slot(p, fd);
    if (p->table_fd[slot] == fd)
        p->table_fd[slot] = -1;

    return mjs_epoll_ctl(p, EPOLL_CTL_DEL, fd, 0);
}

int mjs_epoll_wait(mjs_epoller *p, mjs_poll_event *events, unsigned cap,
                   const uint64_t *timeout_ns, unsigned *out_n)
{
    /* Stack batch: caps one wait's delivery at 256 events; larger ready
     * sets are DROPPED by contract (callers loop). */
    struct epoll_event ev[256];
    unsigned filled = 0;
    unsigned i;
    int n;
    int tm = -1; /* epoll_wait timeout in milliseconds; -1 = infinite */

    if (p == NULL || out_n == NULL)
        return -EFAULT;
    if (cap == 0u)
        return -EINVAL;
    if (events == NULL)
        return -EFAULT;

    if (timeout_ns != NULL) {
        uint64_t ns = *timeout_ns;
        if (ns == 0ull)
            tm = 0;
        else {
            uint64_t ms = ns / 1000000ull;
            tm = (ms > (uint64_t)0x7FFFFFFF) ? 0x7FFFFFFF : (int)ms;
        }
    }

    n = epoll_wait(p->epfd, ev, (int)(cap < 256u ? cap : 256u), tm);
    if (n == -1)
        return -errno; /* raw -EINTR rides through for §38.11 retry */

    for (i = 0; i < (unsigned)n; i++) {
        uint32_t flags = 0;
        uint32_t e = ev[i].events;
        int rfd = ev[i].data.fd; /* the ready DESCRIPTOR (not token bits) */

        if (rfd == p->wfd) {
            uint64_t drain;

            /* Wake eventfd: consume its counter so the stickiness is one
             * wait, then continue collecting real events below. The wake
             * fd is never in the token table, so this compare is exact. */
            (void)!read(p->wfd, &drain, sizeof(drain));
            continue;
        }

        if (filled >= cap)
            continue; /* beyond-cap drop is contractual; callers loop */

        if ((e & EPOLLIN) != 0u)
            flags |= MJS_POLL_READABLE;
        if ((e & EPOLLOUT) != 0u)
            flags |= MJS_POLL_WRITABLE;
        if ((e & EPOLLRDHUP) != 0u || (e & EPOLLHUP) != 0u)
            flags |= MJS_POLL_EOF;
        if ((e & EPOLLERR) != 0u)
            flags |= MJS_POLL_ERROR;

        /* epoll returns ONE 64-bit data field holding the fd; recover the
         * fd's token from the per-fd table before emitting. */
        {
            unsigned slot = mjs_epoll_slot(p, rfd);
            if (p->table_fd[slot] != rfd)
                continue; /* raced: registration dropped mid-wait */
            events[filled].token = p->table_tok[slot];
        }
        events[filled].fd = rfd;
        events[filled].events = flags;
        filled++;
    }

    *out_n = filled;
    return 0;
}

int mjs_epoll_wake(mjs_epoller *p)
{
    uint64_t one = MJS_EPOLL_WAKE_VALUE;

    if (p == NULL)
        return -EINVAL;
    /* eventfd is O_NONBLOCK from create; a single-byte write raises the
     * counter and returns EAGAIN only when saturated (practically never). */
    if (write(p->wfd, &one, sizeof(one)) != (ssize_t)sizeof(one))
        return -errno;
    return 0;
}

int mjs_epoll_close(mjs_epoller **p)
{
    if (p == NULL || *p == NULL)
        return -EINVAL;
    close((*p)->epfd);
    close((*p)->wfd);
    free(*p);
    *p = NULL;
    return 0;
}

#else /* !MJS_HAVE_EPOLL — detect-and-exclude stubs (-ENOSYS exactly) */

struct mjs_epoller {
    int unused;
};

int mjs_epoll_create(mjs_epoller **out)
{
    (void)out;
    return -ENOSYS;
}

int mjs_epoll_register(mjs_epoller *p, int fd, uint32_t interests,
                       uint64_t token)
{
    (void)p;
    (void)fd;
    (void)interests;
    (void)token;
    return -ENOSYS;
}

int mjs_epoll_modify(mjs_epoller *p, int fd, uint32_t interests,
                     uint64_t token)
{
    (void)p;
    (void)fd;
    (void)interests;
    (void)token;
    return -ENOSYS;
}

int mjs_epoll_unregister(mjs_epoller *p, int fd)
{
    (void)p;
    (void)fd;
    return -ENOSYS;
}

int mjs_epoll_wait(mjs_epoller *p, mjs_poll_event *events, unsigned cap,
                   const uint64_t *timeout_ns, unsigned *out_n)
{
    (void)p;
    (void)events;
    (void)cap;
    (void)timeout_ns;
    (void)out_n;
    return -ENOSYS;
}

int mjs_epoll_wake(mjs_epoller *p)
{
    (void)p;
    return -ENOSYS;
}

int mjs_epoll_close(mjs_epoller **p)
{
    (void)p;
    return -ENOSYS;
}

#endif /* MJS_HAVE_EPOLL */