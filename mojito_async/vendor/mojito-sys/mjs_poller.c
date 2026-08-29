/* mojito-sys S6.3 — readiness poller, kqueue backend (issue #75).
 *
 * Frozen-ABI entry points (native/include/mojito_sys.h, s6-poller block):
 *   mjs_poller_{create,register,modify,unregister,wait,wake,close}
 *
 * Return contract: 0 == success; negative == -errno; out-params untouched
 * on failure (NULL out-slot => -EFAULT before anything else happens).
 * Hosts without kqueue return exactly -ENOSYS from every entry point
 * (detect-and-exclude, mirroring the s3-atomic-wait contract).
 *
 * Semantics (normative in the header block; implemented here):
 *   - register/modify are UPSERTS: darwin/BSD kevent re-add of an existing
 *     ident/filter updates fflags/data/udata (verified by probe in this
 *     lane), so the last interests+token win;
 *   - EV_CLEAR edge semantics stay BELOW the wrapper: one report per
 *     readiness transition, per spec §29;
 *   - wake is an internal EVFILT_USER knote with EV_CLEAR: NOTE_TRIGGER
 *     wakes at most one kevent waiter and STICKS when nobody waits
 *     (coalescing like the s3-event token);
 *   - wait maps kevent results into the neutral 16-byte mjs_poll_event:
 *     EVFILT_READ/WRITE -> READABLE/WRITABLE, EV_EOF -> EOF, and a
 *     NEGATIVE data under EV_EOF additionally -> ERROR (e.g. ECONNRESET);
 *   - -EINTR passes through RAW for caller retry (§38.11); timeout expiry
 *     is success with zero events; wake deliveries never occupy an out
 *     slot but DO end the wait early.
 *
 * Blocking (SYS-5): only mjs_poller_wait parks its caller (bounded by
 * timeout_ns when given). Everything else never blocks. Allocation
 * (SYS-4): one fixed-size handle at create, none afterwards — the kevent
 * changelist lives on this file's stack.
 */

#include <errno.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <time.h>

#include "mojito_sys.h"

#if defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__) ||   \
    defined(__NetBSD__) || defined(__DragonFly__)
#define MJS_HAVE_KQUEUE 1
#endif

#if defined(MJS_HAVE_KQUEUE)

#include <sys/event.h>
#include <sys/time.h>

struct mjs_poller {
    int kq;
};

/* Identity of the internal wake knote. Arbitrary but fixed; collides
 * with no real descriptor (fd numbers stay small on every supported
 * host) and is filtered out of wait results before they reach callers. */
#define MJS_POLLER_WAKE_IDENT ((uintptr_t)0x4D4A535953ULL)

/* Validate interests: only the two registerable bits may be set. */
static int mjs_check_interests(uint32_t interests)
{
    if ((interests & ~(MJS_POLL_READABLE | MJS_POLL_WRITABLE)) != 0u)
        return -EINVAL;
    if (interests == 0u)
        return -EINVAL;
    return 0;
}

/* Add or remove ONE filter for `ident`. want != 0 adds with EV_CLEAR and
 * the token as udata (re-add UPDATES an existing knote — upsert);
 * want == 0 deletes, tolerating ENOENT (never-registered / racing
 * unregister degrade to no-ops per the header contract). Returns 0 or
 * negative -errno verbatim from the kernel. */
static int mjs_apply_filter(int kq, uintptr_t ident, int16_t filter,
                            int want, uint64_t token)
{
    struct kevent ch;

    if (want) {
        EV_SET(&ch, ident, filter, EV_ADD | EV_CLEAR, 0, 0,
               (void *)(uintptr_t)token);
        if (kevent(kq, &ch, 1, NULL, 0, NULL) == -1)
            return -errno;
        return 0;
    }
    EV_SET(&ch, ident, filter, EV_DELETE, 0, 0, NULL);
    if (kevent(kq, &ch, 1, NULL, 0, NULL) == -1 && errno != ENOENT)
        return -errno;
    return 0;
}

static int mjs_poll_set(mjs_poller *p, int fd, uint32_t interests,
                        uint64_t token)
{
    int rc;

    if (p == NULL)
        return -EINVAL;
    rc = mjs_check_interests(interests);
    if (rc != 0)
        return rc;
    if (fd < 0)
        return -EBADF;

    rc = mjs_apply_filter(p->kq, (uintptr_t)fd, EVFILT_READ,
                          (interests & MJS_POLL_READABLE) != 0u, token);
    if (rc != 0)
        return rc;
    return mjs_apply_filter(p->kq, (uintptr_t)fd, EVFILT_WRITE,
                            (interests & MJS_POLL_WRITABLE) != 0u, token);
}

int mjs_poller_create(mjs_poller **out)
{
    struct kevent ch;
    mjs_poller *p;
    int kq;

    if (out == NULL)
        return -EFAULT;
    kq = kqueue();
    if (kq == -1)
        return -errno;

    /* Pre-register the sticky wake source: EV_CLEAR so each trigger
     * releases exactly one waiter and coalesces while unobserved. */
    EV_SET(&ch, MJS_POLLER_WAKE_IDENT, EVFILT_USER, EV_ADD | EV_CLEAR, 0, 0,
           NULL);
    if (kevent(kq, &ch, 1, NULL, 0, NULL) == -1) {
        int saved = errno;
        close(kq);
        return -saved;
    }

    p = (mjs_poller *)malloc(sizeof(*p));
    if (p == NULL) {
        close(kq);
        return -ENOMEM;
    }
    p->kq = kq;
    *out = p;
    return 0;
}

int mjs_poller_register(mjs_poller *p, int fd, uint32_t interests,
                        uint64_t token)
{
    return mjs_poll_set(p, fd, interests, token);
}

int mjs_poller_modify(mjs_poller *p, int fd, uint32_t interests,
                      uint64_t token)
{
    return mjs_poll_set(p, fd, interests, token);
}

int mjs_poller_unregister(mjs_poller *p, int fd)
{
    int rc;

    if (p == NULL)
        return -EINVAL;
    if (fd < 0)
        return -EBADF;
    rc = mjs_apply_filter(p->kq, (uintptr_t)fd, EVFILT_READ, 0, 0);
    if (rc != 0)
        return rc;
    return mjs_apply_filter(p->kq, (uintptr_t)fd, EVFILT_WRITE, 0, 0);
}

int mjs_poller_wait(mjs_poller *p, mjs_poll_event *events, unsigned cap,
                    const uint64_t *timeout_ns, unsigned *out_n)
{
    /* Stack batch: caps one wait's delivery at 256 events; larger ready
     * sets are DROPPED by contract (callers loop). 256 * sizeof(kevent)
     * keeps the frame well under any sane stack budget. */
    struct kevent ev[256];
    struct timespec ts;
    struct timespec *tsp = NULL;
    unsigned nret = 0;
    unsigned filled = 0;
    unsigned i;
    int n;

    if (p == NULL || out_n == NULL)
        return -EFAULT;
    if (cap == 0u)
        return -EINVAL;
    if (events == NULL)
        return -EFAULT;

    if (timeout_ns != NULL) {
        ts.tv_sec = (time_t)(*timeout_ns / 1000000000ull);
        ts.tv_nsec = (long)(*timeout_ns % 1000000000ull);
        tsp = &ts;
    }

    n = kevent(p->kq, NULL, 0, ev, (int)(cap < 256u ? cap : 256u), tsp);
    if (n == -1)
        return -errno; /* raw -EINTR rides through for §38.11 retry */

    for (i = 0; i < (unsigned)n; i++) {
        uint32_t flags = 0;

        if ((uintptr_t)ev[i].ident == MJS_POLLER_WAKE_IDENT &&
            ev[i].filter == EVFILT_USER)
            continue; /* wake consumed a slot; it never reaches callers */

        if (filled >= cap)
            continue; /* beyond-cap drop is contractual; callers loop */

        if (ev[i].filter == EVFILT_WRITE)
            flags |= MJS_POLL_WRITABLE;
        else
            flags |= MJS_POLL_READABLE;
        if ((ev[i].flags & EV_EOF) != 0) {
            flags |= MJS_POLL_EOF;
            if (ev[i].data < 0)
                flags |= MJS_POLL_ERROR;
        }

        events[filled].token = (uint64_t)(uintptr_t)ev[i].udata;
        events[filled].fd = (int32_t)ev[i].ident;
        events[filled].events = flags;
        filled++;
    }

    nret = filled;
    *out_n = nret;
    return 0;
}

int mjs_poller_wake(mjs_poller *p)
{
    struct kevent ch;

    if (p == NULL)
        return -EINVAL;
    EV_SET(&ch, MJS_POLLER_WAKE_IDENT, EVFILT_USER, 0, NOTE_TRIGGER, 0,
           NULL);
    if (kevent(p->kq, &ch, 1, NULL, 0, NULL) == -1)
        return -errno;
    return 0;
}

int mjs_poller_close(mjs_poller **p)
{
    if (p == NULL || *p == NULL)
        return -EINVAL;
    close((*p)->kq);
    free(*p);
    *p = NULL;
    return 0;
}

#else /* !MJS_HAVE_KQUEUE — detect-and-exclude stubs (-ENOSYS exactly) */

struct mjs_poller {
    int unused;
};

int mjs_poller_create(mjs_poller **out)
{
    (void)out;
    return -ENOSYS;
}

int mjs_poller_register(mjs_poller *p, int fd, uint32_t interests,
                        uint64_t token)
{
    (void)p;
    (void)fd;
    (void)interests;
    (void)token;
    return -ENOSYS;
}

int mjs_poller_modify(mjs_poller *p, int fd, uint32_t interests,
                      uint64_t token)
{
    (void)p;
    (void)fd;
    (void)interests;
    (void)token;
    return -ENOSYS;
}

int mjs_poller_unregister(mjs_poller *p, int fd)
{
    (void)p;
    (void)fd;
    return -ENOSYS;
}

int mjs_poller_wait(mjs_poller *p, mjs_poll_event *events, unsigned cap,
                    const uint64_t *timeout_ns, unsigned *out_n)
{
    (void)p;
    (void)events;
    (void)cap;
    (void)timeout_ns;
    (void)out_n;
    return -ENOSYS;
}

int mjs_poller_wake(mjs_poller *p)
{
    (void)p;
    return -ENOSYS;
}

int mjs_poller_close(mjs_poller **p)
{
    (void)p;
    return -ENOSYS;
}

#endif /* MJS_HAVE_KQUEUE */
