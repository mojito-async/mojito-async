/* mojito-sys S6.6 — experimental io_uring readiness backend (issue #78).
 *
 * Frozen-ABI entry points (native/include/mojito_sys.h, s6-ioring block):
 *   mjs_iouring_{probe,available,create,register,modify,unregister,wait,
 *                wake,close,entries}
 *
 * Return contract: 0 == success; negative == -errno; out-params untouched on
 * failure (NULL out-slot => -EFAULT before anything else happens). Hosts
 * without io_uring (e.g. Darwin) return EXACTLY -ENOSYS from every entry
 * point that requires a live ring (detect-and-exclude, mirroring the
 * s6-poller backend); mjs_iouring_probe/available are pure predicates and
 * return 0 there.
 *
 * CAPABILITY FLAG (spec §28: "io_uring MUST remain behind a
 * capability/feature flag until ..."): a ring is ONLY instantiated when the
 * host kernel supports io_uring AND the explicit environment flag
 * MOJITO_IO_URING=1 is set. mjs_iouring_available() is the authoritative
 * predicate; mjs_iouring_create() returns -ENOSYS whenever it is false. The
 * Mojo surface raises a decoded error at construction in that case.
 *
 * DESIGN (raw io_uring, no liburing dependency):
 *   - setup via io_uring_setup(); SQ/CQ and SQE rings mmap'd with
 *     MAP_POPULATE; ring indices managed inline with kernel-documented
 *     tail/head orderings.
 *   - A registration is ONE one-shot POLL_ADD (IOSQE_ASYNC, edge). POLL_ADD
 *     fires ONCE, so after a delivered event the poll is re-armed (fresh seq
 *     user_data) on the next wait — edge semantics below the wrapper (§29).
 *   - register/modify UPSERT (cancel-then-add; last interests+token win);
 *     unregister cancels and drops the slot. Each live poll carries a unique
 *     seq user_data resolved through a small fd-indexed table so delivered
 *     events carry the caller's token EXACTLY (§31).
 *   - wake is an internal eventfd armed with a poll_add
 *     (user_data == MJS_URING_WAKE); mjs_iouring_wake writes to it. The wake
 *     CQE is consumed (never delivered) and ends a blocked wait promptly; a
 *     wake with no waiter leaves the byte pending so exactly one later wait
 *     sees it (stickiness, mirroring the s6-poller eventfd poll).
 *   - queue-full (§38.7): before handing out an SQE, if the SQ has no room
 *     we io_uring_enter(..., 0, 0) to flush completed submissions, then
 *     retry once; if still full a control op returns -EAGAIN (SYS-5: NEVER
 *     block in a control op).
 *   - timed wait: a relative IORING_OP_TIMEOUT SQE (user_data
 *     MJS_URING_TIMEOUT) arms a bounded wait; a timeout CQE ends it with
 *     zero events (success).
 *
 * Blocking (SYS-5): ONLY mjs_iouring_wait parks its caller (bounded by
 * timeout_ns when given). register/modify/unregister/wake/close/probe never
 * block. Allocation (SYS-4): one ring handle + its tables at create; a fixed
 * number of SQEs and ring pages afterwards.
 */

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>

#include "mojito_sys.h"

#if defined(__linux__)

#include <unistd.h>
#include <sys/eventfd.h>
#include <sys/syscall.h>
#include <linux/io_uring.h>
#include <linux/time_types.h>
#include <poll.h>

/* ---- raw syscall wrappers (io_uring needs no liburing) -------------------
 * syscall() returns -1 on error with errno set; these decode to the frozen
 * contract's negative -errno. On success they return 0 (or a non-negative
 * informational value). */
static int ioring_setup(unsigned entries, struct io_uring_params *p)
{
    long r = syscall(__NR_io_uring_setup, entries, p);
    if (r < 0)
        return -errno;
    return (int)r;
}

static int ioring_enter(int fd, unsigned to_submit, unsigned min_complete,
                        unsigned flags)
{
    long r = syscall(__NR_io_uring_enter, fd, to_submit, min_complete,
                     flags, NULL, 0);
    if (r < 0)
        return -errno;
    return (int)r;
}

/* Unique poll-add user_data markers. Real poll re-arms use a monotone seq
 * taken from the range below MJS_URING_FIRST_SEQ so they can never collide
 * with the two sentinels. */
#define MJS_URING_WAKE      ((uint64_t)0xFFFFFFFFFFFFFFFEULL)
#define MJS_URING_TIMEOUT   ((uint64_t)0xFFFFFFFFFFFFFFFDULL)
#define MJS_URING_FIRST_SEQ ((uint64_t)0x1000ULL)

struct mjs_uring {
    int ring_fd;               /* the io_uring fd */
    int wake_fd;               /* internal eventfd (pending = stuck wake) */
    unsigned sq_entries;
    unsigned cq_entries;

    /* mmap'd ring memory */
    struct io_uring_sqe *sqes;
    unsigned char *sq_ring;
    unsigned char *cq_ring;
    uint32_t *sq_head;
    uint32_t *sq_tail;
    uint32_t *sq_mask;
    uint32_t *sq_array;
    uint32_t *cq_head;
    uint32_t *cq_tail;
    uint32_t *cq_mask;
    struct io_uring_cqe *cqes;

    /* SQ pending-tail (indexes handed out but not yet committed) */
    unsigned sqe_tail;
    /* Local CQ head (indexes consumed locally but not yet committed) */
    unsigned cqe_head;

    /* fd -> (seq, token, interests) table; parallel arrays, linear-probed by
     * fd. r_seqs hold the user_data of each fd's LIVE poll_add so POLL_REMOVE
     * and completion decoding both resolve. */
    int *r_fds;
    uint64_t *r_seqs;
    uint64_t *r_tok;
    uint32_t *r_int;
    unsigned r_count;
    unsigned r_cap;

    uint64_t next_seq;
    int wake_armed;            /* eventfd poll_add currently live */
    int timeout_armed;         /* a relative timeout SQE is pending */
};

static uint64_t uring_next_seq(struct mjs_uring *u)
{
    uint64_t s = u->next_seq;
    u->next_seq++;
    return s;
}

/* Hash + linear probe over the fd table. The probe is bounded to r_cap
 * iterations so a full table (no empty slot) cannot livelock; the caller
 * checks r_count >= r_cap before inserting. */
static unsigned uring_slot(struct mjs_uring *u, int fd)
{
    unsigned h = (unsigned)((uint64_t)fd * 2654435761u) & (u->r_cap - 1u);
    unsigned i;
    for (i = 0; i < u->r_cap; i++) {
        if (u->r_fds[h] == -1 || u->r_fds[h] == fd)
            break;
        h = (h + 1u) & (u->r_cap - 1u);
    }
    return h;
}

/* Validate interests: only the two registerable bits may be set. */
static int mjs_check_interests(uint32_t interests)
{
    if ((interests & ~(MJS_POLL_READABLE | MJS_POLL_WRITABLE)) != 0u)
        return -EINVAL;
    if (interests == 0u)
        return -EINVAL;
    return 0;
}

static int mjs_interests_to_poll(uint32_t interests)
{
    /* POLLHUP/POLLERR are always watched: EOF/error delivery comes from
     * them (unmaskable mask bits). POLLIN/POLLOUT map 1:1 to interests. */
    int m = POLLHUP | POLLERR;
    if (interests & MJS_POLL_READABLE)
        m |= POLLIN;
    if (interests & MJS_POLL_WRITABLE)
        m |= POLLOUT;
    return m;
}

/* ---- SQ submission helpers -------------------------------------------------
 * get_sqe returns a zeroed SQE or NULL when the SQ is full (queue-full;
 * caller flushes via ioring_enter and may retry — never blocks). */

static struct io_uring_sqe *uring_get_sqe(struct mjs_uring *u)
{
    struct io_uring_sqe *sqe = NULL;
    unsigned tail = u->sqe_tail;
    unsigned idx = tail & *u->sq_mask;

    if (tail - *u->sq_head < u->sq_entries) {
        sqe = &u->sqes[idx];
        memset(sqe, 0, sizeof(*sqe));
        u->sq_array[idx] = idx;
        u->sqe_tail = tail + 1;
    }
    return sqe;
}

/* Commit sqes handed out since the last commit and optionally submit. */
static int uring_submit(struct mjs_uring *u, unsigned min_complete,
                        unsigned flags)
{
    unsigned pending = u->sqe_tail - *u->sq_head;

    if (pending == 0u && min_complete == 0u)
        return 0;
    /* io_uring protocol: release barrier BEFORE publishing sq_tail so the
     * kernel sees all prior SQE writes before the tail store. */
    __sync_synchronize();
    *u->sq_tail = u->sqe_tail;
    return ioring_enter(u->ring_fd, pending, min_complete, flags);
}

/* Stage one poll_add SQE (not yet submitted). */
static int uring_stage_poll_add(struct mjs_uring *u, int fd, int poll_mask,
                                uint64_t user_data)
{
    struct io_uring_sqe *sqe = uring_get_sqe(u);
    if (sqe == NULL)
        return -EAGAIN;
    sqe->opcode = IORING_OP_POLL_ADD;
    sqe->flags = IOSQE_ASYNC;
    sqe->fd = fd;
    sqe->poll_events = (uint16_t)poll_mask;
    sqe->user_data = user_data;
    return 0;
}

/* Stage one poll_remove SQE targeting the poll whose user_data == d. */
static int uring_stage_poll_remove(struct mjs_uring *u, uint64_t d)
{
    struct io_uring_sqe *sqe = uring_get_sqe(u);
    if (sqe == NULL)
        return -EAGAIN;
    sqe->opcode = IORING_OP_POLL_REMOVE;
    sqe->addr = d;
    return 0;
}

/* Stage one relative-timeout SQE (user_data = MJS_URING_TIMEOUT). `ts` must
 * stay valid until the batch is submitted. */
static int uring_stage_timeout(struct mjs_uring *u,
                               const struct __kernel_timespec *ts)
{
    struct io_uring_sqe *sqe = uring_get_sqe(u);
    if (sqe == NULL)
        return -EAGAIN;
    sqe->opcode = IORING_OP_TIMEOUT;
    sqe->addr = (uint64_t)(unsigned long)ts;
    sqe->len = 1;
    sqe->timeout_flags = 0; /* 0 = relative */
    sqe->user_data = MJS_URING_TIMEOUT;
    return 0;
}

/* Stage one timeout-remove SQE cancelling the armed timeout whose user_data
 * == MJS_URING_TIMEOUT (TIMEOUT_REMOVE matches by sqe->addr user_data). */
static int uring_stage_timeout_remove(struct mjs_uring *u)
{
    struct io_uring_sqe *sqe = uring_get_sqe(u);
    if (sqe == NULL)
        return -EAGAIN;
    sqe->opcode = IORING_OP_TIMEOUT_REMOVE;
    sqe->addr = MJS_URING_TIMEOUT;
    return 0;
}

/* Stage a poll_add, flushing the SQ once on queue-full (-EAGAIN); then
 * submit the batch. Never blocks. Returns 0 or negative errno. */
static int uring_submit_poll_add(struct mjs_uring *u, int fd, int poll_mask,
                                 uint64_t user_data)
{
    int rc = uring_stage_poll_add(u, fd, poll_mask, user_data);
    if (rc == -EAGAIN) {
        rc = uring_submit(u, 0, 0); /* flush to free SQ slots */
        if (rc < 0)
            return rc;
        rc = uring_stage_poll_add(u, fd, poll_mask, user_data);
    }
    if (rc != 0)
        return rc;
    return uring_submit(u, 0, 0);
}

/* mjs_iouring_probe(): pure predicate — does THIS KERNEL support io_uring?
 * Cheap setup probe: open (and immediately tear down) a tiny ring. */
int mjs_iouring_probe(void)
{
    struct io_uring_params p;
    memset(&p, 0, sizeof(p));
    int fd = ioring_setup(8u, &p);
    if (fd >= 0) {
        close(fd);
        return 1;
    }
    return 0;
}

/* mjs_iouring_available(): the full capability predicate — host support AND
 * the explicit MOJITO_IO_URING=1 flag (spec §28). Pure; never blocks. */
int mjs_iouring_available(void)
{
    const char *flag = getenv("MOJITO_IO_URING");
    if (flag == NULL || strcmp(flag, "1") != 0)
        return 0;
    return mjs_iouring_probe();
}

static size_t round_page(size_t n)
{
    long page = sysconf(_SC_PAGESIZE);
    if (page <= 0)
        page = 4096;
    return (n + (size_t)page - 1u) & ~((size_t)page - 1u);
}

static void iouring_unmap(struct mjs_uring *u)
{
    size_t sq_sz = round_page((size_t)u->sq_entries * sizeof(uint32_t) * 4u);
    size_t cq_sz = round_page((size_t)u->cq_entries *
                              sizeof(struct io_uring_cqe) * 4u);
    size_t sqe_sz = round_page((size_t)u->sq_entries *
                               sizeof(struct io_uring_sqe));
    if (u->sq_ring != MAP_FAILED && u->sq_ring != NULL)
        munmap(u->sq_ring, sq_sz);
    if (u->cq_ring != MAP_FAILED && u->cq_ring != NULL)
        munmap(u->cq_ring, cq_sz);
    if (u->sqes != MAP_FAILED && u->sqes != NULL)
        munmap(u->sqes, sqe_sz);
}

/* Map the SQ/CQ/SQE rings from the ring_fd into user space. */
static int iouring_map_rings(struct mjs_uring *u,
                             const struct io_uring_params *p)
{
    size_t sq_sz = p->sq_off.array + (size_t)p->sq_entries * sizeof(uint32_t);
    size_t cq_sz = p->cq_off.cqes + (size_t)p->cq_entries *
                   sizeof(struct io_uring_cqe);

    u->sq_ring = (unsigned char *)mmap(NULL, round_page(sq_sz),
                                       PROT_READ | PROT_WRITE,
                                       MAP_SHARED | MAP_POPULATE,
                                       u->ring_fd, IORING_OFF_SQ_RING);
    if (u->sq_ring == MAP_FAILED)
        return -errno;
    u->cq_ring = (unsigned char *)mmap(NULL, round_page(cq_sz),
                                       PROT_READ | PROT_WRITE,
                                       MAP_SHARED | MAP_POPULATE,
                                       u->ring_fd, IORING_OFF_CQ_RING);
    if (u->cq_ring == MAP_FAILED) {
        int saved = errno;
        iouring_unmap(u);
        return -saved;
    }
    u->sqes = (struct io_uring_sqe *)mmap(
        NULL, round_page((size_t)p->sq_entries * sizeof(struct io_uring_sqe)),
        PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, u->ring_fd,
        IORING_OFF_SQES);
    if (u->sqes == MAP_FAILED) {
        int saved = errno;
        iouring_unmap(u);
        return -saved;
    }

    u->sq_head  = (uint32_t *)(u->sq_ring + p->sq_off.head);
    u->sq_tail  = (uint32_t *)(u->sq_ring + p->sq_off.tail);
    u->sq_mask  = (uint32_t *)(u->sq_ring + p->sq_off.ring_mask);
    u->sq_array = (uint32_t *)(u->sq_ring + p->sq_off.array);
    u->cq_head  = (uint32_t *)(u->cq_ring + p->cq_off.head);
    u->cq_tail  = (uint32_t *)(u->cq_ring + p->cq_off.tail);
    u->cq_mask  = (uint32_t *)(u->cq_ring + p->cq_off.ring_mask);
    u->cqes     = (struct io_uring_cqe *)(u->cq_ring + p->cq_off.cqes);
    u->sq_entries = p->sq_entries;
    u->cq_entries = p->cq_entries;
    return 0;
}

int mjs_iouring_create(mjs_uring **out)
{
    struct io_uring_params p;
    struct mjs_uring *u;
    int fd;
    int rc;
    unsigned i;

    if (out == NULL)
        return -EFAULT;
    if (!mjs_iouring_available())
        return -ENOSYS; /* flag missing OR host unsupported: detect-and-exclude */

    memset(&p, 0, sizeof(p));
    fd = ioring_setup(128u, &p);
    if (fd < 0)
        return -ENOSYS;

    u = (struct mjs_uring *)calloc(1, sizeof(*u));
    if (u == NULL) {
        close(fd);
        return -ENOMEM;
    }
    u->ring_fd = fd;
    u->next_seq = MJS_URING_FIRST_SEQ;
    u->wake_fd = -1;
    u->sq_ring = MAP_FAILED;
    u->cq_ring = MAP_FAILED;
    u->sqes = MAP_FAILED;

    rc = iouring_map_rings(u, &p);
    if (rc != 0) {
        close(fd);
        free(u);
        return rc;
    }

    u->r_cap = (u->sq_entries < 16u) ? 16u : u->sq_entries;
    u->r_fds = (int *)malloc((size_t)u->r_cap * sizeof(int));
    u->r_seqs = (uint64_t *)malloc((size_t)u->r_cap * sizeof(uint64_t));
    u->r_tok = (uint64_t *)malloc((size_t)u->r_cap * sizeof(uint64_t));
    u->r_int = (uint32_t *)malloc((size_t)u->r_cap * sizeof(uint32_t));
    if (u->r_fds == NULL || u->r_seqs == NULL || u->r_tok == NULL ||
        u->r_int == NULL) {
        rc = -ENOMEM;
        iouring_unmap(u);
        free(u->r_fds);
        free(u->r_seqs);
        free(u->r_tok);
        free(u->r_int);
        close(fd);
        free(u);
        return rc;
    }
    for (i = 0; i < u->r_cap; i++)
        u->r_fds[i] = -1;
    u->r_count = 0;

    /* internal wake eventfd: once readable, a wait returns promptly. */
    u->wake_fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (u->wake_fd < 0) {
        rc = -errno;
        iouring_unmap(u);
        free(u->r_fds);
        free(u->r_seqs);
        free(u->r_tok);
        free(u->r_int);
        close(fd);
        free(u);
        return rc;
    }

    /* Arm the wake poll so a wake byte collapses into a prompt CQE. */
    rc = uring_submit_poll_add(u, u->wake_fd, POLLIN, MJS_URING_WAKE);
    if (rc != 0) {
        close(u->wake_fd);
        iouring_unmap(u);
        free(u->r_fds);
        free(u->r_seqs);
        free(u->r_tok);
        free(u->r_int);
        close(fd);
        free(u);
        return rc;
    }
    u->wake_armed = 1;

    *out = u;
    return 0;
}

/* Register `fd` (or upsert): cancel any live poll, then add a fresh one. */
int mjs_iouring_register(mjs_uring *p, int fd, uint32_t interests,
                         uint64_t token)
{
    unsigned slot;
    int rc;
    uint64_t seq;

    if (p == NULL)
        return -EINVAL;
    rc = mjs_check_interests(interests);
    if (rc != 0)
        return rc;
    if (fd < 0)
        return -EBADF;

    if (p->r_count >= p->r_cap)
        return -ENOSPC; /* table full: callers must unregister first */
    slot = uring_slot(p, fd);
    seq = uring_next_seq(p);
    if (p->r_fds[slot] == fd) {
        /* Upsert: remove the live poll request, add a new one. */
        rc = uring_stage_poll_remove(p, p->r_seqs[slot]);
        if (rc != 0)
            return rc;
        rc = uring_stage_poll_add(p, fd, mjs_interests_to_poll(interests),
                                  seq);
        if (rc != 0)
            return rc;
        rc = uring_submit(p, 0, 0);
        if (rc < 0)
            return rc;
        p->r_seqs[slot] = seq;
        p->r_tok[slot] = token;
        p->r_int[slot] = interests;
        return 0;
    }
    rc = uring_submit_poll_add(p, fd, mjs_interests_to_poll(interests), seq);
    if (rc != 0)
        return rc;
    p->r_fds[slot] = fd;
    p->r_seqs[slot] = seq;
    p->r_tok[slot] = token;
    p->r_int[slot] = interests;
    p->r_count++;
    return 0;
}

int mjs_iouring_modify(mjs_uring *p, int fd, uint32_t interests,
                       uint64_t token)
{
    if (p == NULL)
        return -EINVAL;
    if (fd < 0)
        return -EBADF;
    /* mjs_iouring_register on an existing fd is the upsert path. */
    return mjs_iouring_register(p, fd, interests, token);
}

int mjs_iouring_unregister(mjs_uring *p, int fd)
{
    unsigned slot;
    int rc;

    if (p == NULL)
        return -EINVAL;
    if (fd < 0)
        return -EBADF;
    slot = uring_slot(p, fd);
    if (p->r_fds[slot] != fd)
        return 0; /* not-registered: no-op (header contract) */
    rc = uring_stage_poll_remove(p, p->r_seqs[slot]);
    if (rc != 0)
        return rc;
    rc = uring_submit(p, 0, 0);
    if (rc < 0)
        return rc;
    p->r_fds[slot] = -1;
    p->r_count--;
    return 0;
}

/* Drain all completed CQEs into `events` (up to cap). Skips/consumes the
 * internal wake and timeout markers. Returns events filled, or negative
 * errno. After a real poll CQE the slot is marked un-armed (r_seqs[i]=0) so
 * the next wait re-arms it. */
static int uring_drain_cq(struct mjs_uring *p, mjs_poll_event *events,
                          unsigned cap, int *saw_wake, int *saw_timeout)
{
    uint32_t cq_head = p->cqe_head;
    uint32_t tail = *p->cq_tail;
    unsigned filled = 0;

    /* io_uring protocol: acquire barrier after reading cq_tail, before
     * dereferencing the CQEs the kernel produced. */
    __sync_synchronize();

    while (cq_head != tail) {
        struct io_uring_cqe *cqe = &p->cqes[cq_head & *p->cq_mask];
        cq_head++;

        if (cqe->user_data == MJS_URING_TIMEOUT) {
            *saw_timeout = 1;
            p->timeout_armed = 0;
            continue;
        }
        if (cqe->user_data == MJS_URING_WAKE) {
            uint64_t v;
            ssize_t drawn;
            *saw_wake = 1;
            p->wake_armed = 0;
            /* Drain the wake eventfd so its sticky counter resets: without
             * this, the re-armed wake poll would fire immediately on the next
             * wait (one-release-then-clear semantics, mirroring the epoll
             * backend's wake drain). */
            drawn = read(p->wake_fd, &v, sizeof(v));
            (void)drawn;
            continue;
        }
        {
            unsigned i;
            int found = 0;
            for (i = 0; i < p->r_cap; i++) {
                if (p->r_fds[i] >= 0 && p->r_seqs[i] == cqe->user_data) {
                    found = 1;
                    if (cqe->res < 0) {
                        /* Poll failed (e.g. fd closed while registered):
                         * silently retire the registration (§31 close-
                         * while-registered). */
                        p->r_fds[i] = -1;
                        p->r_count--;
                        break;
                    }
                    if (filled < cap) {
                        int res = cqe->res;
                        uint32_t flags = 0;
                        if (res & POLLIN)
                            flags |= MJS_POLL_READABLE;
                        if (res & POLLOUT)
                            flags |= MJS_POLL_WRITABLE;
                        if (res & POLLHUP)
                            flags |= MJS_POLL_EOF;
                        if (res & POLLERR)
                            flags |= MJS_POLL_ERROR;
                        events[filled].token = p->r_tok[i];
                        events[filled].fd = p->r_fds[i];
                        events[filled].events = flags;
                        filled++;
                    }
                    /* One-shot: the slot needs a re-arm on the next wait. */
                    p->r_seqs[i] = 0;
                    break;
                }
            }
            /* Unknown seq (already-rearmed stale event) or slot dropped:
             * consumed and ignored. */
            (void)found;
        }
    }

    p->cqe_head = cq_head;
    __sync_synchronize();
    *p->cq_head = cq_head;
    return (int)filled;
}

int mjs_iouring_wait(mjs_uring *p, mjs_poll_event *events, unsigned cap,
                     const uint64_t *timeout_ns, unsigned *out_n)
{
    struct __kernel_timespec ts;
    unsigned to_submit = 0;
    int rc;
    int saw_wake = 0;
    int saw_timeout = 0;
    unsigned filled;
    unsigned i;

    if (p == NULL || out_n == NULL)
        return -EFAULT;
    if (cap == 0u)
        return -EINVAL;
    if (events == NULL)
        return -EFAULT;

    /* Re-arm any fd whose one-shot poll fired since the last wait. */
    for (i = 0; i < p->r_cap; i++) {
        if (p->r_fds[i] >= 0 && p->r_seqs[i] == 0) {
            uint64_t seq = uring_next_seq(p);
            rc = uring_stage_poll_add(p, p->r_fds[i],
                                      mjs_interests_to_poll(p->r_int[i]), seq);
            if (rc != 0)
                return rc;
            p->r_seqs[i] = seq;
            to_submit++;
        }
    }
    /* Re-arm the wake poll if it fired. */
    if (!p->wake_armed) {
        rc = uring_stage_poll_add(p, p->wake_fd, POLLIN, MJS_URING_WAKE);
        if (rc != 0)
            return rc;
        p->wake_armed = 1;
        to_submit++;
    }
    /* Bounded wait armed FRESH from the caller's timeout on every call: we
     * never reuse a previous relative deadline. A stale timeout left armed
     * from an earlier short wait points at a dead stack timespec, so first
     * remove it, then stage a fresh relative timeout. */
    if (timeout_ns != NULL) {
        unsigned long long nsec = *timeout_ns;
        ts.tv_sec = (__kernel_time_t)(nsec / 1000000000ULL);
        ts.tv_nsec = (long)(nsec % 1000000000ULL);
        if (p->timeout_armed) {
            rc = uring_stage_timeout_remove(p);
            if (rc != 0)
                return rc;
            p->timeout_armed = 0;
            to_submit++;
        }
        rc = uring_stage_timeout(p, &ts);
        if (rc != 0)
            return rc;
        p->timeout_armed = 1;
        to_submit++;
    }

    /* Submit pending SQEs and block for at least one completion, bounded by
     * the timeout SQE when armed. GETEVENTS makes the wait actually wait. */
    rc = uring_submit(p, (to_submit != 0u) ? 1u : 1u, IORING_ENTER_GETEVENTS);
    if (rc < 0)
        return rc; /* -EINTR rides through raw for §38.11 retry */

    filled = (unsigned)uring_drain_cq(p, events, cap, &saw_wake, &saw_timeout);
    /* A wake CQE ends a blocked wait promptly; the consumed wake is invisible
     * to callers. Any events that accompanied it are delivered. Timeout
     * expiry is success-with-zero. */
    (void)saw_wake;
    (void)saw_timeout;
    *out_n = filled;
    return 0;
}

int mjs_iouring_wake(mjs_uring *p)
{
    uint64_t one = 1;
    ssize_t r;

    if (p == NULL)
        return -EINVAL;
    /* Non-blocking write: a pending byte (coalesced wake) surfaces as EAGAIN
     * — still a success for stickiness. */
    r = write(p->wake_fd, &one, sizeof(one));
    if (r < 0 && errno != EAGAIN && errno != EWOULDBLOCK)
        return -errno;
    return 0;
}

int mjs_iouring_entries(mjs_uring *p, unsigned *out_sq, unsigned *out_cq)
{
    if (p == NULL || out_sq == NULL || out_cq == NULL)
        return -EFAULT;
    *out_sq = p->sq_entries;
    *out_cq = p->cq_entries;
    return 0;
}

int mjs_iouring_close(mjs_uring **p)
{
    struct mjs_uring *u;

    if (p == NULL || *p == NULL)
        return -EINVAL;
    u = *p;
    if (u->wake_fd >= 0)
        close(u->wake_fd);
    iouring_unmap(u);
    close(u->ring_fd);
    free(u->r_fds);
    free(u->r_seqs);
    free(u->r_tok);
    free(u->r_int);
    free(u);
    *p = NULL;
    return 0;
}

#else /* !__linux__ — detect-and-exclude stubs (-ENOSYS exactly) */

/* On every non-Linux host (e.g. Darwin/arm64) io_uring is unavailable.
 * mjs_iouring_probe/available are pure predicates returning 0; all entries
 * that need a live ring return EXACTLY -ENOSYS (detect-and-exclude, mirror
 * the s6-poller backend) so the packaged dylib always links and the darwin
 * build stays green. mjs_iouring_available() being 0 is what lets the Mojo
 * surface raise an explicit "io-uring backend unavailable" error at
 * construction instead of silently half-running. */

struct mjs_uring {
    int unused;
};

int mjs_iouring_probe(void)
{
    return 0;
}

int mjs_iouring_available(void)
{
    /* Even if MOJITO_IO_URING were set, there is no io_uring on this host. */
    return 0;
}

int mjs_iouring_create(mjs_uring **out)
{
    (void)out;
    return -ENOSYS;
}

int mjs_iouring_register(mjs_uring *p, int fd, uint32_t interests,
                         uint64_t token)
{
    (void)p;
    (void)fd;
    (void)interests;
    (void)token;
    return -ENOSYS;
}

int mjs_iouring_modify(mjs_uring *p, int fd, uint32_t interests,
                       uint64_t token)
{
    (void)p;
    (void)fd;
    (void)interests;
    (void)token;
    return -ENOSYS;
}

int mjs_iouring_unregister(mjs_uring *p, int fd)
{
    (void)p;
    (void)fd;
    return -ENOSYS;
}

int mjs_iouring_wait(mjs_uring *p, mjs_poll_event *events, unsigned cap,
                     const uint64_t *timeout_ns, unsigned *out_n)
{
    (void)p;
    (void)events;
    (void)cap;
    (void)timeout_ns;
    (void)out_n;
    return -ENOSYS;
}

int mjs_iouring_wake(mjs_uring *p)
{
    (void)p;
    return -ENOSYS;
}

int mjs_iouring_entries(mjs_uring *p, unsigned *out_sq, unsigned *out_cq)
{
    (void)p;
    (void)out_sq;
    (void)out_cq;
    return -ENOSYS;
}

int mjs_iouring_close(mjs_uring **p)
{
    (void)p;
    return -ENOSYS;
}

#endif /* __linux__ */