#ifndef MOJITO_SYS_H
#define MOJITO_SYS_H

#include <stddef.h>
#include <stdint.h> /* uintptr_t (s2-tls keys) — additive, issue #50 */

/*
 * mojito-sys S1 — frozen C ABI (issue #24): page / VM / stack services.
 *
 * S1 contract:
 *   - Live stack addresses never move. Once a stack region is allocated and
 *     committed, its usable range is stable for the lifetime of the binding;
 *     mjs_stack_free returns it wholesale. (No stack growth/shrink in S1.)
 *   - Every stack region carries guard_bytes (a POSITIVE page multiple) of
 *     PROT_NONE at the LOW end of the reservation (below *out_guard_low).
 *     Crossing it is a hard fault by design. guard_bytes == 0 or a non-page
 *     multiple is an error (-EINVAL).
 *   - Compatibility: the signatures and semantics below are frozen for the
 *     S1 milestone. Extending the ABI means adding new names, never changing
 *     these declarations. The ABI version is MOJITO_SYS_ABI_VERSION.
 *   - mjs_page_size / mjs_granularity are informational. Byte counts passed
 *     to commit/decommit/protect must be positive page multiples by the
 *     caller (validated where cheap; undefined results otherwise).
 *   - Concurrency: these primitives are NOT thread-safe on a shared region.
 *     A reservation/stack region is single-owner; serialize concurrent use
 *     of the same region (or its cursor) by the caller.
 *
 * Return-value contract (all entry points):
 *   0 == success; negative == -errno (e.g. -ENOMEM, -EINVAL, -EFAULT).
 *   Out-parameters are UNTOUCHED on failure.
 */

#define MOJITO_SYS_ABI_VERSION 1

/* Protection flags, opaque at this boundary. Map 1:1 to POSIX PROT_* today;
 * the Mojo layer translates to platform-specific bits. */
#define MJS_PROT_NONE  0x00
#define MJS_PROT_READ  0x01
#define MJS_PROT_WRITE 0x02
#define MJS_PROT_EXEC  0x04

#ifdef __cplusplus
extern "C" {
#endif

/* Host page size (sysconf(_SC_PAGESIZE)); > 0. */
int mjs_page_size(void);

/* Standardized native callback shape (spec §8): a C-ABI function pointer
 * plus an opaque userdata pointer, transported as one two-word token. */
typedef void (*ms_callback)(void *userdata);
typedef struct mjs_callback_token {
    void *addr;      /* ms_callback code address; NULL = none */
    void *userdata;  /* opaque, passed through unmodified */
} mjs_callback_token;

/* Allocation granularity for reservations; >= page size. */
int mjs_granularity(void);

/* ABI version of this library. Consumers that dynload the dylib SHOULD
 * call this and reject MOJITO_SYS_ABI_VERSION mismatches. */
int mjs_abi_version(void);

/* Reserve `bytes` (rounded up to granularity) of address space without
 * committing physical memory (mmap NORESERVE-equivalent). On success returns
 * 0, stores the reservation base in *out_base and the exact reserved size in
 * *out_reserved. */
int mjs_vm_reserve(size_t bytes, void **out_base, size_t *out_reserved);

/* Make `length` bytes at *addr accessible (back the pages; the POSIX
 * mechanism is mprotect/mmap-modifying; NOT mlock: no physical-frame
 * pinning). `length` must be a positive page multiple.
 * On full success *addr advances to the first byte past the committed range.
 * On failure *addr is unchanged (atomic: 0-or-length). The implementation
 * does not validate the range against any reservation — the caller owns the
 * cursor and must keep it inside a live reservation. */
int mjs_vm_commit(unsigned char **addr, size_t length);

/* Make `length` bytes at *addr inaccessible/zero-fill until re-committed.
 * `length` must be a positive page multiple. Same cursor contract as
 * mjs_vm_commit: *addr advances only on full success. */
int mjs_vm_decommit(unsigned char **addr, size_t length);

/* Change protection of `length` bytes at *addr to protection (an OR of
 * MJS_PROT_READ/MJS_PROT_WRITE/MJS_PROT_EXEC, or MJS_PROT_NONE). Returns 0
 * on success. */
int mjs_vm_protect(unsigned char *addr, size_t length, int protection);

/* Release a reservation made by mjs_vm_reserve. *base is NULLed on success.
 * Double-release (already-NULL *base) is a -EINVAL error. Returns 0 on
 * success. */
int mjs_vm_release(void **base, size_t reserved);

/* Allocate a non-moving stack: reserve `reserve` bytes, paint `guard_bytes`
 * (positive page multiple) PROT_NONE at the bottom, and commit the top
 * `initial_commit` bytes of the usable range. Success: 0, with
 * *out_base = reservation base, *out_guard_low = address of the first
 * usable byte, and *out_top = highest usable address, 16-byte aligned for
 * ABI entry. */
int mjs_stack_alloc(size_t reserve, size_t initial_commit, size_t guard_bytes,
                    void **out_base, void **out_guard_low, size_t *out_top);

/* Free a stack allocated by mjs_stack_alloc (whole reservation).
 * *base is NULLed on success. Double-free (already-NULL *base) is a
 * -EINVAL error, mirroring mjs_vm_release. Returns 0 on success. */
int mjs_stack_free(void **base);

/* --- s2-thread --- */
/*
 * S2.1 native OS threads (issue #48). Same return-value contract as above:
 * 0 == success; negative == -errno; out-params UNTOUCHED on failure.
 *
 * Blocking (SYS-5): every primitive here is non-blocking EXCEPT
 * mjs_thread_join, which blocks until the target thread exits.
 *
 * Handle lifetime: mjs_thread_spawn yields a JOINABLE handle owned by the
 * caller. mjs_thread_join or a successful mjs_thread_detach CONSUMES the
 * handle (both NULL *t on success); any later use of a consumed handle
 * (*t == NULL) is a deterministic -EINVAL. A detached thread frees its
 * handle when it exits; join and detach hand the pthread back to the OS
 * (pthread_join / pthread_detach), so no per-thread resources outlive exit.
 */

/* Thread entry point (spec §11): its return value is the status captured
 * for mjs_thread_join. */
typedef long (*ms_thread_entry)(void *userdata);

/* Opaque thread handle (SYS-3). */
typedef struct mjs_thread mjs_thread;

/* Spawn a thread running entry(userdata). stack_size 0 selects the system
 * default; otherwise it must be >= PTHREAD_STACK_MIN (-EINVAL). name
 * (optional, NULL ok) is applied to the new thread inside its trampoline;
 * at most 15 chars + NUL is the portable floor — longer names are rejected
 * with -ENAMETOOLONG. Precondition enforced: entry == NULL is -EINVAL.
 * Non-blocking (SYS-5). On success returns 0 and stores the handle in
 * *out. */
int mjs_thread_spawn(ms_thread_entry entry, void *userdata,
                     size_t stack_size, const char *name,
                     mjs_thread **out);

/* Join a joinable thread. Blocks (SYS-5) until the target thread exits:
 * stores the entry status in *out_result (may be NULL to discard), NULLs
 * *t, and frees the handle. On pthread_join failure returns -errno with
 * the joinable state rolled back — out_result untouched, *t untouched, no
 * refs dropped. Join-after-detach, double-join, or a NULL or NULLed
 * handle is -EINVAL with out_result untouched. */
int mjs_thread_join(mjs_thread **t, long *out_result);

/* Detach a joinable thread: the thread reclaims its own resources at exit
 * (pthread_detach) and self-frees its handle; the caller's reference is
 * consumed — *t is NULLed on success, so double-detach degenerates to a
 * deterministic -EINVAL (*t == NULL). Already detached/joined is also
 * -EINVAL. Non-blocking (SYS-5). */
int mjs_thread_detach(mjs_thread **t);

/* Identity of the CALLING thread (non-blocking). Equality semantics among
 * currently-live threads ONLY — POSIX may reuse pthread_t values after
 * join, so equality across thread lifetimes carries no meaning; the
 * numeric value itself is non-portable (pthread_self cast). */
unsigned long mjs_thread_self_id(void);

/* Set the CALLING thread's name (darwin pthread_setname_np is self-only;
 * SYS-7 divergence: portable floor is 15 chars + NUL, longer -> -ENAMETOOLONG).
 * NULL name is -EINVAL. Non-blocking (SYS-5). */
int mjs_thread_set_name(const char *name);
/* --- s2-tls --- */

/* Thread-local storage keys (issue #50): a validated layer over
 * pthread_key_* with the standard 0 / negative -errno contract. A key is
 * an opaque nonzero uintptr_t minted by mjs_tls_create; it stays valid in
 * every thread until mjs_tls_destroy and is NEVER reused, so a stale
 * handle can never alias a newer key.
 *
 * The destructor is an ms_callback (§8): void (*)(void *), exactly the
 * pthread_key destructor shape, invoked once per thread at thread exit
 * with that thread's bound value (or not at all when NULL). get() never
 * allocates (SYS-4); it returns the calling thread's binding, NULL when
 * unset or the key is invalid. set()/destroy() return -EINVAL for any key
 * not currently live; out-parameters are untouched on failure. */

/* Mint a TLS key whose per-thread values are passed to `destructor`
 * (may be NULL) at thread exit. 0 on success with *out_key = new key;
 * negative errno on failure, *out_key untouched.
 * Blocking (SYS-5): may block briefly on the global registry mutex and
 * inside pthread_key_create.
 * Allocation: may grow the key registry under the lock (one realloc);
 * issues one pthread_key_create syscall.
 * Task-aware: no — bindings are OS-thread-scoped, never task-scoped. */
int mjs_tls_create(ms_callback destructor, uintptr_t *out_key);

/* Calling thread's value for `key`; NULL when unset or key invalid.
 * Never allocates.
 * Blocking (SYS-5): holds the global registry mutex across a brief
 * key-validation critical section (may wait under contention);
 * pthread_getspecific itself does not block.
 * Allocation: none (SYS-4 names TLS reads as must-not-allocate).
 * Task-aware: no. */
void *mjs_tls_get(uintptr_t key);

/* Set the calling thread's value for `key`. -EINVAL for an invalid key.
 * Blocking (SYS-5): holds the global registry mutex across a brief
 * validate-then-set critical section (may wait under contention).
 * Allocation: none.
 * Task-aware: no. */
int mjs_tls_set(uintptr_t key, void *value);

/* Delete `key`. -EINVAL if invalid/double-destroyed. A destructor does NOT
 * run for values still bound at delete time (POSIX semantics).
 * Blocking (SYS-5): holds the global registry mutex across a brief
 * validate-and-retire critical section, released BEFORE pthread_key_delete
 * runs (no host call under the lock).
 * Allocation: none.
 * Task-aware: no. */
int mjs_tls_destroy(uintptr_t key);

/* --- s2-cpu --- */
/* CPU information and affinity (spec §13). Topology information is
 * ADVISORY. Affinity applies to the CALLING THREAD ONLY. Deliberately no
 * get-API (SYS-1): the spec asks set-only. */

/* Logical CPU count visible to this process (sysconf/sysctl); > 0. */
int mjs_cpu_logical(void);

/* Physical CPU (package core) count. Best-effort: 0 with *out > 0 when the
 * host exposes the value; EXACTLY -ENOTSUP when undeterminable, with *out
 * untouched (maps to Optional[Int] upstream; SYS-7 divergence is VISIBLE).
 * Never any other errno. */
int mjs_cpu_physical(int *out);

/* Pin the calling thread (spec L914-921) to the CPUs selected by `mask`:
 * bit i of word w selects logical CPU w*64 + i. mask == NULL or
 * nwords == 0 is -EINVAL. Linux: sched_setaffinity on the calling thread.
 * Darwin: thread_policy_set best-effort; hosts without thread-affinity
 * support return exactly -ENOTSUP. Where the call SUCCEEDS, the mask
 * contents are ignored: the policy is a coarse prefer-current-core hint,
 * not an exact pin — 0 does not promise the exact set was applied. */
int mjs_cpu_affinity_set_current(const uint64_t *mask, unsigned nwords);

/* --- s4-time --- */

/* Monotonic clock reading, normalized to nanoseconds INSIDE the C layer
 * (Linux: clock_gettime CLOCK_MONOTONIC; macOS: mach_absolute_time scaled
 * through the mach_timebase_info ratio, cached via pthread_once so the
 * calibration happens once per process). The value is a non-decreasing
 * count of nanoseconds since an arbitrary epoch (NOT wall-clock time);
 * scheduler deadlines must be computed against this clock only. Values
 * are opaque ticks for delta arithmetic only; they are NOT a timespec
 * for any platform wait API.
 * On success returns 0 and stores the reading in *out_ns. Failure returns
 * a negative -errno and leaves *out_ns untouched; a NULL out-slot fails
 * with -EFAULT. */
int mjs_clock_now(uint64_t *out_ns);

/* Resolution of the monotonic clock in nanoseconds (the smallest
 * non-zero tick difference the implementation can report; >= 1).
 * On success returns 0 and stores the resolution in *out_res_ns. Failure
 * returns a negative -errno and leaves *out_res_ns untouched; a NULL
 * out-slot fails with -EFAULT. */
int mjs_clock_resolution(uint64_t *out_res_ns);

/* --- s3-mutex --- */
/*
 * S3.1 native mutex (issue #57, spec §15). Same return-value contract as
 * above: 0 == success; negative == -errno; out-params UNTOUCHED on failure.
 *
 * Handle lifetime: mjs_mutex_init yields an owned opaque handle in *out;
 * mjs_mutex_destroy CONSUMES it (*m is NULLed on success). Any use of a
 * consumed or NULL handle is a deterministic -EINVAL. Destroying a LOCKED
 * mutex is a caller bug (POSIX-undefined); the caller must unlock first.
 *
 * try_lock result convention: 0 = acquired, -EBUSY = already locked. The
 * -EBUSY is a STATUS, not a failure — callers map it to their "busy"
 * boolean; every other negative value is a real error.
 *
 * Blocking (SYS-5): mjs_mutex_lock blocks the calling OS thread under
 * contention (futex/ulock wait inside pthread_mutex_lock); try_lock and
 * unlock never block; init/destroy may briefly wait on internal registry/
 * kernel mutexes only insofar as the allocator and pthread_mutex_* do.
 * Allocation (SYS-4): one fixed-size handle at init; NONE afterwards.
 * Task-aware: no — OS-thread granularity; a blocked caller parks its
 * whole OS thread (worker sleep/wake infrastructure per spec §14).
 */

/* Opaque native mutex handle (SYS-3). */
typedef struct mjs_mutex mjs_mutex;

/* Create a native mutex in the unlocked state. On success returns 0 and
 * stores the handle in *out; NULL out-slot is -EFAULT with *out untouched. */
int mjs_mutex_init(mjs_mutex **out);

/* Lock the mutex. Blocks (SYS-5) until acquired under contention. A NULL
 * handle is -EINVAL. Not recursive: re-locking from the owning thread is
 * undefined (POSIX EDEADLK surface), never relied upon. */
int mjs_mutex_lock(mjs_mutex *m);

/* Try to lock WITHOUT blocking. Returns 0 if acquired, -EBUSY if already
 * locked (status, see block comment), another negative -errno on error;
 * NULL handle is -EINVAL. */
int mjs_mutex_try_lock(mjs_mutex *m);

/* Unlock the mutex previously locked by the CALLING thread. A NULL handle
 * is -EINVAL; unlocking an unowned/unlocked mutex is POSIX-undefined and
 * not validated here. Non-blocking (SYS-5). */
int mjs_mutex_unlock(mjs_mutex *m);

/* Destroy the mutex and free its handle: *m is NULLed on success, so any
 * later use (including a second destroy) is a deterministic -EINVAL. On
 * failure the handle is NOT consumed. NULL or NULLed *m is -EINVAL. */
int mjs_mutex_destroy(mjs_mutex **m);


/* --- s3-condvar --- */
/*
 * S3.2 native condition variable (issue #58, spec §16). Same return-
 * value contract as above: 0 == success; negative == -errno; out-params
 * UNTOUCHED on failure.
 *
 * Handle lifetime: mjs_condvar_init yields an owned opaque handle in
 * *out; mjs_condvar_destroy CONSUMES it (*c is NULLed on success). Any
 * use of a consumed or NULL handle is a deterministic -EINVAL.
 * Destroying a condvar while threads still wait on it is a caller bug
 * (POSIX-undefined); the caller must join its waiters first.
 *
 * WAITING CONTRACT (spec §16 + sync/common.mojo spurious-wakeup model):
 *   - The caller MUST hold `m` (an mjs_mutex*) across wait/wait_until,
 *     and re-check its predicate after EVERY return — .ok may be a
 *     spurious wakeup by contract, never proof the condition held.
 *   - wait_until takes an ABSOLUTE deadline in nanoseconds against the
 *     SAME monotonic domain mjs_clock_now reports (NOT wall-clock).
 *     CLOCK-DOMAIN MAPPING (the known trap, issue #58 CAUTION):
 *       Linux: the condvar is initialized with
 *       pthread_condattr_setclock(CLOCK_MONOTONIC), so deadline_ns maps
 *       1:1 onto {sec = ns/1e9, nsec = ns%1e9} for pthread_cond_timedwait.
 *       macOS has NO pthread_condattr_setclock; the implementation uses
 *       pthread_cond_timedwait_relative_np with the RELATIVE remainder
 *       (deadline_ns - mjs_clock_now()) recomputed at every call entry —
 *       the same monotonic source, no mach timebase math duplicated.
 *   - wait_until result convention mirrors try_lock's -EBUSY status:
 *     0 = woken (predicate may STILL be false), -ETIMEDOUT = deadline
 *     expired (a STATUS, not a failure), any other negative = real error.
 *
 * Blocking (SYS-5): wait blocks the calling OS thread until woken or
 *   the deadline expires; signal/broadcast wake but never wait;
 *   init/destroy block only insofar as malloc/pthread_cond_* do.
 * Allocation (SYS-4): one fixed-size handle at init; NONE afterwards.
 * Task-aware: no — OS-thread granularity per spec §14.
 */

/* Opaque native condition variable handle (SYS-3). */
typedef struct mjs_condvar mjs_condvar;

/* Create a condvar. On success returns 0 and stores the handle in *out;
 * NULL out-slot is -EFAULT with *out untouched. Linux pins the internal
 * clock to CLOCK_MONOTONIC via condattr (see block comment); macOS uses
 * the default cond plus the relative-NP timedwait fallback. */
int mjs_condvar_init(mjs_condvar **out);

/* Block the calling OS thread until woken (spurious wakes permitted),
 * releasing `m` atomically on sleep and reacquiring it before return.
 * Caller MUST hold `m`; NULL handles are -EINVAL. */
int mjs_condvar_wait(mjs_condvar *c, mjs_mutex *m);

/* As wait, bounded by an ABSOLUTE monotonic deadline in ns (same domain
 * as mjs_clock_now; see block comment for the platform mapping).
 * Returns 0 if woken (spurious wakes possible: re-check the predicate),
 * -ETIMEDOUT once the deadline passes, another negative on error; NULL
 * handles are -EINVAL. A deadline already in the past returns -ETIMEDOUT
 * without blocking. */
int mjs_condvar_wait_until(mjs_condvar *c, mjs_mutex *m,
                           uint64_t deadline_ns);

/* Wake at most one thread currently blocked in wait/wait_until on `c`
 * (a no-op when none wait). Non-blocking (SYS-5); NULL handle -EINVAL. */
int mjs_condvar_signal(mjs_condvar *c);

/* Wake ALL threads currently blocked in wait/wait_until on `c`.
 * Non-blocking (SYS-5); NULL handle -EINVAL. */
int mjs_condvar_broadcast(mjs_condvar *c);

/* Destroy the condvar and free its handle: *c is NULLed on success, so
 * any later use (including a second destroy) is a deterministic
 * -EINVAL. On failure the handle is NOT consumed. NULL or NULLed *c is
 * -EINVAL. */
int mjs_condvar_destroy(mjs_condvar **c);


/* --- s3-atomic-wait --- */
/*
 * S3.3 atomic wait/wake on u32 words (issue #59, spec §18). Same
 * return-value contract as above: 0 == success; negative == -errno;
 * out-params UNTOUCHED on failure. wake_* return the number of waiters
 * woken (>= 0) or a negative -errno.
 *
 * Backend map (spec §18): Linux futex (FUTEX_WAIT_PRIVATE /
 * FUTEX_WAKE_PRIVATE) and — since #60 — a macOS fallback: an
 * address-keyed waiter table (256 hashed slots of NativeMutex +
 * NativeCondVar, composed from the exported mjs_mutex / mjs_condvar
 * layer, zero new symbols, no __ulock or private kernel interface).
 * Windows WaitOnAddress/WakeByAddress* remains a LATER issue — hosts
 * without a backend return EXACTLY -ENOSYS from every entry point, so
 * callers and the conformance suite can detect-and-exclude cleanly.
 *
 * Deadline: absolute monotonic nanoseconds, SAME epoch as mjs_clock_now.
 * NULL waits indefinitely. Time64 safety: implementations MUST derive
 * any kernel timeout from the remaining span against mjs_clock_now, so
 * no 32-bit time_t ever carries an absolute deadline value.
 *
 * Status mapping contract for wait:
 *   0           — woken by a wake_*, OR the word did not hold `expected`
 *                 when the kernel re-checked it (futex EAGAIN): both mean
 *                 "the caller must re-read the word", i.e. WaitStatus.ok.
 *                 Spurious .ok is permitted by the shared contract
 *                 (mojito_sys/sync/common.mojo); always re-check the
 *                 predicate and re-wait in a loop.
 *   -ETIMEDOUT  — the deadline expired first (WaitStatus.timed_out).
 *   other       — genuine error, negative -errno verbatim (-EFAULT for a
 *                 NULL address).
 */

/* Block the calling thread while *(uint32_t*)addr == expected, until a
 * wake_one/wake_all on addr, the deadline passing, or the word changing.
 * Returns 0 / -ETIMEDOUT / -errno per the mapping above; NULL addr is
 * -EFAULT with nothing read. Blocking (SYS-5): parks the calling OS
 * thread in the kernel waiter queue keyed by addr. Allocation: none
 * (SYS-4). Task-aware: no — OS-thread granularity (§14). */
int mjs_atomic_wait_on_u32(const uint32_t *addr, uint32_t expected,
                           const uint64_t *deadline_ns);

/* Wake ONE waiter blocked on addr (FIFO choice per the OS). Returns the
 * exact number woken (0 when none were waiting), or -errno; NULL addr is
 * -EFAULT. Non-blocking (SYS-5). Allocation: none (SYS-4). Task-aware:
 * no. */
int mjs_atomic_wake_one_u32(uint32_t *addr);

/* Wake ALL waiters currently blocked on addr. Same contract as
 * mjs_atomic_wake_one_u32 with the wake count unbounded. Non-blocking
 * (SYS-5). Allocation: none (SYS-4). Task-aware: no. */
int mjs_atomic_wake_all_u32(uint32_t *addr);


/* --- s3-event --- */
/*
 * S3.5 native event (issue #61, spec §17). Same return-value contract
 * as above: 0 == success; negative == -errno; out-params UNTOUCHED on
 * failure.
 *
 * WAKE SEMANTICS (NORMATIVE — spec §17 leaves breadth and stickiness
 * open; this ABI pins them):
 *   NativeEvent is an AUTO-RESET, BREADTH-ONE event. The event holds at
 *   most ONE pending token. mjs_event_signal stores a token (0 -> 1)
 *   and wakes at most one parked waiter; when no one waits the token
 *   STICKS so exactly one later wait completes without blocking.
 *   Signals issued while a token is already pending COALESCE: five
 *   signals with no waiter release exactly one future wait. A
 *   successful wait/wait_until CONSUMES the token; every other waiter
 *   keeps sleeping until the next signal. Fairness is not promised:
 *   a fresh arrival may consume the token ahead of an already-woken
 *   waiter that has not yet reacquired the internal mutex.
 *
 * Handle lifetime: identical to the s3-mutex / s3-condvar model —
 *   mjs_event_init yields an owned opaque handle in *out;
 *   mjs_event_destroy CONSUMES it (*e NULLed on success); any use of a
 *   consumed or NULL handle is a deterministic -EINVAL. Destroying an
 *   event while threads still wait on it is a caller bug (undefined);
 *   the caller must join its waiters first.
 *
 * IMPLEMENTATION COMPOSITION + FUTURE FAST PATH: this layer composes
 * the portable s3-mutex + s3-condvar primitives (one internal mutex,
 * one internal condvar pinned per the s3-condvar clock domain, one
 * int token). The atomic-wait layer has landed on main (#59/#60); the
 * uncontended fast path slots into exactly two places WITHOUT changing
 * this ABI:
 *   - wait/wait_until: an atomic acquire-load of the token short-
 *     circuits the mutex+condvar park when a token is already visible;
 *     the slow path becomes mjs_atomic_wait_u32(&e->token, 0, ...).
 *   - signal: store-release of the token plus
 *     mjs_atomic_wake_u32_one(&e->token) replaces the internal
 *     lock/cv_signal pair.
 *
 * Blocking (SYS-5): wait blocks the calling OS thread until a token is
 *   available or the deadline expires; signal wakes but never waits;
 *   init/destroy block only insofar as malloc and the composed
 *   pthread primitives do.
 * Allocation (SYS-4): one fixed-size handle at init; NONE afterwards.
 * Task-aware: no — OS-thread granularity per spec §14.
 */

/* Opaque native event handle (SYS-3). */
typedef struct mjs_event mjs_event;

/* Create an event in the no-token state. On success returns 0 and
 * stores the handle in *out; NULL out-slot is -EFAULT with *out
 * untouched. */
int mjs_event_init(mjs_event **out);

/* Block the calling OS thread until a token is available, then consume
 * it. A pre-stored token completes immediately. */
int mjs_event_wait(mjs_event *e);

/* As wait, bounded by an ABSOLUTE monotonic deadline in ns (same
 * domain as mjs_clock_now). Returns 0 once a token was consumed,
 * -ETIMEDOUT once the deadline passes (a STATUS like try_lock's
 * -EBUSY), another negative on error. A deadline already in the past
 * returns -ETIMEDOUT without blocking unless a token is pending.
 *
 * EXPIRY PARITY: a waiter that observes -ETIMEDOUT re-checks the token
 * under the internal lock before returning; a token visible at that
 * re-check is CONSUMED and 0 returned — wake beats timeout, exactly
 * like mjs_atomic_wait_on_u32. -ETIMEDOUT with a token still pending
 * afterwards is a contract violation. */
int mjs_event_wait_until(mjs_event *e, uint64_t deadline_ns);

/* Store one token (if none pending) and wake at most one waiter.
 * Coalesces while a token is already pending (see block comment).
 * Non-blocking (SYS-5); NULL handle -EINVAL. */
int mjs_event_signal(mjs_event *e);

/* Destroy the event and free its handle: *e is NULLed on success, so
 * any later use (including a second destroy) is a deterministic
 * -EINVAL. On failure the handle is NOT consumed. NULL or NULLed *e is
 * -EINVAL. */
int mjs_event_destroy(mjs_event **e);


/* --- s3-sem --- */
/*
 * S3.7 native counting semaphore (issue #106, spec §14/§17). Same
 * return-value contract as above: 0 == success; negative == -errno;
 * out-params UNTOUCHED on failure.
 *
 * PERMIT-ACCOUNTING SEMANTICS (NORMATIVE — spec §14 names permit
 * accounting among the sync primitives but leaves breadth open; this
 * ABI pins them): NativeSemaphore is a COUNTING semaphore holding a
 * NON-NEGATIVE permit count.
 *   - mjs_sem_post releases ONE permit: increments the count and wakes
 *     AT MOST ONE parked waiter. With nobody waiting the permit stays
 *     resident for a later wait, and permits ACCUMULATE — N posts with
 *     no waiter leave N permits, so exactly N later waits complete
 *     without blocking (a counting semaphore, NOT NativeEvent's
 *     coalescing signal).
 *   - mjs_sem_wait acquires ONE permit: it blocks while the count is
 *     zero, then decrements it. Each permit is consumed by EXACTLY ONE
 *     wait; permits are conserved across posts and waits.
 *   - mjs_sem_wait_until is the bounded form: absolute monotonic
 *     deadline in ns (same domain as mjs_clock_now; platform clock
 *     mapping lives in the composed mjs_condvar — Linux condattr
 *     CLOCK_MONOTONIC, macOS relative-NP remainder). EXPIRY PARITY:
 *     a waiter that observes -ETIMEDOUT re-checks the permit count
 *     before returning; a permit visible at that re-check is CONSUMED
 *     and 0 returned — wake beats timeout, exactly like
 *     mjs_event_wait_until and mjs_atomic_wait_on_u32.
 *   - mjs_sem_try_wait acquires WITHOUT blocking: 0 once a permit was
 *     taken, -EBUSY (a STATUS like try_lock's) when the count is zero,
 *     another negative on error.
 *   - The count NEVER goes negative: an acquisition that would take it
 *     below zero instead blocks (wait/wait_until) or returns -EBUSY
 *     (try_wait). Fairness is NOT promised: a fresh arrival may acquire
 *     a permit ahead of an already-woken waiter that has not yet
 *     reacquired the internal mutex.
 *
 * Handle lifetime: identical to the s3-mutex / s3-condvar / s3-event
 * model — mjs_sem_init yields an owned opaque handle in *out;
 * mjs_sem_destroy CONSUMES it (*s NULLed on success); any use of a
 * consumed or NULL handle is a deterministic -EINVAL. Destroying a
 * semaphore while threads still wait on it is a caller bug (undefined);
 * the caller must join its waiters first.
 *
 * IMPLEMENTATION COMPOSITION: this layer composes the portable s3.1 +
 * s3.2 primitives (one internal mjs_mutex, one internal mjs_condvar
 * pinned per the s3-condvar clock domain, one int count), exactly like
 * the s3-event and the AtomicWait-macOS-fallback precedent.
 *
 * Blocking (SYS-5): wait/wait_until block the calling OS thread until a
 *   permit is available or the deadline expires; post wakes but never
 *   waits; try_wait never blocks (returns -EBUSY on an empty
 *   semaphore); init/destroy block only insofar as malloc and the
 *   composed pthread primitives do.
 * Allocation (SYS-4): one fixed-size handle at init; NONE afterwards.
 * Task-aware: no — OS-thread granularity per spec §14.
 */

/* Opaque native counting-semaphore handle (SYS-3). */
typedef struct mjs_sem mjs_sem;

/* Create a semaphore with `initial` permits (>= 0). A count can never
 * be negative, so `initial` < 0 is -EINVAL. On success returns 0 and
 * stores the handle in *out; NULL out-slot is -EFAULT with *out
 * untouched. */
int mjs_sem_init(int initial, mjs_sem **out);

/* Release ONE permit: increments the count and wakes AT MOST ONE
 * waiter. Permits accumulate (counting semantics). Non-blocking
 * (SYS-5); NULL handle -EINVAL. */
int mjs_sem_post(mjs_sem *s);

/* Acquire ONE permit, blocking (SYS-5) while the count is zero, then
 * decrementing it. NULL handle is -EINVAL. */
int mjs_sem_wait(mjs_sem *s);

/* As wait, bounded by an ABSOLUTE monotonic deadline in ns (same domain
 * as mjs_clock_now). Returns 0 once a permit was consumed, -ETIMEDOUT
 * once the deadline passes (a STATUS like try_lock's -EBUSY), another
 * negative on error. EXPIRY PARITY: a permit visible at the post-expiry
 * re-check is CONSUMED and 0 returned — never -ETIMEDOUT with a permit
 * left pending. A deadline already in the past returns -ETIMEDOUT
 * without blocking unless a permit is already available (.ok then).
 * NULL handle is -EINVAL. */
int mjs_sem_wait_until(mjs_sem *s, uint64_t deadline_ns);

/* Acquire ONE permit WITHOUT blocking. Returns 0 once a permit was
 * taken, -EBUSY (a STATUS, not a failure) when the count is zero,
 * another negative on error. Non-blocking (SYS-5); NULL handle -EINVAL.
 * A permit is always taken whenever the count is positive, so this
 * never blocks and never moves the count below zero. */
int mjs_sem_try_wait(mjs_sem *s);

/* Destroy the semaphore and free its handle: *s is NULLed on success,
 * so any later use (including a second destroy) is a deterministic
 * -EINVAL. On failure the handle is NOT consumed. NULL or NULLed *s is
 * -EINVAL. */
int mjs_sem_destroy(mjs_sem **s);


/* --- s5-ctx --- */
/*
 * S5.1 native contexts (issue #64, spec §20.2): stackful cooperative
 * contexts switched at the callee-saved register level. Same additive-
 * only regime as every block above; MOJITO_SYS_ABI_VERSION stays 1.
 *
 * Return-value contract: ms_context_init is the ONLY errno-style entry
 * point in this block (0 == success; negative == -errno). It takes no
 * out-parameters; on failure the caller's context storage is left
 * untouched (uninitialized). capture/switch/destroy are void and cannot
 * fail.
 *
 * Context storage: a context is CALLER-OWNED storage of
 * ms_context_size() bytes with ms_context_alignment() alignment (a
 * struct member, heap block, or stack slot all work). The library never
 * allocates, moves, or frees it.
 *
 * Frozen v2 save-area layout (amendment #19 — panel-proven after silent
 * numeric-frame corruption without d8-d15 saves): Backend-pinned via
 * _Static_asserts in ms_context.c — INTERNAL to the library,
 * informational here, not a consumer promise; kept for the S5.2 ELF
 * port. Register-level backends address these slots directly. What is
 * pinned here is the frozen v2 PREFIX; the #66 lifecycle tail appends a
 * 4-slot v3 TAIL after it (168-byte prefix + 4 x uint64_t = 200 bytes
 * total; see the Lifecycle paragraph below):
 *   v2 prefix — 168 bytes:
 *     regs[12] @   0.. 95  x19..x28, fp(x29) @80, lr(x30) @88
 *     fps[8]   @  96..159  low 64 bits of v8..v15 (d8..d15)
 *     sp       @ 160       saved stack pointer
 * x86-64 restore-time trap #0x6b: besides the shared misuse classes, the
 * x86-64 SysV backend ALSO traps (int3, %eax marker #0x6b) if a context's
 * saved sp slot is not 16-byte aligned when a switch is about to restore
 * it — SysV requires rsp ≡ 0 (mod 16) at callee entry, so a misaligned
 * slot is corruption caught BEFORE the bad stack is committed (mirrors
 * the make-time #0x64 class; the AArch64 backend's restore path has no
 * alignment trap because its initial-sp invariant is enforced at make).
 *
 * BACKEND SUPPORT STATUS (x86-64 SysV, read loudly): NOT
 * NativeContext-supported yet. Native support requires FULL 128-bit
 * preservation of the entire SysV callee-saved SIMD set — xmm14/xmm15 are
 * still omitted because the frozen fps bank holds only 8 low-64 slots
 * (SysV diverges from AAPCS64 here) — plus a passing in-repo conformance
 * suite (spec §38.6). Until then the x86-64 SysV backend is a
 * cross-assembled structural mirror only (assembles cleanly on this host;
 * behavioral rows are RED-EXCLUDED UNSUPPORTED-PLATFORM until a
 * Linux/x86_64 runner exists — tests/s5/x86).
 * Synthetic entry: ms_context_init prepares ctx so its first resume
 * lands on an internal trampoline that calls entry(userdata) with a
 * 16-byte-aligned sp (AAPCS64 at function entry). userdata is passed
 * through UNMODIFIED. When entry returns, the trampoline's completion
 * stage runs — the registered finish hook (see
 * ms_context_set_finish_hook), then the record is marked FINISHED —
 * and control switches PERMANENTLY back to the most recent switcher
 * of ctx; resuming a finished or destroyed context traps loudly
 * (deliberate hard trap, not -errno).
 *
 * Lifecycle (#66): every context RECORD owns its own state machine —
 * DEAD (zeroed storage) / EMPTY (armed by init or capture) / RUNNING
 * (currently executing) / SUSPENDED (mid-life, resumable) / FINISHED
 * (entry returned) — plus its own return target. There is NO global
 * bookkeeping: any number of live contexts may exist simultaneously
 * (the spike-era 64-context return-to-table limit is gone).
 *
 * Thread-safety (#66): PER-CONTEXT exclusivity. The caller serializes
 * concurrent operations on the SAME context (one OS thread at a time
 * per context); distinct contexts are thread-INDEPENDENT — no shared
 * mutable state remains in the library — so they may be created,
 * switched, and finished concurrently on different threads without
 * locking. Resuming a context another thread currently holds RUNNING
 * traps loudly rather than corrupting. NULL context/entry arguments
 * are caller bugs where the signature cannot report them (void entry
 * points).
 *
 * Blocking (SYS-5): all of these are non-blocking. Allocation (SYS-4):
 * none.
 */
typedef struct ms_context ms_context;
typedef void (*ms_context_entry)(void *);
/* Completion hook shape (additive, #66); see ms_context_set_finish_hook. */
typedef void (*ms_context_finish_fn)(void *userdata);

/* Sideband geometry of the caller-owned save area (see layout above).
 * Compile-time constants surfaced as functions so consumers can bind
 * them without knowing the backend. Since #66 the record carries a
 * per-context lifecycle tail after the frozen v2 prefix: size == 200,
 * alignment == 8 (v2 consumers that sized storage via this getter are
 * unaffected; nothing reads or writes the tail beyond its own record). */


/* Register ctx's completion hook (additive, #66). See the declaration
 * near the end of this s5-ctx block for the full contract; this forward
 * declaration keeps consumers compiling when they include the header
 * before the block's tail. */
void ms_context_set_finish_hook(
    ms_context *ctx,
    ms_context_finish_fn hook,
    void *userdata
);

size_t ms_context_size(void);
size_t ms_context_alignment(void);

/* Prepare ctx (caller-owned storage of ms_context_size() bytes,
 * ms_context_alignment()-aligned) to resume at entry(userdata) on top of
 * the stack region [stack_low, stack_low + stack_size). stack_low points
 * at the LOW end; the initial sp is stack_low + stack_size, which MUST
 * be 16-byte aligned (AAPCS64): stack_size must be a nonzero multiple of
 * 16. Returns 0 on success; -EINVAL for NULL ctx/stack_low/entry, a
 * zero / non-16-multiple stack_size, or a stack_low that is itself not
 * 16-byte aligned, with ctx untouched. The region is
 * NOT validated or owned here (mjs_stack_alloc is the usual provider).
 * Only this function returns int in the s5-ctx block. */
int ms_context_init(
    ms_context *ctx,
    void *stack_low,
    size_t stack_size,
    ms_context_entry entry,
    void *userdata
);

/* Save the CURRENT execution state into ctx: a later
 * ms_context_switch(to = ctx) resumes just after this call. Capturing a
 * context that is already live (initialized/captured but not finished)
 * overwrites its saved state. Capture also REVIVES a FINISHED record:
 * the self-switch re-arm is unconditional, so after capture(ctx) a
 * completed context reads SUSPENDED again and resumes normally from the
 * freshly saved snapshot (#66 lifecycle). */
void ms_context_capture(ms_context *ctx);

/* Save the current state into `from` and resume `to`. Switching is
 * allocation-free (spec §20.1). Resuming a finished, destroyed, or
 * currently-RUNNING context traps loudly (#66 per-record state
 * machine). */
void ms_context_switch(
    ms_context *from,
    ms_context *to
);

/* Render ctx unusable: an exited, or destroyed-and-not-subsequently-
 * captured, context traps loudly on resume. Capture REVIVES destroyed
 * storage — the dead-context guard reads saved state only AFTER the
 * save — so capture(ctx) after destroy makes ctx live again. Does not
 * free the caller's storage and does not touch any stack memory.
 * Destroying a currently-running context from inside itself is a
 * caller bug. */
void ms_context_destroy(ms_context *ctx);
/* --- s6-socket --- */
/*
 * S6.2 non-blocking sockets (issue #74, spec §26). Same return-value
 * contract as above: 0 == success; negative == -errno; out-params UNTOUCHED
 * on failure (a NULL out-slot fails with -EFAULT before anything else).
 *
 * Blocking (SYS-5): every entry point here performs ONE readiness-agnostic
 * syscall and never waits for socket readiness. The exceptions:
 *   - mjs_socket_connect on a BLOCKING socket blocks until the handshake
 *     completes or fails (on a non-blocking socket it reports -EINPROGRESS
 *     immediately — a STATUS, like try_lock's -EBUSY);
 *   - close(2)/shutdown(2) may block inside the kernel only insofar as
 *     SO_LINGER demands.
 *
 * EINTR/EAGAIN policy (spec §38.11): accept/recv/send NEVER retry
 * internally and NEVER wait — -EINTR and -EAGAIN surface to the caller so
 * a reactor decides (the Mojo layer maps them to Interrupted/WouldBlock).
 * The one-shot entry points below cannot be interrupted mid-operation;
 * their errno passes through unchanged. On darwin, close(2) reports EINTR
 * only AFTER releasing the descriptor: callers must treat ANY close()
 * return as final and never retry (the Mojo wrapper closes exactly once).
 *
 * Descriptor ownership: fds minted by mjs_socket_socket / returned through
 * mjs_socket_accept are owned by the caller; exactly ONE successful
 * mjs_socket_close releases each. Closing an already-closed fd is caller
 * error (-EBADF surfaces raw).
 *
 * Determinism note: an accepted socket INHERITS its listener's O_NONBLOCK
 * state explicitly (Linux inherits through accept(2), darwin does not) so
 * downstream WouldBlock behavior is identical across hosts.
 */

/* Platform-neutral address families / socket types. Values follow POSIX
 * where hosts agree; AF_INET6 differs numerically (30 darwin / 10 Linux),
 * so it travels under this neutral spelling. */
#define MJS_SOCK_STREAM 1
#define MJS_SOCK_DGRAM  2
#define MJS_SOCK_INET   2
#define MJS_SOCK_UNIX   1
#if defined(__APPLE__)
#define MJS_SOCK_INET6 30
#else
#define MJS_SOCK_INET6 10
#endif

/* Shutdown halves (mjs_socket_shutdown `how`). */
#define MJS_SHUT_READ  0
#define MJS_SHUT_WRITE 1
#define MJS_SHUT_BOTH  2

/* Platform-neutral socket address (SYS-3: fixed layout, no OS sockaddr
 * struct leaks across the firewall). Port is HOST byte order; conversion
 * happens inside the C layer. octets holds IPv4 bytes in [0..3] (rest
 * zero) or all 16 IPv6 bytes; path holds a NUL-terminated unix path of at
 * most 103 chars (+ NUL). Layout is byte-stable: int32 @0, uint16 @4,
 * pad @6, uint32 flowinfo @8, uint32 scope_id @12, octets @16..31,
 * path @32..135; sizeof == 136 on LP64 hosts. */
typedef struct mjs_sockaddr {
    int32_t family;
    uint16_t port;      /* host byte order */
    uint16_t _pad0;
    uint32_t flowinfo;  /* IPv6 flow label; ignored otherwise */
    uint32_t scope_id;  /* IPv6 scope id; ignored otherwise */
    unsigned char octets[16];
    char path[104];
} mjs_sockaddr;

/* Create a socket of `family` (MJS_SOCK_*) and `type` (MJS_SOCK_*). An
 * unknown family/type is -EINVAL with *out_fd untouched. On success stores
 * the owned descriptor in *out_fd. Non-blocking (SYS-5); no allocation. */
int mjs_socket_socket(int family, int type, int *out_fd);

/* Set/clear O_NONBLOCK on `fd`. Unknown flags state is never assumed: the
 * current F_GETFL flags are read-modify-written. Non-blocking (SYS-5). */
int mjs_socket_set_nonblocking(int fd, int enabled);

/* Bind `fd` to the platform-neutral address. The port travels in HOST byte
 * order and is converted inside. A NULL addr is -EFAULT; an unconvertible
 * family is -EINVAL; a too-long unix path (>= its sun_path) is
 * -ENAMETOOLONG. Non-blocking (SYS-5). */
int mjs_socket_bind(int fd, const mjs_sockaddr *addr);

/* Mark `fd` as passive with `backlog`. backlog < 0 is -EINVAL. On success
 * the socket may yield connections via mjs_socket_accept. Non-blocking
 * (SYS-5). */
int mjs_socket_listen(int fd, int backlog);

/* Initiate a connection to `addr`. On a BLOCKING socket this blocks until
 * the handshake settles (SYS-5). On a non-blocking socket it returns
 * -EINPROGRESS as a documented STATUS: the connection attempt continues;
 * callers await writability to learn the outcome. A NULL addr is -EFAULT.
 * Connection failures on a blocking socket surface as their errno
 * (-ECONNREFUSED, -ECONNRESET, ...). */
int mjs_socket_connect(int fd, const mjs_sockaddr *addr);

/* Extract ONE pending connection from listener `fd`. NEVER blocks or waits
 * (SYS-5): without a queued peer it returns -EAGAIN immediately; -EINTR
 * surfaces raw for the caller to retry. On success stores the owned client
 * descriptor in *out_client and, when out_peer is non-NULL, the peer
 * address in *out_peer (out_client/out_peer untouched on failure). The
 * accepted socket inherits the listener's O_NONBLOCK state deterministically. */
int mjs_socket_accept(int fd, int *out_client, mjs_sockaddr *out_peer);

/* ONE recv(2) attempt on `fd` into buf[0..len); NEVER waits (SYS-5).
 * Success: 0 with *out_n = bytes received; *out_n == 0 means orderly EOF.
 * len == 0 is -EINVAL. Partial receives are normal and reported as-is.
 * -EAGAIN/-EWOULDBLOCK when no data is ready; -EINTR surfaces raw; any
 * other errno passes through unchanged (-ECONNRESET after peer reset).
 * buf NULL or out_n NULL with len > 0 is -EFAULT. */
int mjs_socket_recv(int fd, unsigned char *buf, size_t len, size_t *out_n);

/* ONE send(2) attempt on `fd` from buf[0..len); NEVER waits (SYS-5).
 * Success: 0 with *out_n = bytes ACCEPTED by the kernel (which may be a
 * PARTIAL prefix of len — callers must loop over the remainder). len == 0
 * is -EINVAL. -EAGAIN/-EWOULDBLOCK when the send buffer is full; -EINTR
 * surfaces raw; other errnos pass through unchanged. buf NULL or out_n
 * NULL with len > 0 is -EFAULT. */
int mjs_socket_send(int fd, const unsigned char *buf, size_t len,
                    size_t *out_n);

/* Shut down one/both transfer halves: how is MJS_SHUT_READ/WRITE/BOTH.
 * Anything else is -EINVAL. Non-blocking (SYS-5). */
int mjs_socket_shutdown(int fd, int how);

/* Close `fd`, releasing the descriptor exactly once per call. ANY return
 * value is FINAL: never retry close (darwin reports EINTR only after the
 * descriptor is already released). fd < 0 is -EBADF. Blocking (SYS-5):
 * only inside the kernel per SO_LINGER. */
int mjs_socket_close(int fd);

/* ---- sockaddr helpers ---------------------------------------------------
 * Address conversion between dotted-quad text and the neutral layout.
 * Same contract: 0 / negative -errno; out-params untouched on failure.
 * Non-blocking (SYS-5); no allocation beyond libc's fixed buffers. */

/* Parse dotted-quad IPv4 text into *out with host-order `port`.
 * Malformed text (inet_pton != 1), port outside [0, 65535], or a NULL
 * argument is -EFAULT/-EINVAL with *out untouched. */
int mjs_sockaddr_ipv4(const char *dotted, int port, mjs_sockaddr *out);

/* Format the IPv4 address in *addr as dotted-quad text into out_buf
 * (NUL-terminated). Requires addr->family == MJS_SOCK_INET (-EINVAL
 * otherwise); cap < needed length (incl. NUL) is -EINVAL. Stores the
 * string length (no NUL) in *out_len on success. */
int mjs_sockaddr_format4(const mjs_sockaddr *addr, char *out_buf,
                         size_t cap, size_t *out_len);

/* --- s6-poller --- */
/*
 * S6.3 readiness poller (issue #75, spec §27.1/§29/§31). Same
 * return-value contract as above: 0 == success; negative == -errno;
 * out-params UNTOUCHED on failure.
 *
 * BACKEND GUARD: this block is kqueue-backed. Hosts without kqueue
 * return EXACTLY -ENOSYS from every entry point (detect-and-exclude,
 * mirroring the s3-atomic-wait contract); *out slots stay untouched.
 *
 * Interests and events travel in one neutral bitmask:
 *   MJS_POLL_READABLE / MJS_POLL_WRITABLE are REGISTERABLE interests;
 *   MJS_POLL_EOF / MJS_POLL_ERROR are EVENT-ONLY flags, never accepted
 *   as interests (-EINVAL when passed to register/modify).
 *
 * Edge doctrine (spec §29): registrations use edge/clear semantics — a
 * readiness transition is reported ONCE per transition; callers drain
 * (read/write until WouldBlock) or re-arm by state change. The platform
 * spelling (EV_CLEAR) stays BELOW this platform-neutral wrapper.
 *
 * Duplicate registration / modify: re-registering a live descriptor is
 * an UPSERT — the LAST register/modify wins for BOTH interests and
 * token (verified against darwin kevent re-add updates udata).
 * unregister of a not-registered descriptor succeeds (0), so
 * unregister/reuse races degrade to no-ops, never errors.
 *
 * Token: completely opaque upstream identity (spec §31); preserved
 * EXACTLY through every delivered event (64-bit value travels verbatim
 * in kevent.udata). Generation checking belongs to mojito-async.
 *
 * Wake: mjs_poller_wake makes at most ONE blocked wait return 0 events.
 * A wake with NO waiter STICKS: exactly one later wait returns promptly
 * with zero events (EVFILT_USER + EV_CLEAR; signals coalesce like the
 * s3-event token). wake NEVER blocks (SYS-5); only wait blocks its
 * calling OS thread, bounded by timeout_ns.
 *
 * wait status mapping:
 *   0        — success; *out_n carries 0..cap events (0 == timeout);
 *              wake deliveries NEVER occupy an out slot but DO end the
 *              wait early (possibly with *out_n == 0);
 *   -EINTR   — interrupted before any event: raw -EINTR, caller retries
 *              (§38.11 interrupt/retry doctrine);
 *   other    — genuine error, negative -errno verbatim.
 * Events beyond cap are DROPPED (callers loop). cap == 0 is -EINVAL.
 *
 * EOF/error delivery: EVFILT_READ/WRITE reporting EV_EOF sets
 * MJS_POLL_EOF; a NEGATIVE data field under EV_EOF (e.g. ECONNRESET on
 * a reset socket) additionally sets MJS_POLL_ERROR (spec §38.7
 * error/hangup + kqueue-specific cases).
 *
 * Handle lifetime: create yields an owned opaque handle in *out; close
 * CONSUMES it (**p NULLed on success). Any use of a consumed or NULL
 * handle is a deterministic -EINVAL. The poller is NON-OWNING: closing
 * it never closes registered descriptors (spec §25 borrow rule).
 * Closing a REGISTERED descriptor silently retires its knotes (kqueue
 * close-out); no event fires, later waits simply report nothing.
 *
 * Blocking (SYS-5): ONLY mjs_poller_wait may block (bounded by
 * timeout_ns when non-NULL); register/modify/unregister/wake/close
 * never block.
 * Allocation (SYS-4): one fixed-size handle at create; NONE afterwards
 * (the kevent changelist lives on the C stack).
 * Task-aware: no — OS-thread granularity per spec §14.
 */

/* Registerable interests (OR-ed). */
#define MJS_POLL_READABLE 0x1u
#define MJS_POLL_WRITABLE 0x2u

/* Event-only flags (OR-ed into mjs_poll_event.events). */
#define MJS_POLL_EOF      0x4u
#define MJS_POLL_ERROR    0x8u

/* One delivered readiness event. Fixed scalar layout — token@0, fd@8,
 * events@12; 16 bytes, 8-byte alignment — so FFI consumers decode with
 * SCALAR loads only (no aggregate reads in extern-reaching frames). */
typedef struct mjs_poll_event {
    uint64_t token;  /* opaque registration token, preserved exactly */
    int32_t fd;      /* ready descriptor */
    uint32_t events; /* MJS_POLL_* flags for THIS event */
} mjs_poll_event;

/* Opaque native poller handle (SYS-3). */
typedef struct mjs_poller mjs_poller;

/* Create a readiness poller (kqueue-backed; internal EVFILT_USER wake
 * source pre-registered). On success returns 0 and stores the handle in
 * *out; NULL out-slot is -EFAULT with *out untouched. Without a kqueue
 * backend returns exactly -ENOSYS with *out untouched. */
int mjs_poller_create(mjs_poller **out);

/* Register `fd` for `interests` with opaque `token`. Re-registration of
 * a live fd UPSERTS (last interests+token win); event-only flag bits in
 * `interests` are -EINVAL. Non-blocking (SYS-5). */
int mjs_poller_register(mjs_poller *p, int fd, uint32_t interests,
                        uint64_t token);

/* Modify an existing registration atomically-from-the-caller's-view:
 * same upsert contract as register; filters dropped from `interests`
 * are removed (a not-present filter deletes as a no-op). */
int mjs_poller_modify(mjs_poller *p, int fd, uint32_t interests,
                      uint64_t token);

/* Remove ALL filters for `fd`. Not-registered descriptors succeed (0);
 * only real kernel errors (-errno) fail. Non-blocking (SYS-5). */
int mjs_poller_unregister(mjs_poller *p, int fd);

/* Wait for readiness on ANY registered descriptor into `events` (up to
 * cap entries; cap == 0 is -EINVAL). timeout_ns: NULL = block until an
 * event/wake; *timeout_ns == 0 = non-blocking poll; otherwise relative
 * nanoseconds. Timeout expiry is SUCCESS with *out_n == 0. Returns
 * -EINTR raw for retry; out_n untouched on failure. Blocking (SYS-5):
 * parks the calling OS thread per the timeout contract ONLY. */
int mjs_poller_wait(mjs_poller *p, mjs_poll_event *events, unsigned cap,
                    const uint64_t *timeout_ns, unsigned *out_n);

/* Make at most ONE blocked wait return promptly with zero events. A
 * wake with no waiter sticks for exactly one later wait. Coalescing:
 * N wakes while nobody waits still release exactly ONE wait. Never
 * blocks (SYS-5). Allocation: none. */
int mjs_poller_wake(mjs_poller *p);

/* Destroy the poller and free its handle: *p is NULLed on success, so
 * any later use (including a second destroy) is a deterministic
 * -EINVAL. Registered DESCRIPTORS are NOT closed. On failure the
 * handle is NOT consumed. NULL or NULLed *p is -EINVAL. */
int mjs_poller_close(mjs_poller **p);

/* --- s6-epoll --- */
/*
 * S6.4 epoll lifecycle (issue #76, spec §27.1/§28/§31/§38.7).
 * Same return-value contract as the s6-poller block: 0 == success;
 * negative == -errno; out-params UNTOUCHED on failure.
 *
 * BACKEND GUARD: epoll is LINUX-ONLY. Hosts without epoll (darwin, BSD,
 * Windows) return EXACTLY -ENOSYS from every entry point
 * (detect-and-exclude, mirroring the s6-poller kqueue contract) so the
 * packaged dylib builds everywhere; the Mojo EpollPoller raises a decoded
 * unsupported-platform error.
 *
 * Interests and events ship in one neutral bitmask:
 *   MJS_POLL_READABLE / MJS_POLL_WRITABLE are REGISTERABLE interests;
 *   MJS_POLL_EOF / MJS_POLL_ERROR are EVENT-ONLY flags, never accepted
 *   as interests (-EINVAL when passed to register/modify).
 *
 * Trigger doctrine (spec §29 kept below the platform-neutral wrapper;
 * §38.7 epoll-specific): this backend exposes LEVEL-triggered semantics
 * by default — a ready fd is reported on every wait until the caller
 * drains it. EPOLLET (edge) and EPOLLONESHOT are deliberately NOT
 * exposed through the frozen ABI (documented exclusions; a reactor that
 * needs edge selects it inside mojito-async, not here).
 *
 * Duplicate registration / modify: re-registering a live descriptor is an
 * UPSERT via EPOLL_CTL_MOD — the LAST register/modify wins for BOTH
 * interests and token. unregister of a not-registered descriptor
 * succeeds (0); only real kernel errors (-errno) fail, so unregister/
 * reuse races degrade to no-ops, never errors.
 *
 * Token: completely opaque identity (spec §31); preserved EXACTLY through
 * every delivered event (64-bit value travels verbatim in
 * epoll_event.data.u64).
 *
 * Wake: mjs_epoll_wake writes one 64-bit value on the internal eventfd
 * (create pre-registers it for EPOLLIN). A wake makes at most ONE blocked
 * wait return promptly; a wake with NO waiter STICKS so exactly one later
 * wait returns promptly. N wakes with no waiter still release exactly ONE
 * wait (the drained counter condenses to a single release). wake NEVER
 * blocks (SYS-5); only wait parks its calling OS thread, bounded by
 * timeout_ns.
 *
 * wait status mapping:
 *   0        — success; *out_n carries 0..cap events (0 == timeout);
 *              wake deliveries NEVER occupy an out slot but DO end the
 *              wait early (possibly with *out_n == 0);
 *   -EINTR   — interrupted before any event: raw -EINTR, caller retries
 *               (§38.11 interrupt/retry doctrine);
 *   other    — genuine error, negative -errno verbatim.
 * Events beyond cap are DROPPED (callers loop). cap == 0 is -EINVAL.
 *
 * EOF/error delivery (§38.7 epoll-specific): EPOLLRDHUP or EPOLLHUP sets
 * MJS_POLL_EOF; EPOLLERR sets MJS_POLL_ERROR; readiness bits OR in when
 * present.
 *
 * Handle lifetime: create yields an owned opaque handle in *out; close
 * CONSUMES it (**p NULLed on success). Any use of a consumed or NULL
 * handle is a deterministic -EINVAL. The poller is NON-OWNING: closing
 * it never closes registered descriptors (spec §25 borrow rule). Closing
 * a REGISTERED descriptor silently retires its epoll entry (Linux drops
 * it on close); no event fires, later waits report nothing.
 *
 * Blocking (SYS-5): ONLY mjs_epoll_wait may block (bounded by timeout_ns
 * when non-NULL); register/modify/unregister/wake/close never block.
 * Allocation (SYS-4): one fixed-size handle at create; NONE afterwards
 * (the epoll_event batch lives on this file's stack).
 * Task-aware: no — OS-thread granularity per spec §14.
 */
/* Opaque epoll poller handle (SYS-3). */
typedef struct mjs_epoller mjs_epoller;

/* Create a readiness poller (epoll-backed; internal eventfd wake source
 * pre-registered). On success returns 0 and stores the handle in *out;
 * NULL out-slot is -EFAULT with *out untouched. Without an epoll backend
 * returns exactly -ENOSYS with *out untouched. */
int mjs_epoll_create(mjs_epoller **out);

/* Register `fd` for `interests` with opaque `token`. Re-registration of a
 * live fd UPSERTS (last interests+token win); event-only flag bits in
 * `interests` are -EINVAL. Non-blocking (SYS-5). */
int mjs_epoll_register(mjs_epoller *p, int fd, uint32_t interests,
                       uint64_t token);

/* Modify an existing registration atomically-from-the-caller's-view:
 * same upsert contract as register; filters dropped from `interests` are
 * removed. */
int mjs_epoll_modify(mjs_epoller *p, int fd, uint32_t interests,
                     uint64_t token);

/* Remove the registration for `fd`. Not-registered descriptors succeed
 * (0); only real kernel errors (-errno) fail. Non-blocking (SYS-5). */
int mjs_epoll_unregister(mjs_epoller *p, int fd);

/* Wait for readiness on ANY registered descriptor into `events` (up to
 * cap entries; cap == 0 is -EINVAL). timeout_ns: NULL = block until an
 * event/wake; *timeout_ns == 0 = non-blocking poll; otherwise relative
 * nanoseconds. Timeout expiry is SUCCESS with *out_n == 0. Returns
 * -EINTR raw for retry; out_n untouched on failure. Blocking (SYS-5):
 * parks the calling OS thread per the timeout contract ONLY. */
int mjs_epoll_wait(mjs_epoller *p, mjs_poll_event *events, unsigned cap,
                   const uint64_t *timeout_ns, unsigned *out_n);

/* Make at most ONE blocked wait return promptly with zero events. A wake
 * with no waiter sticks for exactly one later wait. Coalescing: N wakes
 * while nobody waits still release exactly ONE wait. Never blocks
 * (SYS-5). Allocation: none. */
int mjs_epoll_wake(mjs_epoller *p);

/* Destroy the poller and free its handle: *p is NULLed on success, so
 * any later use (including a second destroy) is a deterministic
 * -EINVAL. Registered DESCRIPTORS are NOT closed. On failure the handle
 * is NOT consumed. NULL or NULLed *p is -EINVAL. */
int mjs_epoll_close(mjs_epoller **p);

/* --- s6-ioring --- */
/*
 * S6.6 EXPERIMENTAL io_uring readiness backend (issue #78, spec §28/§30/
 * §27.2/§31). Same return-value contract as above: 0 == success; negative
 * == -errno; out-params UNTOUCHED on failure.
 *
 * EXPERIMENTAL / CAPABILITY FLAGGED (spec §28): io_uring MUST remain behind
 * a capability/feature flag until operation coverage, cancellation
 * semantics, kernel-version behavior and benchmarks are understood. A ring
 * is ONLY instantiated when the host kernel supports io_uring AND the
 * explicit environment flag MOJITO_IO_URING=1 is set; otherwise
 * mjs_iouring_create returns EXACTLY -ENOSYS. mjs_iouring_available() is
 * the authoritative capability predicate; mjs_iouring_probe() tests host
 * kernel support alone.
 *
 * BACKEND GUARD: this block is io_uring-backed on Linux only. Hosts without
 * io_uring (e.g. Darwin) return EXACTLY -ENOSYS from every entry point that
 * requires a live ring (detect-and-exclude, mirroring the s6-poller kqueue
 * block); mjs_iouring_probe/available return 0 there.
 *
 * The interests/event bitmask and the mjs_poll_event wire format are shared
 * with the s6-poller block (same MJS_POLL_* flags, same 16-byte layout), so
 * the §27.1 ReadinessPoller surface is drop-in across backends.
 *
 * Readiness semantics: one-shot edge polls (IORING_OP_POLL_ADD) re-armed
 * after each delivery (§29 edge doctrine below the wrapper). register/
 * modify UPSERT (last interests+token win); unregister of a not-registered
 * descriptor is a no-op (0). Tokens ride through poll user_data and are
 * preserved EXACTLY in delivered events (§31). EOF/error map from POLLHUP
 * -> MJS_POLL_EOF and POLLERR -> MJS_POLL_ERROR.
 *
 * Wake: mjs_iouring_wake makes at most ONE blocked wait return 0 events. A
 * wake with no waiter STICKS (the eventfd byte stays pending for exactly
 * one later wait). wake NEVER blocks (SYS-5); only mjs_iouring_wait parks
 * its calling OS thread, bounded by timeout_ns.
 *
 * wait status mapping:
 *   0        — success; *out_n carries 0..cap events (0 == timeout);
 *              wake deliveries NEVER occupy an out slot but DO end the
 *              wait early;
 *   -EINTR   — interrupted before any event: raw -EINTR, caller retries
 *              (§38.11); timeout expiry is success-with-zero;
 *   other    — genuine error, negative -errno verbatim.
 * Events beyond cap are DROPPED (callers loop). cap == 0 is -EINVAL.
 *
 * Handle lifetime: create yields an owned opaque handle in *out; close
 * CONSUMES it (**p NULLed on success). Any use of a consumed or NULL handle
 * is a deterministic -EINVAL. The backend is NON-OWNING: closing it never
 * closes registered descriptors (§25 borrow rule). Closing a REGISTERED
 * descriptor silently retires its poll (§31 close-while-registered).
 *
 * Blocking (SYS-5): ONLY mjs_iouring_wait may block (bounded by timeout_ns
 * when non-NULL); register/modify/unregister/wake/close/probe never block.
 * Allocation (SYS-4): one ring handle + its fd/token tables at create; a
 * fixed number of SQEs and ring pages afterwards.
 * Task-aware: no — OS-thread granularity per spec §14.
 */

/* Opaque native io_uring backend handle (SYS-3). */
typedef struct mjs_uring mjs_uring;

/* Pure predicate: does THIS KERNEL support io_uring? Returns 1 if a tiny
 * setup probe succeeds, 0 otherwise. Never blocks. */
int mjs_iouring_probe(void);

/* Full capability predicate (spec §28): host io_uring support AND
 * MOJITO_IO_URING=1 set. Returns 1 iff a ring can be created. Never blocks. */
int mjs_iouring_available(void);

/* Create an io_uring-backed readiness ring. On success returns 0 and stores
 * the handle in *out; NULL out-slot is -EFAULT with *out untouched. Without
 * host support or the capability flag returns EXACTLY -ENOSYS with *out
 * untouched. */
int mjs_iouring_create(mjs_uring **out);

/* Register `fd` for `interests` with opaque `token`. Re-registration of a
 * live fd UPSERTS (last interests+token win); event-only flag bits in
 * `interests` are -EINVAL. Non-blocking (SYS-5). */
int mjs_iouring_register(mjs_uring *p, int fd, uint32_t interests,
                         uint64_t token);

/* Modify an existing registration (same upsert contract as register).
 * Non-blocking (SYS-5). */
int mjs_iouring_modify(mjs_uring *p, int fd, uint32_t interests,
                       uint64_t token);

/* Remove the registration for `fd`. Not-registered descriptors succeed (0);
 * only real kernel errors (-errno) fail. Non-blocking (SYS-5). */
int mjs_iouring_unregister(mjs_uring *p, int fd);

/* Wait for readiness on ANY registered descriptor into `events` (up to cap
 * entries; cap == 0 is -EINVAL). timeout_ns: NULL = block until an
 * event/wake; *timeout_ns == 0 = non-blocking poll; otherwise relative
 * nanoseconds. Timeout expiry is SUCCESS with *out_n == 0. Returns -EINTR
 * raw for retry; out_n untouched on failure. Blocking (SYS-5): parks the
 * calling OS thread per the timeout contract ONLY. */
int mjs_iouring_wait(mjs_uring *p, mjs_poll_event *events, unsigned cap,
                     const uint64_t *timeout_ns, unsigned *out_n);

/* Make at most ONE blocked wait return promptly with zero events. A wake
 * with no waiter sticks for exactly one later wait. Never blocks (SYS-5). */
int mjs_iouring_wake(mjs_uring *p);

/* Report the configured SQ and CQ entry counts (SQ/CQ geometry, §38.7
 * io_uring-specific "SQ/CQ growth"). Non-blocking. */
int mjs_iouring_entries(mjs_uring *p, unsigned *out_sq, unsigned *out_cq);

/* Destroy the backend and free its handle: *p is NULLed on success, so any
 * later use (including a second destroy) is a deterministic -EINVAL.
 * Registered DESCRIPTORS are NOT closed. On failure the handle is NOT
 * consumed. NULL or NULLed *p is -EINVAL. */
int mjs_iouring_close(mjs_uring **p);


/* Register ctx's completion hook (additive, #66): hook(userdata) runs
 * EXACTLY ONCE, on ctx's synthetic stack, after ctx's entry() returns
 * and before the permanent switch-out to the most recent switcher.
 * A context completes at most one lifetime, so a hook fires at most
 * once per record; a hook registered after the context has already
 * FINISHED never fires (the completion pass has already run), and
 * destroy(ctx) discards any registered hook. hook == NULL clears.
 * Non-blocking (SYS-5); no allocation (SYS-4); NULL ctx is a caller
 * bug and is ignored (void entry point, same regime as
 * ms_context_destroy). (Declared above with the frozen s5-ctx block; the
 * s5-api section that follows is purely additive.) */
/* --- s5-api --- */
/*
 * S5.4 (issue #67): dynamic-dispatch shim. The Mojo NativeContext wrapper
 * must call a caller-supplied function whose address is a RUNTIME value
 * (opaque cookie). An indirect branch generated by Mojo inline-asm crashes
 * the b2 JIT, so the dispatch is moved to this C helper (SYS-2 C-firewall;
 * same pattern as the S1 callback lanes). mjs_ctx_call(fn, arg) calls
 * fn(arg); fn is an `ms_context_entry`-shaped void(void *). Always
 * succeeds (pure call). Purely additive: it neither reads nor writes the
 * 200-byte context record and is not part of the frozen ms_context ABI.
 */
void mjs_ctx_call(uintptr_t fn, uintptr_t arg);

/* --- s5-stack --- */
/*
 * S5.7 STACK GROWTH POLICY (issue #70, 5-expert panel ruling recorded in
 * docs §55): NativeContext stack reservations are FIXED — there is NO
 * automatic software growth and NO grow-on-demand. The enclosing
 * reservation is created by its OWNER (mjs_stack_alloc is the usual
 * provider: it paints a PROT_NONE guard page at the low end and returns
 * guard_low), and growth is the top-down descent over the range the
 * owner has COMMITTED (mjs_vm_commit / NativeStack.grow). Crossing the
 * guard floor faults synchronously at the faulting store
 * (SIGBUS/SIGSEGV) — the library never installs the guard, never
 * mprotects caller storage, and never soft-grows (SYS-4, ADR-SYS-005).
 *
 * The library enforces what it can without an ABI change (the record
 * stays 200 bytes; the v3 lifecycle tail is fully allocated):
 *   - ms_context_init rejects stack_low/size combinations that wrap
 *     (stack_top overflow, -EINVAL);
 *   - the switch backend traps (brk #0x6b) on a restored sp that is
 *     NOT 16-aligned (AAPCS64) BEFORE sp goes live — a poisoned or
 *     corrupt record can never resume into a wild stack pointer;
 *   - recorded bounds enforcement (sp inside the committed span) is
 *     the OWNER's job at dispatch: read the saved sp slot (@160) and
 *     reject to->sp < owner_floor || to->sp > top before switching.
 * Unguarded static-buffer reservations remain accepted for tests and
 * compatibility; overflow on them is documented caller error, never a
 * library promise.
 */

#ifdef __cplusplus
}
#endif

#endif /* MOJITO_SYS_H */
