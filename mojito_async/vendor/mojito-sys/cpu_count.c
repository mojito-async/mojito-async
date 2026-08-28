/*
 * cpu_count.c — ms_cpu_logical_count: logical processor count for scheduler
 * sizing (A2.1, issue #67; spec §89 "Defaults should use detected
 * hardware/runtime guidance").
 *
 * Deliberately standalone: does NOT include mojito_spike.h, so the frozen
 * S0 header stays untouched; the single exported symbol rides the dylib
 * alongside the ms_* contract surface.
 *
 * macOS: hw.logicalcpu via sysctl (includes efficiency cores, which is what
 * an M:N scheduling pool should size against — THREAD_COUNT equivalent).
 * Other POSIX: sysconf(_SC_NPROCESSORS_ONLN).  Any failure degrades to 1
 * (a pool of one worker is always valid — the A1 single-worker invariant).
 */
#include <stddef.h>
#include <stdint.h>

#if defined(__APPLE__)
#include <sys/sysctl.h>
#include <sys/types.h>
#else
#include <unistd.h>
#endif

int ms_cpu_logical_count(void) {
#if defined(__APPLE__)
    int n = 1;
    size_t len = sizeof(n);
    if (sysctlbyname("hw.logicalcpu", &n, &len, NULL, 0) != 0)
        return 1;
    return n > 0 ? n : 1;
#else
    long n = sysconf(_SC_NPROCESSORS_ONLN);
    return n > 0 ? (int)n : 1;
#endif
}