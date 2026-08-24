/* A0.1 contract verification for the vendored mojito-sys S0 substrate
 * (issue #10). Compiled and run directly with cc — no dylib required.
 *
 * Verifies:
 *   - frozen v2 ms_ctx_t layout via _Static_assert (168 B: regs[12] @0,
 *     fps[8] @96, sp @160) — the layout aarch64_switch.S and ms_ctx.c
 *     pin, kept in sync by the substrate's own asserts;
 *   - runtime: ms_page_size() > 0; ms_stack_alloc returns a 16-byte
 *     aligned top below a valid base; ms_stack_free round-trip;
 *   - aarch64_switch.S `#if !__APPLE__` guard: the asm refuses to
 *     assemble off Apple arm64; enforced here by an equivalent compile-
 *     time platform check plus a runtime check that the guard is present
 *     verbatim in the vendored source (path via AARCH64_SWITCH_S macro).
 *
 * Build (from spike/colorless_runtime/):
 *   cc -O2 -g -Wall -Wextra -I vendor/mojito-sys/include \
 *      -DAARCH64_SWITCH_S="<abs path to vendor/mojito-sys/aarch64_switch.S>" \
 *      tests/contract_verify.c \
 *      vendor/mojito-sys/native_stack.c \
 *      vendor/mojito-sys/ms_ctx.c \
 *      vendor/mojito-sys/aarch64_switch.S \
 *      -o build/contract_verify
 *   ./build/contract_verify
 */
#include "mojito_spike.h"

#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mach/mach.h>
#include <mach/mach_vm.h>

/* ------------------------------------------------------------------ */
/* Frozen v2 layout (aarch64_switch.S immediate offsets depend on it)  */
/* ------------------------------------------------------------------ */
_Static_assert(sizeof(ms_ctx_t) == 168,
               "ms_ctx_t must be 12 regs + 8 fps + sp = 168 bytes");
_Static_assert(offsetof(ms_ctx_t, regs) == 0,
               "regs[] must be at offset 0: asm stores x19..x30 @0..88");
_Static_assert(offsetof(ms_ctx_t, fps) == 96,
               "fps[] must be at offset 96: asm stp/ldp d8-d15 @96..159");
_Static_assert(offsetof(ms_ctx_t, sp) == 160,
               "sp must be at offset 160: asm str/ldr x16, [x1,#160]");

/* aarch64_switch.S targets Apple arm64 only (`#if !__APPLE__` #error).
 * Mirror that guard at C compile time so this test can never pass on a
 * host where the asm would refuse to build. */
#if !defined(__APPLE__)
#error "aarch64_switch.S targets Apple arm64 (Mach-O) only"
#endif

#ifndef AARCH64_SWITCH_S
#define AARCH64_SWITCH_S "vendor/mojito-sys/aarch64_switch.S"
#endif

/* ------------------------------------------------------------------ */

static int g_checks_failed = 0;
static int g_checks_total  = 0;

#define CHECK(cond, name)                                                    \
    do {                                                                     \
        ++g_checks_total;                                                    \
        if (cond) {                                                          \
            printf("PASS: %s\n", (name));                                    \
        } else {                                                             \
            printf("FAIL: %s\n", (name));                                    \
            ++g_checks_failed;                                               \
        }                                                                    \
    } while (0)

static void test_page_size(void) {
    long ps = ms_page_size();
    CHECK(ps > 0, "ms_page_size returns page size > 0");
}

static void test_stack_alloc_layout(void) {
    long ps = ms_page_size();
    void *base = NULL, *top = NULL;
    int rc = ms_stack_alloc((size_t)2 * (size_t)ps, &base, &top);
    CHECK(rc == 0, "ms_stack_alloc succeeds for 2 pages");
    if (rc != 0) {
        return;
    }
    CHECK(base != NULL, "ms_stack_alloc reports a base");
    CHECK(top != NULL, "ms_stack_alloc reports a top");
    CHECK((uintptr_t)base < (uintptr_t)top, "base (low) below top (high)");
    CHECK(((uintptr_t)top & 0xF) == 0, "top (initial SP) is 16-byte aligned");
    /* usable region spans at least the requested bytes below top */
    CHECK((size_t)((uintptr_t)top - (uintptr_t)base) >= 2 * (size_t)ps,
          "usable region covers the requested bytes");
    ms_stack_free(base);
    CHECK(1, "ms_stack_free round-trips without fault");
}

/* aarch64_switch.S `#if !__APPLE__` guard must exist verbatim in the
 * vendored source; read the whole file and require both lines. */
static void test_asm_guard(void) {
    FILE *f = fopen(AARCH64_SWITCH_S, "rb");
    if (f == NULL) {
        CHECK(0, "aarch64_switch.S readable at AARCH64_SWITCH_S");
        return;
    }
    long len;
    fseek(f, 0, SEEK_END);
    len = ftell(f);
    fseek(f, 0, SEEK_SET);
    char *buf = malloc((size_t)len + 1);
    if (buf == NULL) {
        fclose(f);
        CHECK(0, "alloc for full aarch64_switch.S read");
        return;
    }
    size_t n = fread(buf, 1, (size_t)len, f);
    fclose(f);
    buf[n] = '\0';
    CHECK(strstr(buf, "#if !defined(__APPLE__)") != NULL,
          "aarch64_switch.S contains `#if !defined(__APPLE__)` guard");
    CHECK(strstr(buf, "#error \"aarch64_switch.S targets Apple arm64 (Mach-O) only\"")
              != NULL,
          "aarch64_switch.S guard #error present");
    /* Pin the .set VALUES, not just the identifiers: a self-consistent
     * edit moving MS_CTX_SP_OFF to 96 would otherwise pass every check
     * while silently corrupting the saved sp. */
    CHECK(strstr(buf, "MS_CTX_FPS_OFF, 96") != NULL,
          "aarch64_switch.S pins fps offset to 96");
    CHECK(strstr(buf, "MS_CTX_SP_OFF, 160") != NULL,
          "aarch64_switch.S pins sp offset to 160");
    free(buf);
}

/* The allocator maps [base, base+page) as PROT_NONE. Geometry checks
 * alone cannot see protection (removing the guard page changes no
 * returned value), so query the mapping directly — read-only, no
 * allocation, macOS-arm64-only test. */
static void test_stack_guard_protection(void) {
    long ps = ms_page_size();
    void *base = NULL, *top = NULL;
    if (ms_stack_alloc((size_t)2 * (size_t)ps, &base, &top) != 0) {
        CHECK(0, "guard probe: ms_stack_alloc succeeds");
        return;
    }
    mach_vm_address_t addr = (mach_vm_address_t)(uintptr_t)base;
    mach_vm_size_t size = 0;
    vm_region_basic_info_data_64_t info;
    mach_msg_type_number_t count = VM_REGION_BASIC_INFO_COUNT_64;
    mach_port_t obj;
    kern_return_t kr =
        mach_vm_region(mach_task_self(), &addr, &size,
                       VM_REGION_BASIC_INFO_64,
                       (vm_region_info_t)&info, &count, &obj);
    CHECK(kr == KERN_SUCCESS, "guard probe: mach_vm_region query on base");
    if (kr == KERN_SUCCESS) {
        CHECK((info.protection & (VM_PROT_READ | VM_PROT_WRITE)) == 0,
              "guard page below base has no read/write protection");
    }
    ms_stack_free(base);
}

int main(void) {
    printf("contract_verify: A0.1 vendored substrate (issue #10)\n");
    test_page_size();
    test_stack_alloc_layout();
    test_asm_guard();
    test_stack_guard_protection();
    printf("contract_verify: %d checks, %d failed\n",
           g_checks_total, g_checks_failed);
    return g_checks_failed == 0 ? 0 : 1;
}