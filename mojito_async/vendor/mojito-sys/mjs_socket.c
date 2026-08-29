/* mojito-sys S6.2 — non-blocking sockets (issue #74, spec §26).
 *
 * Frozen-ABI entry points (native/include/mojito_sys.h, s6-socket block):
 *   mjs_socket_{socket,set_nonblocking,bind,listen,connect,accept,
 *               recv,send,shutdown,close}
 *   mjs_sockaddr_{ipv4,format4}
 *
 * Return contract: 0 == success; negative == -errno; out-params untouched
 * on failure (NULL out-slot => -EFAULT before anything else happens).
 *
 * EINTR/EAGAIN doctrine (spec §38.11): accept/recv/send perform exactly ONE
 * syscall attempt and surface -EINTR/-EAGAIN raw — a reactor decides
 * whether to retry or re-poll; this layer never spins and never waits. The
 * one-shot entry points cannot be interrupted mid-operation, so their errno
 * passes through unchanged.
 *
 * Portability notes:
 *   - AF_INET6 is 30 on darwin, 10 on Linux: callers use the neutral
 *     MJS_SOCK_INET6 spelling and the mapping lives HERE.
 *   - darwin sockaddr_in carries sin_len at byte 0; Linux does not. The
 *     neutral mjs_sockaddr keeps that mess out of the Mojo layer: the port
 *     travels host-order at a fixed offset and the OS-specific prefix is
 *     synthesized inside mjs_sa_to_os().
 *   - Linux accepted sockets inherit O_NONBLOCK through accept(2); darwin
 *     does not. mjs_socket_accept re-applies the listener's flags to the
 *     child explicitly so WouldBlock behavior is identical across hosts.
 */

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <stddef.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include "mojito_sys.h"


/* ---- neutral <-> OS sockaddr conversion -------------------------------- */

static int mjs_af_of(int family)
{
    switch (family) {
    case MJS_SOCK_INET:
        return AF_INET;
    case MJS_SOCK_INET6:
        return AF_INET6;
    case MJS_SOCK_UNIX:
        return AF_UNIX;
    default:
        return -1;
    }
}

/* Build the OS sockaddr for `in`. Returns 0, or -EINVAL/-ENAMETOOLONG with
 * `out` zeroed but otherwise meaningless (callers treat any nonzero as
 * "nothing happened"). */
static int mjs_sa_to_os(const mjs_sockaddr *in, struct sockaddr_storage *out,
                        socklen_t *outlen)
{
    memset(out, 0, sizeof(*out));

    if (in->family == MJS_SOCK_INET) {
        struct sockaddr_in *sin = (struct sockaddr_in *)out;
        sin->sin_family = AF_INET;
        memcpy(&sin->sin_addr, in->octets, 4);
        sin->sin_port = htons(in->port);
        *outlen = (socklen_t)sizeof(*sin);
        return 0;
    }
    if (in->family == MJS_SOCK_INET6) {
        struct sockaddr_in6 *sin6 = (struct sockaddr_in6 *)out;
        sin6->sin6_family = AF_INET6;
        memcpy(&sin6->sin6_addr, in->octets, 16);
        sin6->sin6_port = htons(in->port);
        sin6->sin6_flowinfo = in->flowinfo;
        sin6->sin6_scope_id = in->scope_id;
        *outlen = (socklen_t)sizeof(*sin6);
        return 0;
    }
    if (in->family == MJS_SOCK_UNIX) {
        struct sockaddr_un *sun = (struct sockaddr_un *)out;
        sun->sun_family = AF_UNIX;
        size_t n = strnlen(in->path, sizeof(in->path));
        if (n == 0 || n >= sizeof(sun->sun_path))
            return -ENAMETOOLONG;
        memcpy(sun->sun_path, in->path, n + 1);
        *outlen = (socklen_t)(offsetof(struct sockaddr_un, sun_path) + n + 1);
        return 0;
    }
    return -EINVAL;
}

/* Best-effort reverse conversion for peer addresses reported by accept(2).
 * Unrecognized families pass the raw family number through so diagnostics
 * keep something; every other field is zeroed first. Never fails. */
static void mjs_sa_to_neutral(const struct sockaddr_storage *ss, socklen_t len,
                              mjs_sockaddr *out)
{
    memset(out, 0, sizeof(*out));

    if (ss->ss_family == AF_INET && len >= sizeof(struct sockaddr_in)) {
        const struct sockaddr_in *sin = (const struct sockaddr_in *)ss;
        out->family = MJS_SOCK_INET;
        out->port = ntohs(sin->sin_port);
        memcpy(out->octets, &sin->sin_addr, 4);
    } else if (ss->ss_family == AF_INET6 &&
               len >= sizeof(struct sockaddr_in6)) {
        const struct sockaddr_in6 *sin6 = (const struct sockaddr_in6 *)ss;
        out->family = MJS_SOCK_INET6;
        out->port = ntohs(sin6->sin6_port);
        out->flowinfo = sin6->sin6_flowinfo;
        out->scope_id = sin6->sin6_scope_id;
        memcpy(out->octets, &sin6->sin6_addr, 16);
    } else if (ss->ss_family == AF_UNIX && len >= sizeof(sa_family_t)) {
        const struct sockaddr_un *sun = (const struct sockaddr_un *)ss;
        out->family = MJS_SOCK_UNIX;
        size_t cap = (size_t)(len - offsetof(struct sockaddr_un, sun_path));
        if (cap > sizeof(out->path) - 1)
            cap = sizeof(out->path) - 1;
        /* Bound-copy; NUL-terminate even for unnamed/abstract peers. */
        memcpy(out->path, sun->sun_path, cap);
        out->path[cap] = '\0';
        out->path[sizeof(out->path) - 1] = '\0';
    } else {
        out->family = (int32_t)ss->ss_family;
    }
}

/* ---- entry points ------------------------------------------------------- */

int mjs_socket_socket(int family, int type, int *out_fd)
{
    if (out_fd == NULL)
        return -EFAULT;
    int af = mjs_af_of(family);
    if (af < 0)
        return -EINVAL;
    if (type != MJS_SOCK_STREAM && type != MJS_SOCK_DGRAM)
        return -EINVAL;
    int fd = socket(af, type, 0);
    if (fd < 0)
        return -errno;
    *out_fd = fd;
    return 0;
}

int mjs_socket_set_nonblocking(int fd, int enabled)
{
    if (fd < 0)
        return -EBADF;
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0)
        return -errno;
    int next = (enabled != 0) ? (flags | O_NONBLOCK) : (flags & ~O_NONBLOCK);
    if (fcntl(fd, F_SETFL, next) < 0)
        return -errno;
    return 0;
}

int mjs_socket_bind(int fd, const mjs_sockaddr *addr)
{
    if (addr == NULL)
        return -EFAULT;
    if (fd < 0)
        return -EBADF;
    struct sockaddr_storage ss;
    socklen_t sl = 0;
    int conv = mjs_sa_to_os(addr, &ss, &sl);
    if (conv != 0)
        return conv;
    if (bind(fd, (const struct sockaddr *)&ss, sl) < 0)
        return -errno;
    return 0;
}

int mjs_socket_listen(int fd, int backlog)
{
    if (fd < 0)
        return -EBADF;
    if (backlog < 0)
        return -EINVAL;
    if (listen(fd, backlog) < 0)
        return -errno;
    return 0;
}

int mjs_socket_connect(int fd, const mjs_sockaddr *addr)
{
    if (addr == NULL)
        return -EFAULT;
    if (fd < 0)
        return -EBADF;
    struct sockaddr_storage ss;
    socklen_t sl = 0;
    int conv = mjs_sa_to_os(addr, &ss, &sl);
    if (conv != 0)
        return conv;
    if (connect(fd, (const struct sockaddr *)&ss, sl) < 0)
        return -errno;
    return 0;
}

int mjs_socket_accept(int fd, int *out_client, mjs_sockaddr *out_peer)
{
    if (out_client == NULL)
        return -EFAULT;
    if (fd < 0)
        return -EBADF;

    struct sockaddr_storage ss;
    socklen_t sl = sizeof(ss);
    struct sockaddr *peer = NULL;
    socklen_t *peerlen = NULL;
    if (out_peer != NULL) {
        peer = (struct sockaddr *)&ss;
        peerlen = &sl;
    }

    int c = accept(fd, peer, peerlen);
    if (c < 0)
        return -errno;

    /* Deterministic O_NONBLOCK inheritance (see file header). */
    int child_flags = fcntl(c, F_GETFL, 0);
    int parent_flags = fcntl(fd, F_GETFL, 0);
    if (child_flags >= 0 && parent_flags >= 0) {
        int wanted = (parent_flags & O_NONBLOCK)
                         ? (child_flags | O_NONBLOCK)
                         : (child_flags & ~O_NONBLOCK);
        if (wanted != child_flags)
            (void)fcntl(c, F_SETFL, wanted);
    }

    if (out_peer != NULL)
        mjs_sa_to_neutral(&ss, sl, out_peer);

    *out_client = c;
    return 0;
}

int mjs_socket_recv(int fd, unsigned char *buf, size_t len, size_t *out_n)
{
    if (len == 0)
        return -EINVAL;
    if (buf == NULL || out_n == NULL)
        return -EFAULT;
    if (fd < 0)
        return -EBADF;

    ssize_t r = recv(fd, buf, len, 0);
    if (r < 0)
        return -errno;
    *out_n = (size_t)r;
    return 0;
}

int mjs_socket_send(int fd, const unsigned char *buf, size_t len, size_t *out_n)
{
    if (len == 0)
        return -EINVAL;
    if (buf == NULL || out_n == NULL)
        return -EFAULT;
    if (fd < 0)
        return -EBADF;

    ssize_t r = send(fd, buf, len, 0);
    if (r < 0)
        return -errno;
    *out_n = (size_t)r;
    return 0;
}

int mjs_socket_shutdown(int fd, int how)
{
    if (fd < 0)
        return -EBADF;
    if (how != MJS_SHUT_READ && how != MJS_SHUT_WRITE && how != MJS_SHUT_BOTH)
        return -EINVAL;
    if (shutdown(fd, how) < 0)
        return -errno;
    return 0;
}

int mjs_socket_close(int fd)
{
    if (fd < 0)
        return -EBADF;
    if (close(fd) < 0)
        return -errno;
    return 0;
}

/* ---- A7.3/A7.4 (issues #77/#78) deviation: SO_REUSEADDR / SO_ERROR -----
 *
 * Not part of the frozen s6-socket block above (native/include/
 * mojito_sys.h) — these two entry points extend the vendored ABI for the
 * mojito-async connect/accept lane and are NOT upstreamed to
 * github.com/mojito-async/mojito-sys. Same return contract as every other
 * entry point in this file: 0 == success, negative == -errno.
 */

int mjs_socket_set_reuseaddr(int fd, int enabled)
{
    if (fd < 0)
        return -EBADF;
    int val = (enabled != 0) ? 1 : 0;
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val)) < 0)
        return -errno;
    return 0;
}

/* Decode a pending non-blocking connect()'s outcome via
 * getsockopt(SO_ERROR): *out_err receives 0 (established) or the positive
 * errno the failed handshake reported (e.g. ECONNREFUSED). The RETURN
 * value is the getsockopt(2) call's own status (0/-errno) — distinct from
 * *out_err, which carries the CONNECT outcome. */
int mjs_socket_connect_error(int fd, int *out_err)
{
    if (out_err == NULL)
        return -EFAULT;
    if (fd < 0)
        return -EBADF;
    int so_err = 0;
    socklen_t sl = sizeof(so_err);
    if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &so_err, &sl) < 0)
        return -errno;
    *out_err = so_err;
    return 0;
}

int mjs_sockaddr_ipv4(const char *dotted, int port, mjs_sockaddr *out)
{
    if (dotted == NULL || out == NULL)
        return -EFAULT;
    if (port < 0 || port > 65535)
        return -EINVAL;

    struct in_addr a;
    memset(&a, 0, sizeof(a));
    if (inet_pton(AF_INET, dotted, &a) != 1)
        return -EINVAL;

    memset(out, 0, sizeof(*out));
    out->family = MJS_SOCK_INET;
    out->port = (uint16_t)port;
    memcpy(out->octets, &a.s_addr, 4);
    return 0;
}

int mjs_sockaddr_format4(const mjs_sockaddr *addr, char *out_buf, size_t cap,
                         size_t *out_len)
{
    if (addr == NULL || out_buf == NULL || out_len == NULL)
        return -EFAULT;
    if (addr->family != MJS_SOCK_INET)
        return -EINVAL;

    char tmp[INET_ADDRSTRLEN];
    memset(tmp, 0, sizeof(tmp));
    if (inet_ntop(AF_INET, addr->octets, tmp, sizeof(tmp)) == NULL)
        return -errno;

    size_t n = strlen(tmp);
    if (cap < n + 1)
        return -EINVAL;
    memcpy(out_buf, tmp, n + 1);
    *out_len = n;
    return 0;
}
