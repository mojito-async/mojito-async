# tests/docker/Dockerfile — the Linux host the mojito-async suite has never had.
#
# mojito-async#141's finding is that the Linux/epoll/x86-64 lanes have never
# executed anywhere: no CI, and a developer host that is macOS/arm64.  This
# image is that host, reproducible from any machine with Docker rather than
# only inside GitHub Actions, and it is the SAME base the ci.yml suite-linux
# job runs in, so a local red and a CI red mean the same thing.
#
# The Mojo toolchain is NOT baked in: .github/scripts/install-mojo.sh pins the
# version and rewrites modular.cfg, and having one place that knows how to
# install Mojo is worth more than a faster image.
FROM debian:bookworm

RUN apt-get update -qq \
 && apt-get install -y -qq --no-install-recommends \
      build-essential ca-certificates curl git unzip zstd \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /work
