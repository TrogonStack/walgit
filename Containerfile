# walgit container images: one Containerfile, one final target per binary.
#
#   podman build -t walgit -f Containerfile .                          # --target walgit, the default
#   podman run --rm -p 8080:8080 \
#       -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY \
#       -v ./walgit.toml:/etc/walgit/walgit.toml:ro \
#       -v walgit-cache:/var/lib/walgit \
#       walgit
#
#   podman build --target walgit-nats -t walgit-nats -f Containerfile .
#   podman run --rm \
#       -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e NATS_URL -e NATS_CREDS \
#       -v ./walgit.toml:/etc/walgit/walgit.toml:ro \
#       walgit-nats
#
# walgit serves git, so its image carries git (upload-pack, repack and index-pack run as
# subprocesses), git-lfs, GNU coreutils (bounded sort/comm), CA certificates and tini, and
# listens on 8080 with a /readyz healthcheck. Its cache (materialized repositories, a
# self-signed TLS cert) lives under /var/lib/walgit and can be wiped at any time.
#
# walgit-nats tails the WAL and publishes to JetStream. It has no listener, so it has no
# port and no HTTP healthcheck, and it builds without the web stage. Its only subprocesses are
# `git init --bare` and `git config` when it creates a repository's local refs copy, so its
# image carries git and CA certificates and nothing else. That refs-only cache lives under /var/lib/walgit-nats
# (tmpfs is fine).
#
# Both read the same walgit.toml and WALGIT__SECTION__KEY overrides; each binary reads only
# its own sections (docs/EVENTS.md). `nix build .#image` and `.#image-nats` produce the same
# two images from flake.nix.

# ---- 1. web UI (embedded into the walgit binary at compile time) ---------------------
FROM docker.io/library/node:24-bookworm-slim AS web
RUN corepack enable && corepack prepare pnpm@10 --activate
WORKDIR /src/web
COPY web/package.json web/pnpm-lock.yaml ./
RUN pnpm install --frozen-lockfile
COPY web/ ./
RUN pnpm run build && test -f dist/index.html && test -f dist/repos.js

# ---- 2. rust toolchain, shared by both builds ----------------------------------------
# libprotobuf-dev, not just protobuf-compiler: Debian ships protoc in one package and the
# well-known types it resolves `import "google/protobuf/timestamp.proto"` against in the other,
# and --no-install-recommends means the second one has to be asked for by name.
FROM docker.io/library/rust:1.97-bookworm AS rust
RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler libprotobuf-dev pkg-config cmake perl python3 \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates ./crates
ARG WALGIT_BUILD_SHA=dev
ENV WALGIT_BUILD_SHA=${WALGIT_BUILD_SHA}

# ---- 3a. walgit (with the web UI) ------------------------------------------------------
FROM rust AS build-walgit
COPY --from=web /src/web/dist ./web/dist
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target,id=walgit-target \
    cargo build --release --locked -p walgit-cli \
    && install -D target/release/walgit /out/bin/walgit \
    && install -D target/release/walgit-server /out/bin/walgit-server

# ---- 3b. walgit-nats (no web stage) -----------------------------------------------------
FROM rust AS build-nats
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target,id=walgit-nats-target \
    cargo build --release --locked -p walgit-nats \
    && install -D target/release/walgit-nats /out/bin/walgit-nats

# ---- 4a. walgit-nats runtime ------------------------------------------------------------
FROM docker.io/library/debian:trixie-slim AS walgit-nats
RUN apt-get update && apt-get install -y --no-install-recommends git ca-certificates \
    && rm -rf /var/lib/apt/lists/*
RUN useradd --uid 1000 --create-home --shell /usr/sbin/nologin walgit \
    && mkdir -p /etc/walgit /var/lib/walgit-nats && chown walgit:walgit /var/lib/walgit-nats
COPY --from=build-nats /out/bin/walgit-nats /usr/local/bin/walgit-nats
COPY walgit.example.toml /etc/walgit/walgit.toml
ENV RUST_LOG=info,walgit=debug \
    WALGIT_CONFIG=/etc/walgit/walgit.toml \
    WALGIT__CACHE__DIR=/var/lib/walgit-nats
USER walgit
WORKDIR /var/lib/walgit-nats
VOLUME ["/var/lib/walgit-nats"]
ENTRYPOINT ["/usr/local/bin/walgit-nats"]

# ---- 4b. walgit runtime (the default target, last stage) --------------------------------
# trixie ships git 2.47+: walgit wants >= 2.47 on the server (pack.writeReverseIndex,
# `index-pack --rev-index`); clients need >= 2.46.
FROM docker.io/library/debian:trixie-slim AS walgit
RUN apt-get update && apt-get install -y --no-install-recommends git git-lfs coreutils ca-certificates tini curl \
    && rm -rf /var/lib/apt/lists/* \
    && git --version
RUN useradd --uid 1000 --create-home --shell /bin/sh walgit \
    && mkdir -p /etc/walgit /var/lib/walgit && chown walgit:walgit /var/lib/walgit
COPY --from=build-walgit /out/bin/walgit /out/bin/walgit-server /usr/local/bin/
COPY walgit.example.toml /etc/walgit/walgit.toml
ENV RUST_LOG=info,walgit=debug \
    WALGIT_CONFIG=/etc/walgit/walgit.toml \
    WALGIT__CACHE__DIR=/var/lib/walgit \
    WALGIT__SERVER__LISTEN=0.0.0.0:8080
USER walgit
WORKDIR /home/walgit
EXPOSE 8080
VOLUME ["/var/lib/walgit"]
HEALTHCHECK --interval=30s --timeout=5s CMD curl -fsS http://127.0.0.1:8080/readyz || exit 1
ENTRYPOINT ["tini", "--", "walgit"]
CMD ["serve"]
