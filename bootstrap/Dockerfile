# Build from the libs workspace root:
# docker build -f bootstrap/Dockerfile -t swarm-bootstrap:local .
FROM rust:1-bookworm AS builder

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY core ./core
COPY bootstrap ./bootstrap

RUN cargo build --release -p swarm-bootstrap

FROM debian:bookworm-slim AS runtime

RUN useradd --system --uid 10001 --create-home --home-dir /data swarm-bootstrap \
    && mkdir -p /data \
    && chown -R swarm-bootstrap:swarm-bootstrap /data

COPY --from=builder /app/target/release/swarm-bootstrap /usr/local/bin/swarm-bootstrap

USER swarm-bootstrap
WORKDIR /data

ENV RUST_LOG=info \
    SWARM_BOOTSTRAP_KEY_FILE=/data/identity.key \
    SWARM_BOOTSTRAP_LISTEN_ADDR=0.0.0.0 \
    SWARM_BOOTSTRAP_TCP_PORT=4001 \
    SWARM_BOOTSTRAP_QUIC_PORT=4001

EXPOSE 4001/tcp 4001/udp
ENTRYPOINT ["swarm-bootstrap"]
CMD ["run"]
