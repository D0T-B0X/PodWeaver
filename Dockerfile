FROM rust:1.89-bookworm AS builder
WORKDIR /app
COPY . .
RUN cargo build --release --features ui \
    --bin pod_weaver --bin lighthouse --bin podweaver-ui

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/pod_weaver   /usr/local/bin/
COPY --from=builder /app/target/release/lighthouse   /usr/local/bin/
COPY --from=builder /app/target/release/podweaver-ui /usr/local/bin/

ENTRYPOINT ["pod_weaver"]