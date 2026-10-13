FROM rust:1.88 AS builder
WORKDIR /app
COPY . .
RUN cargo build --release --bin pod_weaver --bin lighthouse

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/pod_weaver /usr/local/bin/pod_weaver
COPY --from=builder /app/target/release/lighthouse /usr/local/bin/lighthouse
ENTRYPOINT ["pod_weaver"]