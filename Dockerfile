FROM rust:1.87 AS builder
WORKDIR /app
COPY . .
RUN cargo build --release --bin node

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/node /usr/local/bin/node
ENTRYPOINT ["node"]