FROM rust:1.94.1-bookworm AS builder
RUN apt-get update \
    && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY . .
RUN cargo build --release -p logpose-server

FROM debian:bookworm-slim
WORKDIR /opt/logpose
COPY --from=builder /app/target/release/logpose-server /usr/local/bin/logpose-server
EXPOSE 8080 50051
ENTRYPOINT ["/usr/local/bin/logpose-server"]
