FROM rust:1.95.0-bookworm AS build
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY services/api services/api
RUN cargo build --release --locked -p pgderive-api

FROM debian:bookworm-slim
COPY --from=build /app/target/release/pgderive-api /usr/local/bin/pgderive-api
USER 10001:10001
STOPSIGNAL SIGINT
ENTRYPOINT ["/usr/local/bin/pgderive-api"]
