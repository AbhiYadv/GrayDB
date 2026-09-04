FROM rust:1.95-bookworm AS build

ARG GIT_SHA=unrecorded
WORKDIR /workspace
COPY . .
RUN test -n "$GIT_SHA" \
    && cargo build --locked --release --package graydb-studio

FROM debian:bookworm-slim

ARG GIT_SHA=unrecorded
LABEL org.opencontainers.image.revision=$GIT_SHA
RUN apt-get update \
    && apt-get install --no-install-recommends -y ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /workspace/target/release/graydb-studio /usr/local/bin/graydb-studio
COPY bench/r1/graydb-r1.toml /etc/graydb/graydb-r1.toml
ENTRYPOINT ["/usr/local/bin/graydb-studio"]
