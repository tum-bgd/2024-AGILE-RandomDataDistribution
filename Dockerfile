FROM rust:slim AS builder

WORKDIR /app

COPY ./ .

RUN cargo build -p pc-server --release


FROM debian:stable-slim AS server

WORKDIR /app

COPY --from=builder /app/target/release/pc-server ./

CMD [ "./pc-server" ]