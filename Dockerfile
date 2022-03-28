FROM rust:1.59-buster as build

# MongoDB binaries
RUN apt-get update && \
    apt-get install -y mongo-tools

# Postgres binaries
RUN apt-get install -y wget postgresql-client

# create a new empty shell project
RUN USER=root cargo new --bin replibyte
WORKDIR /replibyte
RUN USER=root cargo new --lib replibyte
RUN USER=root cargo new --lib dump-parser

# copy over your manifests
# root
COPY ./Cargo.lock ./Cargo.lock
COPY ./Cargo.toml ./Cargo.toml

# dump-parser
COPY ./dump-parser ./dump-parser

# replibyte
COPY ./replibyte/Cargo.toml ./replibyte/Cargo.toml
COPY ./replibyte/Cargo.lock ./replibyte/Cargo.lock

# this build step will cache your dependencies
RUN cargo build --release
RUN rm src/*.rs

# copy your source tree
COPY ./replibyte/src ./replibyte/src
COPY ./dump-parser/src ./dump-parser/src

# build for release
RUN rm ./target/release/deps/replibyte*
RUN cargo build --release

# our final base
FROM rust:1.59-slim-buster

# copy the build artifact from the build stage
COPY --from=build /replibyte/target/release/replibyte .

COPY ./docker/* /
RUN chmod +x exec.sh && chmod +x replibyte

ARG S3_ACCESS_KEY_ID
ENV S3_ACCESS_KEY_ID $S3_ACCESS_KEY_ID

ARG S3_SECRET_ACCESS_KEY
ENV S3_SECRET_ACCESS_KEY $S3_SECRET_ACCESS_KEY

ARG S3_REGION
ENV S3_REGION $S3_REGION

ARG S3_BUCKET
ENV S3_BUCKET $S3_BUCKET

ARG SOURCE_CONNECTION_URI
ENV SOURCE_CONNECTION_URI $SOURCE_CONNECTION_URI

ARG DESTINATION_CONNECTION_URI
ENV DESTINATION_CONNECTION_URI $DESTINATION_CONNECTION_URI

ARG ENCRYPTION_SECRET
ENV ENCRYPTION_SECRET $ENCRYPTION_SECRET

ENTRYPOINT ["sh", "exec.sh"]
