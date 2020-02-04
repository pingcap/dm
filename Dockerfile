FROM golang:1.13-alpine as builder
MAINTAINER siddontang

RUN apk add --no-cache \
    git make wget

RUN wget http://download.pingcap.org/tidb-enterprise-tools-latest-linux-amd64.tar.gz && \
    tar -xzf tidb-enterprise-tools-latest-linux-amd64.tar.gz && \
    mv tidb-enterprise-tools-latest-linux-amd64 /opt/tools

RUN mkdir -p /go/src/github.com/pingcap/dm
WORKDIR /go/src/github.com/pingcap/dm

# Cache dependencies
COPY go.mod .
COPY go.sum .

RUN GO111MODULE=on go mod download

COPY . .

RUN make dm-worker dm-master dm-tracer dmctl

FROM alpine:3.10

COPY --from=builder /go/src/github.com/pingcap/dm/bin/dm-worker /dm-worker
COPY --from=builder /go/src/github.com/pingcap/dm/bin/dm-master /dm-master
COPY --from=builder /go/src/github.com/pingcap/dm/bin/dm-tracer /dm-tracer
COPY --from=builder /go/src/github.com/pingcap/dm/bin/dmctl /dmctl
COPY --from=builder /opt/tools/bin/mydumper /mydumper

EXPOSE 8291 8261 8262
