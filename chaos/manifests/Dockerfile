FROM alpine:3.10
ADD dm-master /dm-master
ADD dm-worker /dm-worker
ADD dmctl /dmctl

RUN chmod +x /dm-master
RUN chmod +x /dm-worker
RUN chmod +x /dmctl

WORKDIR /

EXPOSE 8291 8261 8262
