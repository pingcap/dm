FROM alpine:3.10
RUN apk add --no-cache tzdata
ADD dm-master /dm-master
ADD dm-worker /dm-worker
ADD dmctl /dmctl
ADD chaos-case /chaos-case
ADD source1.yaml /source1.yaml
ADD source2.yaml /source2.yaml
ADD task-single.yaml /task-single.yaml

RUN chmod +x /dm-master
RUN chmod +x /dm-worker
RUN chmod +x /dmctl
RUN chmod +x /chaos-case

WORKDIR /

EXPOSE 8291 8261 8262
