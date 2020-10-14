FROM golang:1.14-alpine AS builder

RUN apk add --no-cache build-base

COPY . /home
WORKDIR /home

ARG version
ARG buildnum

RUN go build -v -mod=vendor -o hds-data-synchronizer

###########
FROM alpine

RUN apk --no-cache add ca-certificates

WORKDIR /home

LABEL NAME="LinkSmart HDS data synchronizer"

COPY --from=builder /home/hds-data-synchronizer .
COPY sample-conf/conf.json /home/conf/

ENV SYNC_TLS_CA=/tls/ca.pem
ENV SYNC_TLS_KEY=/tls/key.pem
ENV SYNC_TLS_STORAGE_DSN=/tls/certificates

VOLUME /tls

ENTRYPOINT ["./hds-data-synchronizer"]