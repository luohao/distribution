FROM golang:1.13.4-alpine AS build

ENV DISTRIBUTION_DIR /go/src/github.com/docker/distribution
ENV DOCKER_BUILDTAGS include_oss include_gcs

ARG GOOS=linux
ARG GOARCH=amd64
ARG GOARM=6
ARG VERSION
ARG REVISION

RUN set -ex \
    && apk add --no-cache make git file

WORKDIR $DISTRIBUTION_DIR
COPY . $DISTRIBUTION_DIR
RUN CGO_ENABLED=0 make PREFIX=/go clean binaries && file ./bin/registry | grep "statically linked"

FROM docker-releases-local.artifactory.twitter.biz/kaos/twitter-os

COPY --from=build /go/src/github.com/docker/distribution/bin/registry /bin/registry
COPY --from=build /go/src/github.com/docker/distribution/registry/storage/driver/hdfs/config /etc/config/hadoop/
ENTRYPOINT ["registry"]
