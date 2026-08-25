# syntax=docker/dockerfile:1.26.0@sha256:ecfaec9ed6d810b56388c508f4121597bfbba70d41a6dfeee4d8cad5f295fc32
FROM golang:1.26.6-alpine3.23@sha256:e57c41c1d5864341031181b0db34b9a537bb5773eb6428e4e5bdaea0f9135406 AS build

ARG VERSION=dev
ARG REVISION=unknown
ARG BUILD_DATE=1970-01-01T00:00:00Z

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
COPY pkg ./pkg
RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath -buildvcs=false \
    -ldflags="-s -w -X main.version=${VERSION} -X main.revision=${REVISION} -X main.buildDate=${BUILD_DATE}" \
    -o /out/sirenaix-gateway ./cmd/sirenaix-gateway

FROM scratch

ARG VERSION=dev
ARG REVISION=unknown
ARG BUILD_DATE=1970-01-01T00:00:00Z
ARG SOURCE=https://github.com/SirenaAIx/sirenaix-messaging-gateway

LABEL org.opencontainers.image.title="SirenaIX Messaging Gateway" \
      org.opencontainers.image.description="Multi-tenant Google Messages gateway" \
      org.opencontainers.image.source="${SOURCE}" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${REVISION}" \
      org.opencontainers.image.created="${BUILD_DATE}" \
      org.opencontainers.image.licenses="AGPL-3.0-or-later"

COPY --from=build /out/sirenaix-gateway /usr/local/bin/sirenaix-gateway
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY LICENSE NOTICE.md /usr/share/licenses/sirenaix-messaging-gateway/
COPY LICENSE.exceptions /usr/share/licenses/sirenaix-messaging-gateway/LICENSE.exceptions
COPY third_party /usr/share/licenses/sirenaix-messaging-gateway/third_party/

USER 65532:65532
EXPOSE 8443 9090
ENTRYPOINT ["/usr/local/bin/sirenaix-gateway"]
