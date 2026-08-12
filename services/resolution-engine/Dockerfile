# syntax=docker/dockerfile:1

# ---------- build ----------
FROM golang:1.23-alpine AS build

ARG VERSION=dev
WORKDIR /src

RUN apk add --no-cache git ca-certificates

# Dependencies first so the module layer caches independently of source edits.
COPY go.mod go.sum ./
RUN go mod download

COPY *.go ./
COPY internal/ ./internal/
RUN go mod verify

RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags "-s -w -X main.buildVersion=${VERSION}" \
    -o /out/resolver .

# ---------- runtime ----------
FROM alpine:3.20

RUN apk add --no-cache ca-certificates wget \
 && addgroup -g 10001 -S openontology \
 && adduser -u 10001 -S -G openontology openontology

COPY --from=build /out/resolver /usr/local/bin/resolver

USER openontology:openontology
EXPOSE 8081

HEALTHCHECK --interval=15s --timeout=3s --start-period=20s --retries=5 \
    CMD wget -qO- http://127.0.0.1:8081/readyz >/dev/null 2>&1 || exit 1

ENTRYPOINT ["/usr/local/bin/resolver"]
