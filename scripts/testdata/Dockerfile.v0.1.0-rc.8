FROM node:22-alpine AS web-build
WORKDIR /src
COPY web/package.json web/package-lock.json ./web/
RUN npm --prefix web ci
COPY web ./web
COPY internal/server/static ./internal/server/static
RUN npm --prefix web run build

FROM golang:1.23-alpine AS build
WORKDIR /src
ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_TIME=unknown
ARG RELEASE_CHANNEL=development
COPY go.mod ./
COPY . .
COPY --from=web-build /src/internal/server/static/ui ./internal/server/static/ui
RUN set -eu; \
	mkdir -p /out/initial /out/adapters /out/plugins; \
    app_ldflags="-s -w -X github.com/integrated-recorder/core/internal/buildinfo.version=${VERSION} -X github.com/integrated-recorder/core/internal/buildinfo.commit=${COMMIT} -X github.com/integrated-recorder/core/internal/buildinfo.buildTime=${BUILD_TIME} -X github.com/integrated-recorder/core/internal/buildinfo.releaseChannel=${RELEASE_CHANNEL}"; \
    CGO_ENABLED=0 go build -trimpath -ldflags="${app_ldflags}" -o /out/runtime-host ./cmd/runtime-host; \
	CGO_ENABLED=0 go build -trimpath -ldflags="${app_ldflags}" -o /out/initial/control-plane ./cmd/control-plane; \
	CGO_ENABLED=0 go build -trimpath -ldflags="${app_ldflags}" -o /out/initial/recorder-engine ./cmd/recorder-engine; \
	CGO_ENABLED=0 go build -trimpath -ldflags="${app_ldflags}" -o /out/adapters/integrated-recorder-adapter-hls ./cmd/adapters/hls; \
	CGO_ENABLED=0 go build -trimpath -ldflags="${app_ldflags}" -o /out/plugins/storage.local ./cmd/storage-local; \
	chmod 0555 /out/runtime-host /out/initial/control-plane /out/initial/recorder-engine /out/adapters/integrated-recorder-adapter-hls /out/plugins/storage.local

FROM alpine:3.21
RUN apk add --no-cache ca-certificates ffmpeg && adduser -D -H -u 10001 archiver && mkdir -p /data /opt/integrated-recorder/initial /usr/local/lib/integrated-recorder/plugins && chown archiver:archiver /data
COPY --from=build /out/runtime-host /usr/local/bin/runtime-host
COPY --from=build /out/initial/ /opt/integrated-recorder/initial/
COPY --from=build /out/adapters/ /adapters/
COPY --from=build /out/plugins/storage.local /usr/local/lib/integrated-recorder/plugins/storage.local
RUN chmod 0555 /usr/local/bin/runtime-host /opt/integrated-recorder/initial/control-plane /opt/integrated-recorder/initial/recorder-engine /usr/local/lib/integrated-recorder/plugins/storage.local && chown -R root:root /opt/integrated-recorder /usr/local/lib/integrated-recorder /adapters
USER 10001:10001
ENV ADDR=:8080 DATA_DIR=/data ADAPTER_DIR=/external-adapters
EXPOSE 8080
VOLUME ["/data"]
ENTRYPOINT ["/usr/local/bin/runtime-host"]
