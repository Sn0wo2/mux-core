FROM cgr.dev/chainguard/wolfi-base:latest

ARG TARGETPLATFORM
ARG BIN_NAME

RUN case "${TARGETPLATFORM}" in \
        linux/amd64|linux/arm64) ;; \
        *) echo "unsupported target platform: ${TARGETPLATFORM}" >&2; exit 1 ;; \
    esac \
    && apk add --no-cache ca-certificates libgcc \
    && mkdir -p /app \
    && chown 65532:65532 /app

COPY $TARGETPLATFORM/$BIN_NAME /usr/local/bin/app

USER 65532:65532

WORKDIR /app

ENTRYPOINT ["/usr/local/bin/app"]
