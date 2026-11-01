# syntax=docker/dockerfile:1

# ---- build stage ----
FROM golang:1.22-alpine AS build
WORKDIR /src
RUN apk add --no-cache git ca-certificates

# 先复制依赖文件，利用 Docker 层缓存
COPY app/go.mod app/go.sum ./
RUN go mod download

# 复制运行所需源码（发布包不包含测试和开发工具）
COPY app/ ./

# TARGETARCH 由 buildx 自动注入；普通 docker build 时回退当前 Go 架构
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH:-$(go env GOARCH)} \
    go build -trimpath -ldflags="-s -w" -o /out/emby-air ./cmd/emby-air

# ---- runtime stage ----
FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata ffmpeg
COPY --from=build /out/emby-air /usr/local/bin/emby-air

ENV DATA_DIR=/data \
    MEDIA_ROOT=/media \
    PORT=8098 \
    TZ=Asia/Shanghai

VOLUME ["/data"]
EXPOSE 8098
STOPSIGNAL SIGTERM
ENTRYPOINT ["/usr/local/bin/emby-air"]
