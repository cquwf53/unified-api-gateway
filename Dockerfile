# syntax=docker/dockerfile:1

FROM golang:1.26 AS build
WORKDIR /src
RUN mkdir -p /src/data

COPY go.mod ./
COPY go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath \
    -ldflags="-s -w -X unified-api-gateway/internal/server.BuildVersion=${VERSION}" \
    -o /out/unified-api-gateway ./cmd/gateway

FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /app
COPY --from=build /out/unified-api-gateway /app/unified-api-gateway
# Web console assets are embedded in the binary; only the example config and
# data directory remain on disk.
COPY --chown=nonroot:nonroot configs /app/configs
COPY --from=build --chown=nonroot:nonroot /src/data /app/data

EXPOSE 8780 8781 8782
USER nonroot:nonroot
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s \
    CMD ["/app/unified-api-gateway", "validate", "--config", "/app/configs/gateway.yaml"]
ENTRYPOINT ["/app/unified-api-gateway"]
CMD ["--config", "/app/configs/gateway.yaml", "--admin", ":8781"]
