# syntax=docker/dockerfile:1

FROM golang:1.26 AS build
WORKDIR /src
RUN mkdir -p /src/data

COPY go.mod ./
COPY go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o /out/unified-api-gateway ./cmd/gateway

FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /app
COPY --from=build /out/unified-api-gateway /app/unified-api-gateway
COPY --chown=nonroot:nonroot configs /app/configs
COPY --chown=nonroot:nonroot web /app/web
COPY --from=build --chown=nonroot:nonroot /src/data /app/data

EXPOSE 8780 8781 8782
USER nonroot:nonroot
ENTRYPOINT ["/app/unified-api-gateway"]
CMD ["--config", "/app/configs/gateway.yaml", "--admin", ":8781"]
