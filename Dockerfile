# Multi-stage build: compila os binários do backend (api + poller).
# O agente NÃO entra aqui — ele roda no SRVIMPORT (Windows), cross-compilado à parte.
FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/tracker-api ./cmd/api && \
    CGO_ENABLED=0 GOOS=linux go build -o /out/tracker-poller ./cmd/poller && \
    CGO_ENABLED=0 GOOS=linux go build -o /out/tracker-migrate ./cmd/migrate

FROM alpine:latest
RUN apk add --no-cache ca-certificates tzdata && adduser -D -u 10001 tracker
COPY --from=build /out/tracker-api /usr/local/bin/tracker-api
COPY --from=build /out/tracker-poller /usr/local/bin/tracker-poller
COPY --from=build /out/tracker-migrate /usr/local/bin/tracker-migrate
USER tracker
# command é definido por serviço no docker-compose (tracker-api / tracker-poller)
CMD ["tracker-api"]
