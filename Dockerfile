# Multi-stage build: compila os binários do backend (api + poller).
# O agente NÃO entra aqui — ele roda no SRVIMPORT (Windows), cross-compilado à parte.
FROM golang:1.25-alpine AS build
# Versão do build (sha + data), injetada pelo deploy.sh via --build-arg. Sem os args
# (build manual) fica "dev" — o /health denuncia. O .dockerignore exclui o .git, então
# o buildinfo do Go não tem vcs aqui dentro; -ldflags é o único caminho.
ARG GIT_COMMIT=dev
ARG BUILT_AT=
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN LDFLAGS="-X github.com/EnzzoHosaki/rps-xml-tracker/internal/version.Commit=${GIT_COMMIT} -X github.com/EnzzoHosaki/rps-xml-tracker/internal/version.BuiltAt=${BUILT_AT}" && \
    CGO_ENABLED=0 GOOS=linux go build -ldflags "$LDFLAGS" -o /out/tracker-api ./cmd/api && \
    CGO_ENABLED=0 GOOS=linux go build -ldflags "$LDFLAGS" -o /out/tracker-poller ./cmd/poller && \
    CGO_ENABLED=0 GOOS=linux go build -ldflags "$LDFLAGS" -o /out/tracker-migrate ./cmd/migrate && \
    CGO_ENABLED=0 GOOS=linux go build -ldflags "$LDFLAGS" -o /out/tracker-repoll ./cmd/repoll

FROM alpine:latest
RUN apk add --no-cache ca-certificates tzdata && adduser -D -u 10001 tracker
COPY --from=build /out/tracker-api /usr/local/bin/tracker-api
COPY --from=build /out/tracker-poller /usr/local/bin/tracker-poller
COPY --from=build /out/tracker-migrate /usr/local/bin/tracker-migrate
COPY --from=build /out/tracker-repoll /usr/local/bin/tracker-repoll
USER tracker
# command é definido por serviço no docker-compose (tracker-api / tracker-poller)
CMD ["tracker-api"]
