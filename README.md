# rps-xml-tracker

Rastreabilidade ponta-a-ponta de notas fiscais (XML) da RPS Contábil. Trata a **chave de acesso
(44 dígitos)** como *trace ID* e cada etapa do fluxo como um *span*: **chegada** (XML_ASINCRONIZAR)
→ **sincronização** (XML_SINCRONIZADO) → **importação** (Athenas/Firebird). Observação 100%
**read-only**. A UI vive no `rps-maestro` (seção `/xml`) e consome esta API.

- Plano/arquitetura: `~/.claude/plans/enumerated-moseying-goose.md`
- Investigação (Fase 0, read-only): `phase0/` — `fbinspect` (Firebird) e `fsscan` (filesystem)
- Design: `design/schema.sql`, `design/openapi.yaml`

## Componentes (Fase 1)
- `cmd/api` — API HTTP (Gin). Ingestão (HMAC do agente) + leitura (JWT do maestro).
- `internal/derive` — função pura: observações → estado derivado da nota.
- `internal/store` — interface `Store` com impl em memória e **Postgres (pgx)**.
- (próximos) `cmd/poller` (Firebird, chave-driven) e `cmd/agent` (watch em SRVIMPORT).

## Rodar local

### Em memória (sem banco, para smoke)
```bash
MAESTRO_JWT_SECRET=dev TRACKER_AGENT_SECRET=dev TRACKER_STORE=memory go run ./cmd/api
```

### Com Postgres
```bash
docker compose -f docker-compose.dev.yml up -d
go install github.com/pressly/goose/v3/cmd/goose@latest   # se ainda não tiver
DSN="postgres://tracker:tracker@localhost:5433/xml_tracker?sslmode=disable"
goose -dir migrations postgres "$DSN" up

TRACKER_STORE=postgres TRACKER_PG_DSN="$DSN" \
  MAESTRO_JWT_SECRET=dev TRACKER_AGENT_SECRET=dev go run ./cmd/api
```
API em `:8090` (configurável por `TRACKER_API_PORT`). `MAESTRO_JWT_SECRET` e
`TRACKER_AGENT_SECRET` são obrigatórios (fail-closed).

## Testes
```bash
go test ./...                                   # unitários + httptest (sem banco)
TRACKER_TEST_PG_DSN="$DSN" go test ./internal/store/   # + integração Postgres (aplica a migração)
```

## Variáveis de ambiente
| Var | Descrição |
|---|---|
| `MAESTRO_JWT_SECRET` | segredo HS256 do maestro (valida o JWT da UI) — obrigatório |
| `TRACKER_AGENT_SECRET` | segredo HMAC compartilhado com o agente (autentica `/ingest`) — obrigatório |
| `TRACKER_STORE` | `memory` (default) ou `postgres` |
| `TRACKER_PG_DSN` | DSN do Postgres (com `TRACKER_STORE=postgres`) |
| `TRACKER_API_PORT` | porta da API (default `8090`) |
