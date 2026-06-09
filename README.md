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
- `cmd/poller` — fecha a etapa de **importação**: lê o Firebird do Athenas (read-only,
  chave-driven) e emite observações `imported`/`import_ignored`. `internal/firebird` é o leitor RO.
- `cmd/agent` — roda no SRVIMPORT: varre as pastas (read-only), parseia XML novos
  (`internal/xmlparse`), e envia observações assinadas (`internal/ingest`, HMAC + retry + spool).
  Estado em bbolt; backfill opcional. Etapas **chegada** e **sincronização**.

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

### Poller (Firebird)
```bash
TRACKER_FB_DSN='SYSDBA:masterkey@192.168.10.160:3050/e:\Athenas\rps.fdb?charset=NONE&auth_plugin_name=Legacy_Auth&wire_crypt=disabled' \
TRACKER_STORE=postgres TRACKER_PG_DSN="$DSN" TRACKER_POLL_INTERVAL=30s go run ./cmd/poller
```

### Agente (SRVIMPORT, Windows)
```bash
# cross-compilar o .exe para o SRVIMPORT:
GOOS=windows GOARCH=amd64 go build -o agent.exe ./cmd/agent
```
No SRVIMPORT (PowerShell), com as variáveis abaixo. 1º run com `BACKFILL=false` apenas semeia o
backlog (não emite); depois emite só o que chega.
```powershell
$env:TRACKER_API_URL="http://192.168.10.46:8090"
$env:TRACKER_AGENT_SECRET="<segredo HMAC>"
$env:TRACKER_AGENT_ARRIVAL_ROOT="F:\Xml_ASincronizar"
$env:TRACKER_AGENT_SYNC_ROOT="F:\XML SINCRONIZADO"
.\agent.exe
```

## Testes
```bash
go test ./...                                   # unitários + httptest (sem banco)
TRACKER_TEST_PG_DSN="$DSN" go test ./internal/store/    # + integração Postgres (aplica a migração)
# leitor + poller contra o Firebird real (read-only):
TRACKER_TEST_FB_DSN="..." TRACKER_TEST_FB_CHAVE="<chave importada>" go test ./internal/firebird/ ./internal/poller/
```

## Variáveis de ambiente
| Var | Descrição |
|---|---|
| `MAESTRO_JWT_SECRET` | segredo HS256 do maestro (valida o JWT da UI) — obrigatório |
| `TRACKER_AGENT_SECRET` | segredo HMAC compartilhado com o agente (autentica `/ingest`) — obrigatório |
| `TRACKER_STORE` | `memory` (default) ou `postgres` |
| `TRACKER_PG_DSN` | DSN do Postgres (com `TRACKER_STORE=postgres`) |
| `TRACKER_API_PORT` | porta da API (default `8090`) |
| `TRACKER_FB_DSN` | DSN do Firebird do Athenas (read-only) — usado pelo `cmd/poller` |
| `TRACKER_POLL_INTERVAL` | intervalo do poller (default `30s`) |
| `TRACKER_API_URL` | URL da API (agente) |
| `TRACKER_AGENT_ARRIVAL_ROOT` / `_SYNC_ROOT` | pastas observadas no SRVIMPORT |
| `TRACKER_AGENT_NAME` | nome do agente (default `SRVIMPORT`) → `source` da observação |
| `TRACKER_AGENT_STATE` / `_SPOOL` | arquivo de estado bbolt / pasta de spool |
| `TRACKER_AGENT_SCAN_INTERVAL` | intervalo de varredura (default `60s`) |
| `TRACKER_AGENT_BACKFILL` | `true` processa o backlog; `false` (default) só semeia |
