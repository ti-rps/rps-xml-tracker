# Backlog — rps-xml-tracker

Itens de trabalho futuro (escopo backend/API; **a UI do rps-maestro/maestro_web NÃO é mexida aqui**).

## Dashboard: separar etapas e renomear cards (decisão de produto pendente)

Hoje o overview soma `arrived + synced` num único card "Em trânsito" (`store.InTransit`).
O backend **já distingue** as duas etapas — `arrived` (na pasta `A Sincronizar`) e `synced`
(movido para `SINCRONIZADO`, aguardando importação) — e ambos já vão no payload do overview.

Pedido do usuário (2026-06-10): mostrar as etapas separadas (ex.: **"A Sincronizar"** = `arrived`
e **"Sincronizado"** = `synced`) e rever os rótulos (ex.: "Chegaram" → "Recebidas"/"Baixadas").

- A mudança visual é **frontend (maestro_web)** — fora deste repo, não fazer aqui.
- Avaliar do lado backend/API apenas se vale **renomear/clarificar campos** do overview no
  contrato (`design/openapi.yaml`) antes de a UI consumir. Decisão de produto: confirmar nomes.

## ✅ FEITO (2026-06-11) — selectState: 1 linha representativa por chave (multi-empresa)
# TABLISTACHAVEACESSO tem N linhas por chave (dona + terceiros). O Lookup antigo fazia OR das
# flags + 1ª linha não-vazia -> atribuía a nota a empresa arbitrária e um IGNORADA de terceiro
# encerrava a nota antes de o dono importar (caso real: chave da CLW mostrada como ROSEMBERG/
# ignorada). Novo selectState: (1) linha IMPORTADO=1 -> dona/imported; (2) senão pendente 0/0 ->
# em trânsito (poller emite seen_pending, NÃO termina); (3) senão tudo IGNORADA -> import_ignored.
# Desempate por menor CODIGOEMPRESA (determinístico). Provado contra o Firebird ao vivo.
# RETROATIVO: notas já import_ignored (terminais) não são re-polladas -> precisam de re-poll
# one-off p/ corrigir (re-emitir imported tem dedup_key diferente, então é aceito).
# -> FEITO: cmd/repoll (tracker-repoll) + store.ListChavesByStatus + poller.RepollImportIgnored.
# Roda 1x: re-polla as import_ignored, emite 'imported' p/ as que resolvem p/ a dona (IMPORTADO=1).
# As que resolvem p/ pendente NÃO são corrigíveis por append (import_ignored > pending_import) ->
# reportadas em StillPending p/ remoção manual. derive: nome da empresa agora é último-não-vazio
# (acompanha o código numa correção; antes era setIfEmpty -> ficava ROSEMBERG com código CLW).
# Prod: docker compose run --rm tracker-poller tracker-repoll

## ✅ FEITO (2026-06-11) — fix encoding Firebird (Latin-1 -> UTF-8)
# Em prod, com a opção 2 ligada, o poller passou a inserir muito mais linhas e quebrou com
# "invalid byte sequence for encoding UTF8: 0xc1..." (SQLSTATE 22021): o Firebird conecta com
# charset=NONE e devolve texto Latin-1 (0xC1='Á' etc.), inválido em UTF-8, derrubando o lote
# inteiro do ciclo. Fix: toUTF8() no poller (importObs + motivo) decodifica Latin-1 quando a
# string não é UTF-8 válida. Não toca no reader.go (refactor on-hold). Ciclos auto-recuperam
# (chaves seguem in-flight e voltam na rotação). Postgres não ficou com lixo (insert falhava).

## ✅ FEITO (2026-06-11) — pending_import REAL + latência sem backfill
# pending_import era status morto (só o branch default do derive; ninguém emitia). Agora:
# poller emite seen_pending (StageImport) quando a chave está na TABLISTACHAVEACESSO com
# IMPORTADO=0 e não-ignorada; novo Nota.PendingAt + coluna pending_at (migration 00005);
# precedência do derive vira imported>import_ignored>pending_import>synced>arrived;
# ListInflightChaves passa a pollar 'pending_import' (senão a nota nunca chega a imported).
# RECONCILIAR com o refactor on-hold do firebird (selectState caso "pendente" devolve 0/0 ->
# o default do poller emite seen_pending; coerente, mas os dois precisam subir juntos).
# Latência: percentis do overview agora usam JANELA MÓVEL de 30 dias (latencyWindow) sobre
# arrived_at/synced_at -> exclui backfill histórico que inflava p50/p95. UI: rotular "últimos 30 dias".

## ✅ FEITO (2026-06-11) — Drill-down por filial + bucket "Sem empresa" (decisões do maestro)
# Fix do 500: /empresas apontava p/ coluna inexistente `nome_empresa` (real: `empresa_nome`).
# /notas: +codigo_filial (AND com codigo_empresa) e +sem_empresa=true (codigo_empresa IS NULL).
# /empresas: removido WHERE codigo_empresa IS NOT NULL — notas sem empresa colapsam numa linha
# única (codigo_empresa/codigo_filial AUSENTES via omitempty) p/ fechar com o overview.
# sort=nome/total NÃO implementado (maestro ordena no cliente com limit=0). openapi atualizado.

## ✅ FEITO (2026-06-10 tarde) — Visão por EMPRESA + cards no contrato (itens 2 e 3)
# Implementado: nome_empresa + in_transit no EmpresaAgg; /empresas com sort (codigo|pendentes) e
# paginação (limit/offset, total real); EmpresaPendencias no openapi alinhado (flat) + params de
# /empresas e /notas (empresa, date_field); NotaStatus documentado (arrived=A Sincronizar,
# synced=SINCRONIZADO, etc.). A separação visual dos cards é do maestro_web (fora daqui).
# --- spec original abaixo ---
## Visão por EMPRESA — completar e documentar o contrato (backend/API only)

Objetivo (pedido 2026-06-10): além da visão por nota, uma visão **segregada por empresa** —
para cada empresa, quantas notas aguardando sincronização, aguardando importação, importadas, etc.
(mesmos status das notas, agregados por empresa). Escopo = só backend/API; a UI é do rps-maestro.
Meu papel: entregar os "ingredientes" e deixar EXPLÍCITO no contrato o que o maestro pode consumir.

Já existe (não refazer):
- `GET /empresas` (`handleEmpresas`) → lista `EmpresaAgg` (StatusCounts por empresa: arrived, synced,
  imported, import_ignored, pending_import, stuck, lost), com filtro `?pendentes=true`.
- Drill-down empresa→notas já funciona: `GET /notas?codigo_empresa=X&status=Y` (+ doc_type, empresa,
  cnpj, q, date_field/from/to, limit/offset).

Gaps a fazer:
- **`nome_empresa` no `EmpresaAgg`** (model.go) — hoje só tem `codigo_empresa`/`codigo_filial`. A UI
  precisa do nome; já temos via JOIN TABEMPRESAS (usado nas notas). Incluir no agregado.
- **`in_transit` por empresa** explícito (arrived+synced), espelhando o overview, em vez de a UI somar.
- **Ordenação + paginação** em `/empresas` (ex.: ordenar por pendentes desc; paginar se houver muitas
  empresas). Hoje retorna tudo sem ordem e `total=len(items)`.
- **(Opcional) latências p50/p95 por empresa**, paridade com o overview.
- **Documentar no `design/openapi.yaml`**: schema do `EmpresaAgg` (com nome/in_transit), query params de
  `/empresas` e os filtros de `/notas` — deixar o contrato explícito pro maestro montar a visão.

## ✅ FEITO (2026-06-10 tarde) — Latência chegada→sync irreal (agente carimba mtime, não detecção)

Sintoma (2026-06-10): muitas notas com `Chegada` e `Sincronização` no MESMO timestamp exato
(ex.: ambas `01:19:04`) → latência chegada→sync = 0s.

Causa: o agente usa `ObservedAt = info.ModTime()` (`agent.go`, em `parseToObservation`) para
TODAS as etapas. O mover de terceiros preserva o mtime ao mover `Xml_ASincronizar` →
`XML SINCRONIZADO`, então o arquivo de chegada e o de sincronizado têm o mesmo mtime → as duas
observações ficam com o mesmo horário. (A latência sync→import é confiável: vem do poller com
timestamp real de detecção do `IMPORTADO 0→1`.)

Correção (a fazer à tarde):
- Na etapa **sync**, usar a hora de **detecção** do agente (`a.now()` no momento do parse) como
  `ObservedAt`, em vez do mtime. Chegada continua com mtime (≈ quando a nota foi escrita).
- Resultado: latência chegada→sync vira positiva e real, com precisão de ±intervalo de scan (60s).
- Trade-off aceito: horário de sync = momento da detecção (até ~60s após o movimento real), não o
  instante exato do mover.
- Lembrar de atualizar o teste do agente e o comentário em `parseToObservation`.

## Fase de alertas: implementar `stuck` e `lost` (hoje sempre 0)

`derive.status()` só retorna `imported / import_ignored / synced / arrived / pending_import`.
`stuck` e `lost` existem no schema/contagens mas **nunca são produzidos** — os cards "Travadas"
e "Sumidas" mostram 0.

Para implementar (backend):
- **stuck**: nota em trânsito (`arrived`/`synced`) há mais que um SLA sem importar. Definir o SLA
  (ex.: > N horas) — provavelmente por doc_type/empresa.
- **lost**: vista na chegada e sumiu antes de importar. Definir critério de "sumiu" (ex.: deixou de
  ser observada por M tempo e nunca importou).
- Como `derive.Nota` é função pura sem relógio, decidir onde entra o "agora"/SLA (no derive com
  clock injetado, ou numa camada de avaliação no store/poller).
