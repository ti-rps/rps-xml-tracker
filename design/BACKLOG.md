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
