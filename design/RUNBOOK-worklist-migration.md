# Runbook — migrar o syncer de varredura (sweep) para worklist

Objetivo: parar de re-varrer os milhões de XML do `F:\Xml_ASincronizar` a cada
ciclo (o log do soak 17–20/07 mostrou `escaneados=50000 planejados=0` na
esmagadora maioria dos ciclos, ~29ms/arquivo, 2 wrap-arounds/dia só pra achar um
punhado de chegadas) e passar a agir por uma **lista de pendências** que o agent
já produziu (`arrived ∧ ¬synced`). Agent = olhos, syncer = mãos.

Estado atual (2026-07-20): serviço `RpsXmlTrackerSyncer` no SRVIMPORT roda em
**varredura** (modo real, sincronizador único, DownloadXML pausado desde 16/07).

---

## ⚠️ O que "migrar" realmente exige (não é só uma flag)

O serviço (kardianos) roda `program.cycle()`, que chama **`SweepOnce` hard-coded**
(`cmd/syncer/main.go`). Os modos `--worklist`/`--worklist-api` são **one-shot**:
rodam uma vez e saem (`return` antes de o serviço subir). Portanto o serviço, do
jeito que está, **não sabe rodar worklist em loop**. Há dois caminhos:

- **Opção A (recomendada, destino final):** pequeno ajuste de código para o
  `cycle()` rodar worklist quando `TRACKER_SYNCER_MODE=worklist`, e reinstalar o
  serviço. Mantém o modelo de serviço/heartbeat/auto-restart.
- **Opção B (interino, sem código):** rodar `syncer.exe --worklist-api` one-shot
  via **Agendador de Tarefas do Windows** a cada N min, e parar o serviço de
  varredura. Funciona **hoje** (com o #58 no ar), sem esperar código.

---

## Pré-requisitos (ambos os caminhos)

1. **PR #58 mergeado e a API redeployada** no SRVRPS03 (o endpoint
   `POST /api/v1/ingest/worklist` precisa existir). Conferir:
   ```bash
   # no SRVRPS03 — deve responder 400 "roots é obrigatório" (não 404):
   curl -s -X POST http://localhost:8090/api/v1/ingest/worklist \
     -H 'Content-Type: application/json' -d '{}' -H 'X-Agent-Signature: x' | head
   ```
   (401 assinatura inválida também confirma que a rota existe; 404 = API velha.)
2. **Binário do syncer atualizado** no SRVIMPORT (com `--worklist-api`).
3. **Allowlist definida**: `TRACKER_SYNCER_EMPRESAS` (csv de `codigo_empresa`). No
   soak ela estava **vazia** (todas as empresas) — worklist **exige** allowlist
   (não sincroniza tudo sem cerca). Escolher o conjunto do piloto.
4. Envs já usados pelo serviço servem: `TRACKER_API_URL`, `TRACKER_AGENT_SECRET`,
   `TRACKER_FB_DSN` (resolve empresa→CNPJ), `TRACKER_FB_WRITE_DSN` (escrita),
   `TRACKER_SYNCER_SYNC_ROOT`.

---

## Ensaio antes do cutover (dry-run, seguro)

Rodar UMA vez em dry-run e conferir que a worklist enxerga o que o sweep enxergava:
```powershell
# no SRVIMPORT, com os envs do serviço carregados:
.\syncer.exe --worklist-api --dry-run
# esperado: "worklist-api: N nota(s) pendente(s)... planejados=N executados=0"
```
Sanidade: `N` deve ser da ordem do backlog `arrived∧¬synced` da allowlist (dezenas
de milhares no total; muito menos por empresa). `planejados` ~ `N` menos os skips
(fora_da_janela, arquivo_sumiu, ja_processada). Se `N=0`, revisar a allowlist e a
janela de emissão (o fetch usa 1º dia do mês anterior como piso).

---

## Opção A — worklist como serviço (recomendada)

> Requer o ajuste de código: `cycle()` roda worklist quando
> `TRACKER_SYNCER_MODE=worklist` (senão, sweep — default, retrocompatível). Ainda
> **não** está no #58; abrir como mudança separada antes deste passo.

1. Parar e desinstalar o serviço atual (varredura):
   ```powershell
   .\syncer.exe stop
   .\syncer.exe uninstall
   ```
2. Reinstalar com o modo worklist (o install **captura os envs** do processo):
   ```powershell
   $env:TRACKER_SYNCER_MODE   = "worklist"
   $env:TRACKER_SYNCER_EMPRESAS = "<csv de codigo_empresa do piloto>"
   # ...demais envs já usados no install anterior (API_URL, AGENT_SECRET, DSNs, roots)...
   .\syncer.exe install
   .\syncer.exe start
   ```
3. Acompanhar o log: agora cada ciclo deve ser `WORKLIST fetched=… planejados=…
   executados=…` (sem `escaneados=50000`). O re-scan sumiu.

## Opção B — Task Scheduler (interino, sem código)

1. Parar o serviço de varredura (sem desinstalar, p/ rollback rápido):
   ```powershell
   .\syncer.exe stop
   ```
2. Criar tarefa agendada rodando o one-shot a cada 5 min (ajustar caminho/conta):
   ```powershell
   $act = New-ScheduledTaskAction -Execute "C:\rps-xml-tracker\syncer\syncer.exe" -Argument "--worklist-api" -WorkingDirectory "C:\rps-xml-tracker\syncer"
   $trg = New-ScheduledTaskTrigger -Once -At (Get-Date) -RepetitionInterval (New-TimeSpan -Minutes 5)
   Register-ScheduledTask -TaskName "RpsXmlTrackerWorklist" -Action $act -Trigger $trg -RunLevel Highest -User "SYSTEM"
   ```
   ⚠️ O one-shot lê os envs do **ambiente da tarefa** — garantir que
   `TRACKER_*` (API_URL, AGENT_SECRET, DSNs, EMPRESAS, SYNC_ROOT) estejam no
   escopo da conta/tarefa (ex.: variáveis de sistema), senão o `mustEnv` aborta.

---

## Validação (após o cutover, qualquer opção)

1. **Executados > 0 e erros = 0** nos primeiros ciclos (log do syncer).
2. **Audit** (Firebird, do dev ou SRVIMPORT):
   ```
   syncer.exe --audit --since <hoje>   # total/importadas devem subir
   ```
3. **Reconcile** (no SRVRPS03) — cobertura sem perda:
   ```bash
   docker compose exec tracker-poller tracker-repoll --reconcile --since <hoje> --limit 0 \
     | grep -oP 'estado no tracker: \K.*' | sort | uniq -c
   ```
   Esperado: sem `nunca vista`; `pending_import` só a cauda de entrada/multi-part.
   (Lembrar: o "faltando" é inflado por janela — ver reconcile-window-skew.)
4. **Comparar com o sweep**: a worklist deve sincronizar as MESMAS chegadas que a
   varredura sincronizaria — se alguma empresa da allowlist parar de aparecer,
   suspeitar do filtro CNPJ-base (checar `RootsForEmpresas` / `cnpj_emitente`).

---

## Rollback (voltar para varredura)

- **Opção A**: `stop` → `uninstall` → reinstalar SEM `TRACKER_SYNCER_MODE` (ou
  `=sweep`) → `start`. Volta ao comportamento do soak.
- **Opção B**: `Unregister-ScheduledTask -TaskName "RpsXmlTrackerWorklist"` e
  `.\syncer.exe start` (o serviço de varredura volta).
- O rollback é seguro: nenhum dos modos é destrutivo (INSERT marcador + move; o
  `syncer --rollback-all --yes` desfaz o que estiver IMPORTADO=0 se preciso).

---

## Depois: aposentar o DownloadXML

Só após a worklist rodar estável em produção e a validação bater por alguns dias.
O DownloadXML já está pausado desde 16/07 (o syncer cobre); "aposentar" é
formalizar (desabilitar o serviço/tarefa do DownloadXML) — passo separado, com o
dono da Athenas ciente.

## Follow-ups conhecidos (não bloqueiam o cutover)

- **Índice funcional** p/ o filtro CNPJ da worklist (a query não usa índice hoje):
  criar `CONCURRENTLY` — ver o corpo do PR #58. Fazer quando o volume/latência da
  worklist pedir.
- **Cruzar "arquivo existe"**: `arrived∧¬synced` superconta (o DownloadXML movia
  sem avisar o tracker); o skip `arquivo_sumiu` já distingue isso no log.
- **Itens 2 e 3** (fundir synced+pending_import; contagem inflada no derive) —
  trabalho de derive/UI, separado desta migração.
