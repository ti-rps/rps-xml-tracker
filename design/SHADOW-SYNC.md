# Shadow-sync — plano técnico do piloto

> Status: proposta (2026-07-08), aprovada a premissa pelo dono da Athenas: o tracker
> pode mover ASINCRONIZAR → SINCRONIZADO e inserir na TABLISTACHAVEACESSO com
> IMPORTADO=0; o AthenasHorse segue dono da importação.
>
> **F0 — CONCLUÍDA (2026-07-09):** código (`internal/syncpath`,
> `internal/firebird/investigate.go`, modos `repoll --profile-insert /
> --watch-chave / --check-path`) + investigação rodada contra o Firebird de prod.
> Entregável em **`design/SHADOW-SYNC-F0-ACHADOS.md`**: PK via generator
> `GEN_CHAVEACESSOXML`, INSERT mínimo, derivação de URL validada em 5.000 URLs
> (cnpj/competência/arquivo/direção 100% NFe/NFCe/CTe; empresa 98%),
> multi-participação = 12% (uma cópia por empresa/filial → M0 confirmado),
> gotcha da trigger `CHECK_FORCAIMPORTACAO`, e o achado colateral DATAROBO=NULL.
>
> **M0 — CONCLUÍDA (2026-07-09):** migração 00015 (`nota_empresa`, vazia no boot —
> backfill go-forward via recompute), poller emite observação POR PARTICIPAÇÃO
> (dedup_key ganha empresa/filial), derive agrega ("importada 1/2" fica
> pending_import até TODAS terminarem), `GET /notas/{chave}` ganha
> `participacoes[]`. Reconcile migrado de status→`KnownImported` (imported_at),
> senão "1/2" viraria faltante eterna. Filtros de LISTA seguem na `notas`
> (representante) até o backfill retroativo — follow-up documentado abaixo.
> Notas ao deploy: (1) a 1ª reemissão de seen_pending pós-deploy é aceita de novo
> (chave de dedup nova) — ruído único e inofensivo na timeline; (2) notas já
> terminais NÃO reabrem (recompute só acontece em observação nova aceita).
>
> **F1 — código implementado (2026-07-09):** `internal/syncer` (plano+execução
> por participação, journal bbolt, retomada de crash, conflito nunca sobrescreve),
> `cmd/syncer` (serviço Windows kardianos; trava TRACKER_SYNCER_ENABLED; modos
> --dry-run/--chave+--file/--once; heartbeat "syncer" no GET /status),
> `internal/firebird/writer.go` (TRACKER_FB_WRITE_DSN, GEN_ID, INSERT da F0 sem
> TIPODOCUMENTO/TIPO, UTF-8→Latin-1), eventos sync_* no derive (só
> file_moved/sync_moved progridem). Gatilho single-key exige `--file` (o nome do
> arquivo não contém a chave; pegar o file_path da observação de chegada).
> syncpath aceita CPF (11 díg.) de produtor rural. Falta: rodar o dry-run dias no
> SRVIMPORT e comparar os planos (syncer-plans.jsonl) com o que o DownloadXML
> fizer (via --check-path) antes do piloto F2.

## 0. Pré-requisito de modelagem: participação por empresa (nota_empresa)

**Problema (levantado pelo usuário, confirmado no código):** uma mesma chave pode
envolver DUAS empresas clientes — emitente (saída para A) e destinatário (entrada
para B) — cada uma com seu próprio ciclo de importação no Athenas. A
TABLISTACHAVEACESSO já modela isso (uma linha por empresa/chave), mas o tracker
COLAPSA: `selectState` (reader.go) elege uma linha representante (importou >
pendente > ignorada, menor CODIGOEMPRESA desempata) e a `notas` guarda UMA
empresa, UMA direction, UM status.

**Ponto cego concreto do modelo atual:** A importou, B ainda não → nota vira
`imported` (terminal) → poller PARA de acompanhar → a importação de B nunca é
registrada. O caso CLW/ROSEMBERG foi sintoma disso; o repoll --reconcile pode
estar contando como "ok" notas cuja segunda participação está pendida.

**Modelagem alvo (M0):**
- `notas` — fatos da CHAVE (imutáveis): chave, doc_type, emitente, destinatário,
  data de emissão, valor, arrived_at (a chegada é do arquivo, não da empresa).
- `nota_empresa` — uma linha por PARTICIPAÇÃO: chave, codigo_empresa,
  codigo_filial, papel (emitente|destinatario), direction (saida|entrada),
  status Athenas próprio (pending/imported/ignored + motivo + imported_at),
  synced_at/url próprios (o SINCRONIZADO tem UMA CÓPIA POR EMPRESA — confirmar
  em F0 comparando URLs das linhas irmãs; as "cópias na timeline da EMPRESA
  TESTE" já sugeriam isso).
- Status da nota vira agregado das participações ("importada 1/2") e o
  in-flight do poller passa a ser por participação — só sai do radar quando
  TODAS as participações terminam.

**Mudanças:** migração (tabela nova + backfill go-forward; retroativo por
re-poll janelado/on-demand — 14,3M linhas, não redigerir tudo); poller emite
observação POR PARTICIPAÇÃO (dedup_key ganha empresa; `ImportState.Rows` já
carrega as linhas todas — hoje jogamos fora); derive agrupa import/sync por
empresa; API: `GET /notas/{chave}` ganha `participacoes: []` (filtros de
empresa/direção passam a olhar participação); UI mostra as participações.

**Por que é pré-requisito do syncer (não do F0):** a unidade de trabalho da
sincronização é (chave, empresa) — para nota entre dois clientes o syncer copia
o arquivo para a pasta de CADA participante e insere UMA LINHA POR EMPRESA
(comportamento do DownloadXML a confirmar em F0). Sem M0, as observações do
syncer não teriam onde ancorar a segunda participação. Sequência: F0
(investigação, mede inclusive a prevalência do multi-participação) → M0
(modelagem) → F1 (syncer).

## 1. Arquitetura proposta

**Novo componente `cmd/syncer`, rodando no SRVIMPORT ao lado do agente (Opção B),
com o move E o INSERT no mesmo processo.** Justificativa contra as alternativas:

- **Não expandir o agente (A):** o agente é o observador read-only e a fonte de
  verdade do rastreio. Se ele mesmo sincroniza, perde-se a verificação
  independente — hoje, se o syncer mover um arquivo, o AGENTE enxerga o arquivo
  aparecer no SINCRONIZADO e emite `file_moved` por conta própria, e o POLLER
  enxerga a linha IMPORTADO=0 e emite `seen_pending`. Ou seja: mantendo os três
  separados, cada efeito do syncer é confirmado por um componente que não sabe
  que o syncer existe. Isso é o mecanismo de validação do piloto.
- **Poller não sincroniza (D):** move em F:\ a partir do SRVRPS03 exigiria o mount
  CIFS (credencial pendente) e criaria falha parcial coordenada entre 2 máquinas.
- **API orquestra (C):** fica para a fase 2 — a API ganha uma fila de intenções
  (`POST /sync-requests`) que o syncer consome via polling HTTP (mesmo HMAC do
  ingest). É o caminho para disparar sync pela UI do maestro sem a API tocar
  filesystem/Firebird. No piloto, o gatilho é local (flag `--chave`).

O syncer é o único dono da sequência move+insert (raciocínio de falha simples),
reusa os padrões existentes: serviço Windows kardianos (como o agente), estado
local bbolt (journal), cliente `internal/ingest` com HMAC+spool para reportar
observações, e conexão Firebird do `internal/firebird` — porém com **DSN separado
de escrita** (`TRACKER_FB_WRITE_DSN`), para o poller continuar com credencial
read-only e o raio de dano da credencial de escrita ficar restrito ao syncer.

## 2. Fluxo exato da sincronização (uma chave)

Ordem pensada para que QUALQUER falha no meio deixe o sistema num estado seguro
(arquivo nunca some antes de estar garantido no destino + registrado no banco):

```
0. candidato        arquivo em ASINCRONIZAR (flag --chave no piloto)
1. parse            xmlparse → chave, docType, emit/dest, dhEmi  (falhou? skip)
2. resolve          Firebird RO: TABFILIAL (CNPJ→empresa/filial), TABEMPRESAS (nome)
                    direção = raiz do CNPJ da filial vs emit/dest (lógica já usada
                    no repoll --backfill-direction)
3. deriva URL       internal/syncpath (função pura) → caminho relativo
4. pre-check        idempotência: destino já existe? linha já existe na
                    TABLISTACHAVEACESSO p/ (CHAVEACESSO, empresa, filial)?
                    → se ambos: só remover a origem (retomada de crash)
5. journal          bbolt: chave → estado "planned" (payload: origem, destino)
6. copy             origem → destino com sufixo .tracker-tmp (cria diretórios)
7. verify           re-lê o .tracker-tmp: mesmo tamanho + mesma chave no parse
8. rename           .tracker-tmp → <CHAVE>.xml (atômico no mesmo volume NTFS)
                    journal: "moved" | observação: sync_moved (stage sync)
9. INSERT           TABLISTACHAVEACESSO com IMPORTADO=0, URL=caminho relativo,
                    marcador do tracker (ver §6) | journal: "inserted"
                    observação: sync_db_inserted (stage sync, não-progresso)
10. delete origem   remove de ASINCRONIZAR | journal: "done"
11. (validação)     agente emite file_moved sozinho; poller emite seen_pending
                    sozinho; AthenasHorse importa; poller emite imported.
```

**Multi-participação (ver §0):** os passos 2–9 rodam POR PARTICIPAÇÃO — para
nota entre dois clientes: deriva-se um destino por empresa (direção de cada
uma), copia-se para os dois, insere-se uma linha por empresa. O passo 10
(delete da origem) só roda depois de TODAS as participações completarem 9. O
journal guarda estado por (chave, empresa). Piloto começa com notas de
participação única (a allowlist single-key permite escolher).

Falhas e retomada (journal bbolt dirige o resume no restart):
- falha antes do rename (6–7): lixo `.tracker-tmp` no destino; origem intacta.
  Retry limpa o tmp e recomeça. DownloadXML nunca disputa o `.tracker-tmp`.
- falha entre rename e INSERT (9): arquivo no destino, sem linha → AthenasHorse
  não importa ainda; retry só refaz o INSERT (pre-check vê o destino ok).
- falha no delete da origem (10): arquivo nos dois lados + linha ok → retry só
  deleta. (Se o DownloadXML pegar a origem nesse meio-tempo, o destino tem o
  mesmo nome `<CHAVE>.xml` — sobrescrita de conteúdo idêntico — e a linha extra
  que ele inserir é o comportamento normal da tabela, que já tem múltiplas
  linhas por chave; o poller já lida com isso.)
- **nunca** sobrescrever destino existente com conteúdo diferente (hash difere →
  `sync_failed` motivo "conflito", intervenção manual).
- **nunca** deletar a origem sem destino verificado + linha presente.

## 3. Segurança / idempotência / modos

- `TRACKER_SYNCER_ENABLED` (default **false**) — flag geral; sem ela o binário sai.
- `--dry-run` — executa 0–4 e loga o plano completo (origem, destino, INSERT que
  faria); nenhuma escrita em lugar nenhum. Modo default do piloto.
- `--chave <44>` — sincroniza SÓ essa chave (single-key). Sem `--chave`, modo
  varredura exige allowlist não-vazia.
- `TRACKER_SYNCER_EMPRESAS` (allowlist CODIGOEMPRESA) e/ou
  `TRACKER_SYNCER_DIRS` (allowlist de subpastas da ASINCRONIZAR) +
  `TRACKER_SYNCER_MAX_PER_CYCLE` (piloto: 1).
- **Janela do AthenasHorse (diagnóstico 2026-07-09):** o importador só importa
  notas com EMISSÃO no mês atual ou anterior. Sincronizar nota com emissão mais
  velha cria linha IMPORTADO=0 eterna (lixo). O syncer NÃO sincroniza fora da
  janela por default (`--allow-stale` p/ exceções conscientes); confirmar a
  regra exata em F0.
- Idempotência: pre-check (arquivo destino + SELECT por chave/empresa/filial) +
  journal bbolt + INSERT só depois de conferir que não existe linha NOSSA para a
  chave. Corrida com o DownloadXML mitigada por: piloto single-key, allowlist
  disjunta do que ele processa, e a partir de 15/07 ele desligado nos testes.
- Logs: uma linha por transição de estado por chave (padrão do agente), com o
  plano completo no dry-run.

## 4. Observações novas no tracker

Novos event types (stage `sync`):

| evento | quando | efeito no derive |
|---|---|---|
| `sync_moved` | rename concluído | marca SyncedAt (progresso, como file_moved) |
| `sync_db_inserted` | INSERT ok | só timeline (não-progresso) |
| `sync_failed` | qualquer falha (payload: passo + erro) | só timeline (não-progresso) |

**Mudança necessária no `derive`**: hoje QUALQUER observação de stage sync seta
`SyncedAt` (derive.go, case StageSync). Passa a setar só em
`file_moved`/`sync_moved`; os demais aparecem na timeline sem mudar o status.
`sync_attempted` não vira observação (vira log) — observação só para fato
consumado ou falha, senão polui a timeline. O `seen_pending`/`imported`
continuam vindo do POLLER (verificação independente; não emitimos import).

## 5. Queries de investigação da TABLISTACHAVEACESSO (fase 0, read-only)

Generator/trigger da PK (não há identity no DDL — Firebird clássico usa
generator + trigger BEFORE INSERT, OU o app faz GEN_ID client-side):

```sql
SELECT RDB$TRIGGER_NAME, RDB$TRIGGER_INACTIVE, RDB$TRIGGER_SOURCE
FROM RDB$TRIGGERS
WHERE RDB$RELATION_NAME = 'TABLISTACHAVEACESSO'
  AND COALESCE(RDB$SYSTEM_FLAG, 0) = 0;

SELECT RDB$GENERATOR_NAME FROM RDB$GENERATORS
WHERE COALESCE(RDB$SYSTEM_FLAG,0)=0 AND RDB$GENERATOR_NAME CONTAINING 'CHAVE';
```

Se houver trigger ativa que faz `IF (NEW.CODIGO_CHAVEACESSO IS NULL) THEN ... GEN_ID`,
inserimos com a PK nula e ela resolve; senão, fazemos
`SELECT GEN_ID(<GENERATOR>, 1) FROM RDB$DATABASE` e passamos explícito.

Perfil do que o DownloadXML preenche (fill-rate por coluna nas linhas recentes):

```sql
SELECT COUNT(*) AS total,
       COUNT(SERIE) AS serie, COUNT(NUMERODOCUMENTO) AS numerodocumento,
       COUNT("DATA") AS data_, COUNT(DOWNLOAD) AS download,
       COUNT(SITUACAO) AS situacao, COUNT(URL) AS url,
       COUNT(DATAEMISSAO) AS dataemissao, COUNT(ORIGEM) AS origem,
       COUNT(VALORTOTAL) AS valortotal, COUNT(TIPO) AS tipo,
       COUNT(TIPODOCUMENTO) AS tipodocumento, COUNT(CNPJEMITENTE) AS cnpjemitente,
       COUNT(CNPJDESTINATARIO) AS cnpjdestinatario, COUNT(EMITENTE) AS emitente,
       COUNT(DESTINATARIO) AS destinatario, COUNT(CAMINHOORIGINAL) AS caminhooriginal,
       COUNT(DATAINCLUSAO) AS datainclusao, COUNT(DATADEENTRADA) AS datadeentrada,
       COUNT(HORAEMISSAO) AS horaemissao, COUNT(CODIGOTIPOMOVIMENTO) AS codtipomov
FROM TABLISTACHAVEACESSO
WHERE DATAINCLUSAO >= CURRENT_DATE - 7;
```

(estender a TODAS as colunas na ferramenta; e o mesmo agrupado por
TIPODOCUMENTO p/ ver diferenças NFe/NFCe/CTe e por TIPO p/ entrada/saída)

Amostra crua para diff manual + observação de uma nota específica:

```sql
SELECT FIRST 20 * FROM TABLISTACHAVEACESSO
WHERE DATAINCLUSAO >= CURRENT_DATE ORDER BY CODIGO_CHAVEACESSO DESC;

SELECT * FROM TABLISTACHAVEACESSO WHERE CHAVEACESSO = '<chave>';
```

Prevalência do multi-participação (dimensiona o §0 — quantas chaves têm 2+
empresas clientes, e em que combinação de status):

```sql
-- quantas chaves recentes têm mais de uma empresa
SELECT COUNT(*) FROM (
  SELECT CHAVEACESSO FROM TABLISTACHAVEACESSO
  WHERE DATAINCLUSAO >= CURRENT_DATE - 30
  GROUP BY CHAVEACESSO
  HAVING COUNT(DISTINCT CODIGOEMPRESA) > 1
);

-- combinações de status entre as participações (importada+pendente é o ponto cego)
SELECT SUM(CASE WHEN IMPORTADO=1 THEN 1 ELSE 0 END) AS importadas,
       SUM(CASE WHEN IMPORTADO=0 AND COALESCE(IMPORTACAOIGNORADA,0)=0 THEN 1 ELSE 0 END) AS pendentes,
       SUM(CASE WHEN COALESCE(IMPORTACAOIGNORADA,0)=1 THEN 1 ELSE 0 END) AS ignoradas,
       COUNT(*) AS participacoes
FROM TABLISTACHAVEACESSO
WHERE DATAINCLUSAO >= CURRENT_DATE - 30
GROUP BY CHAVEACESSO
HAVING COUNT(DISTINCT CODIGOEMPRESA) > 1;   -- agregar no cliente por combinação

-- as URLs das linhas irmãs divergem? (uma cópia física por empresa?)
SELECT FIRST 20 t.CHAVEACESSO, t.CODIGOEMPRESA, t.TIPO, t.URL
FROM TABLISTACHAVEACESSO t
WHERE t.CHAVEACESSO IN (SELECT CHAVEACESSO FROM TABLISTACHAVEACESSO
                        WHERE DATAINCLUSAO >= CURRENT_DATE - 7
                        GROUP BY CHAVEACESSO
                        HAVING COUNT(DISTINCT CODIGOEMPRESA) > 1)
ORDER BY t.CHAVEACESSO, t.CODIGOEMPRESA;
```

Janela de importação do AthenasHorse (fiscal diz: emissão mês atual + anterior;
confirmar empiricamente) e assinatura dos "descartes silenciosos":

```sql
-- quantos meses entre emissão e importação, nas importadas (confirma a janela)
SELECT (EXTRACT(YEAR FROM DATAROBO)*12 + EXTRACT(MONTH FROM DATAROBO))
     - (EXTRACT(YEAR FROM DATAEMISSAO)*12 + EXTRACT(MONTH FROM DATAEMISSAO)) AS meses,
       COUNT(*)
FROM TABLISTACHAVEACESSO
WHERE IMPORTADO = 1 AND DATAROBO >= CURRENT_DATE - 180
GROUP BY 1 ORDER BY 1;

-- pendentes DENTRO da janela vs importadas: alguma coluna separa os descartes
-- (Simples Nacional, devolução...)? comparar SITUACAO, CODIGOTIPOMOVIMENTO,
-- SEMDEPARA, CODIGOTIPOCONTABIL, TIPO entre os dois grupos
SELECT IMPORTADO, SITUACAO, CODIGOTIPOMOVIMENTO, SEMDEPARA, COUNT(*)
FROM TABLISTACHAVEACESSO
WHERE DATAEMISSAO >= CURRENT_DATE - 60 AND COALESCE(IMPORTACAOIGNORADA,0)=0
GROUP BY 1,2,3,4 ORDER BY 5 DESC;
```

Valores distintos dos campos de semântica desconhecida:

```sql
SELECT ORIGEM, COUNT(*) FROM TABLISTACHAVEACESSO
WHERE DATAINCLUSAO >= CURRENT_DATE - 30 GROUP BY ORIGEM;
-- idem p/ SITUACAO, DOWNLOAD, TIPO, TIPODOCUMENTO, CODIGOTIPOMOVIMENTO, ORDEMATHENAS
```

## 6. Como descobrir o INSERT mínimo compatível

Ferramenta de investigação: **novos modos read-only no `cmd/repoll`** (que já tem
o ferramental de conexão FB+PG e roda em prod via `docker compose run`):

- `--profile-insert`: roda o perfil de fill-rate acima (todas as colunas, janela
  `--since`, quebras por TIPODOCUMENTO/TIPO) e imprime relatório → nos diz o
  conjunto de colunas que o DownloadXML SEMPRE preenche (candidato a INSERT
  mínimo) vs às vezes vs nunca.
- `--watch-chave <chave>`: faz polling da chave a cada 15–30s e imprime a linha
  inteira quando aparecer/mudar (coluna a coluna, com diff entre snapshots).
  Uso: escolher uma nota fresca na ASINCRONIZAR, rodar o watch, deixar o
  DownloadXML sincronizá-la e capturar exatamente o que ele gravou no INSERT
  e o que o AthenasHorse muda depois (IMPORTADO 0→1, OBSERVACOESIMPORTACAO...).
- Trigger/generator: `--profile-insert` também roda as queries de RDB$ acima.

Regras do INSERT que vamos montar com esse resultado:
- preencher o que o DownloadXML preenche sempre (esperado: CHAVEACESSO,
  CODIGOEMPRESA, CODIGOFILIAL, CNPJEMITENTE, CNPJDESTINATARIO, EMITENTE,
  DESTINATARIO, DATAEMISSAO, VALORTOTAL, TIPO, TIPODOCUMENTO, URL, DATAINCLUSAO,
  IMPORTADO=0 — confirmar com dados);
- **marcador de autoria**: gravar `OBSERVACOES = 'sync rps-xml-tracker vX.Y'`
  (ou um valor próprio de ORIGEM, se a investigação mostrar que ORIGEM é um
  enum de fonte — perguntar à Athenas qual valor é seguro). Sem marcador não há
  rollback limpo nem auditoria de "quem sincronizou o quê";
- **charset**: a conexão é charset=NONE e o banco fala Latin-1 — TUDO que
  escrevermos (EMITENTE, URL com nome de empresa acentuado...) precisa ser
  transcodificado UTF-8 → Latin-1 (o inverso do toUTF8 do poller), senão
  gravamos mojibake que o Athenas exibe errado;
- teste inicial: 1 INSERT de uma chave controlada num horário calmo, seguido de
  `--watch-chave` até o AthenasHorse importá-la.

## 7. Derivação do caminho URL (`internal/syncpath`, função pura)

Hipótese (do exemplo real):
`\<NOME_EMPRESA>\<CNPJ_FILIAL_14>\<TIPODOC>\<ENTRADA|SAIDA>\<AAAAMM>\<CHAVE>.xml`

Perguntas em aberto e como cada uma se responde EMPIRICAMENTE, sem depender da
Athenas: novo modo `repoll --check-path` pega N linhas recentes com URL
preenchida, roda a nossa derivação com os dados da própria linha + TABEMPRESAS/
TABFILIAL, e compara segmento a segmento com a URL real. Relatório: % de acerto
por segmento + exemplos das divergências. Isso responde:
- 1º segmento = TABEMPRESAS.NOME? (ou nome fantasia/campo de diretório);
- 2º segmento = CNPJ da FILIAL dona (e não do emitente) — no exemplo coincidem
  porque NFCe de saída é emitida pela própria empresa; ENTRADA é quem separa;
- 3º = TIPODOCUMENTO (mapear NFe/NFCe/CTe/NFSe da nossa classificação);
- 4º = TIPO E/S → ENTRADA/SAIDA;
- 5º = AAAAMM de DATAEMISSAO ou de DATAINCLUSAO (casos de virada de mês decidem);
- eventos/CCe/substitutas: ver como aparecem nas URLs reais (TPEVENTO,
  CHAVEACESSOSUBS preenchidos) — se forem outro padrão, piloto NÃO os cobre
  (allowlist só NFe/NFCe "normais"); DocEvento fica fora do escopo;
- CAMINHOORIGINAL: preencher com o caminho de origem na ASINCRONIZAR se o
  fill-rate mostrar que o DownloadXML preenche; senão NULL.

Detalhes da função: sanitização de nome p/ NTFS (caracteres inválidos, ponto/
espaço final), caminho relativo com `\` e prefixo `\` como no exemplo, e a
assinatura `Derive(parse xmlparse.Result, emp EmpresaInfo, dir Direction) (rel
string, err error)` com tabela de testes usando URLs reais coletadas no
`--check-path`. Meta de aceite do piloto: ≥99% de match nos últimos 30 dias
para NFe/NFCe; o 1% divergente vira caso de teste documentado.

## 8. Piloto com uma única chave (roteiro)

1. (fase 0 concluída: INSERT mínimo conhecido, derivação ≥99%, generator resolvido)
2. Escolher a cobaia: nota NFCe/NFe de empresa combinada (candidata natural: a
   EMPRESA TESTE já usada nas cópias — confirmar que o AthenasHorse a importa)
   recém-chegada na ASINCRONIZAR e que o DownloadXML ainda não pegou.
3. `syncer --dry-run --chave <chave>` no SRVIMPORT → conferir o plano impresso
   (destino derivado vs onde o DownloadXML colocaria; INSERT completo).
4. `repoll --watch-chave <chave>` rodando em paralelo (SRVRPS03).
5. `syncer --chave <chave>` (ENABLED=true) → executa o fluxo do §2.
6. Verificar, na ordem: arquivo no destino certo e fora da origem (filesystem);
   linha na TABLISTACHAVEACESSO com nossos campos (watch); timeline no tracker
   com `sync_moved`+`sync_db_inserted` do syncer E `file_moved` do agente;
   depois `seen_pending` do poller.

## 9. Validar que o AthenasHorse importou

Nada novo a construir — é exatamente o que o tracker já faz:
- o poller detecta IMPORTADO 0→1 e emite `imported` (timeline completa:
  arrival → sync(syncer) → pending → imported);
- `repoll --reconcile` audita depois em lote (a acurácia já validada em 100%);
- painel humano: o fiscal confere a nota no Athenas (TABENTRADASAIDA/livro).
Critério de sucesso do piloto: a cobaia chega a `imported` sem NENHUMA
intervenção e sem divergência no reconcile; o fiscal acha o XML na pasta.

## 10. Rollback manual (documentado, por chave)

Vale apenas enquanto `IMPORTADO=0`. Se já importou, não há rollback técnico —
é estorno fiscal no Athenas (fora do escopo).

```
1. DELETE FROM TABLISTACHAVEACESSO
   WHERE CHAVEACESSO='<chave>' AND IMPORTADO=0
     AND OBSERVACOES STARTING WITH 'sync rps-xml-tracker';   -- só a NOSSA linha
2. mover o arquivo de volta: copiar destino → ASINCRONIZAR (nome original,
   registrado no journal/observação sync_moved payload) e apagar o destino.
3. tracker: as observações são append-only; emitir manualmente (ou via flag do
   syncer `--rollback --chave`) um sync_failed com motivo "rollback manual"
   para a timeline contar a história. A nota volta a 'arrived' quando o agente
   a revir na ASINCRONIZAR... (na prática o status derivado já fica correto no
   próximo derive, pois synced_at deriva das observações existentes — o
   registro do rollback é para auditoria humana).
```

**IMPLEMENTADO (2026-07-15):** `syncer --rollback <chave> --yes` executa 1–3
sozinho — única operação destrutiva do syncer. Roda sempre em modo REAL (precisa
do `TRACKER_FB_WRITE_DSN` p/ o DELETE) e exige `--yes` além do `TRACKER_SYNCER_ENABLED`.
O DELETE filtra por `IMPORTADO=0` + `OBSERVACOES STARTING WITH 'sync
rps-xml-tracker'` (nunca toca linha do DownloadXML nem já importada); restaura a
origem a partir da 1ª cópia íntegra (via journal) e apaga os destinos; emite
`sync_failed` "rollback manual" por participação. Se já importou, o filtro
`IMPORTADO=0` protege e o resultado reporta 0 linhas apagadas (aí é estorno
fiscal, fora do escopo). Testes: `syncer.TestRollback_DesfazSync`,
`firebird` (DeleteOurRows). Se não houver journal (ex.: outra máquina), faz só o
DELETE e avisa que o arquivo é manual.

## 11. Arquivos/módulos alterados, por fase

**F0 — investigação (read-only, zero risco, dá pra começar já):**
- `cmd/repoll`: modos `--profile-insert`, `--watch-chave`, `--check-path`
- `internal/firebird`: SELECTs novos de apoio (RDB$, perfil, linha completa,
  prevalência multi-participação)
- `internal/syncpath` (novo): derivação pura + testes com URLs reais
- Entregável: relatório do INSERT mínimo + % de acerto da derivação + decisão
  do marcador (OBSERVACOES vs ORIGEM, confirmar com a Athenas) + prevalência e
  comportamento do DownloadXML no multi-participação (uma cópia por empresa?)

**M0 — modelagem participação por empresa (§0), antes do syncer:**
- migração `nota_empresa` + backfill go-forward
- `internal/firebird`: expor as linhas todas (hoje `ImportState.Rows` é jogado fora)
- `internal/poller`: observação por participação (dedup_key com empresa);
  in-flight por participação (nota só sai do radar com TODAS terminais)
- `internal/derive` + `internal/model`: participações + status agregado
- API/UI: `participacoes` no detail, filtros por participação
- Entregável independente do shadow-sync: corrige o ponto cego
  "A importou, B pendente" que existe HOJE

**F1 — syncer dry-run (escreve só log/journal):**
- `internal/syncer` (novo): state machine + journal bbolt + pre-checks
- `cmd/syncer` (novo): serviço Windows (padrão kardianos do agente), flags/envs
- `internal/firebird/writer.go` (novo): conexão TRACKER_FB_WRITE_DSN,
  InsertChaveAcesso (transcodificação Latin-1), GEN_ID
- `internal/model`: consts `sync_moved`/`sync_db_inserted`/`sync_failed`
- `internal/derive`: StageSync progride só com file_moved/sync_moved
- Rodar dry-run no SRVIMPORT alguns dias comparando o plano com o que o
  DownloadXML faz de verdade (diff automático possível: dry-run grava o plano,
  check-path compara com a linha que o DownloadXML criou depois)

(sequência: F0 → M0 → F1 — o syncer nasce com unidade de trabalho
(chave, empresa) ancorada nas participações)

**F2 — piloto single-key (§8), com DownloadXML ainda ligado (cobaia controlada)**

**F3 — allowlist com DownloadXML desligado (a partir de 15/07/2026):**
empresa piloto → grupo de empresas → tudo; `TRACKER_SYNCER_MAX_PER_CYCLE`
crescendo; o reconcile e o /metrics/latency medem o resultado (a latência
chegada→sync deve desabar de p50 45h para minutos).

**Fase posterior (fora do piloto):** fila de intenções na API + botão na UI do
maestro; NFSe; eventos/CCe.

## 12. Pendências e correções conhecidas (registradas 2026-07-10, pré-F2)

**12.1 Participações-lixo de contas SIEG "pega-tudo" (EMPRESA TESTE/ROSEMBERG) —
CORREÇÃO APROVADA EM CONCEITO, NÃO IMPLEMENTADA:**
O DownloadXML cria linha na TABLISTACHAVEACESSO para cada CÓPIA que encontra,
inclusive das contas SIEG que baixam XML de todo o escritório: ROSEMBERG (120) e
EMPRESA TESTE (996), ambas com CPF 55283390578 que não é parte de nota nenhuma
(e RPS SERVICOS 52 em menor grau). Essas linhas ficam IMPORTADO=0 eternas em
quase toda nota — é a fonte do caso CLW/ROSEMBERG e de boa parte das pendentes
eternas. Confirmado ao vivo na chave 29260706129109000100550010002015071463906718
(POSTO DO TAXISTA + DAMASCO importadas; ROSEMBERG + EMPRESA TESTE pendentes).
Dois efeitos no tracker pós-M0:
- **regressão de exibição**: a empresa da nota usa "última observação não-vazia
  vence"; como a participação-lixo re-emite seen_pending por último, ela rouba o
  rótulo (nota aparece como "EMPRESA TESTE" com emitente/destinatário reais);
- **terminalidade corroída**: participação-lixo nunca importa → status agregado
  fica pending_import para sempre → in-transit inflado.
Correção proposta (derive, 2 pontos): (1) atribuição da nota (empresa/nome/
direção) passa a derivar das PARTICIPAÇÕES com a precedência do selectState
(importada > pendente, menor código), ignorando as sem direção; (2) participação
NÃO-PARTE (direction vazia = CNPJ/CPF da filial não casa emitente nem
destinatário) não segura a terminalidade — continua listada no detail por
transparência. Trade-off documentado: participação legítima com CNPJ faltante no
cadastro também tem direção vazia e deixaria de segurar a nota (raro; mesmo
ponto cego pré-M0). Testes devem usar o caso POSTO DO TAXISTA acima.
**Correção de raiz (operacional, fora do tracker):** arrumar as contas SIEG de
ROSEMBERG/EMPRESA TESTE que baixam XML de todo mundo.

**12.2 Syncer — pular a cópia quando a linha já existe (refinamento pré-modo
real):** quando HasRow(chave, empresa, filial) é true ANTES de qualquer move
(nota já sincronizada por outra cópia), pular também a CÓPIA da participação —
hoje o syncer copiaria para a pasta derivada se o destino não existir (ex.:
pasta antiga com outro nome), gerando arquivo duplicado. Com isso, no backlog de
cópias duplicadas o syncer age como FAXINEIRO (verifica e remove a origem).

**12.3 Cadastro da empresa como gate do Horse (melhoria pós-piloto):** o Horse
só importa o que o cadastro da empresa permite (tipos de movimento). O
DownloadXML insere sem consultar (daí parte das pendentes eternas); o syncer,
por paridade, também. Melhoria: descobrir a tabela de config e pular
participação não-importável; casa com o status terminal novo p/ pendentes stale.

**12.4 Anomalia rara de URL com segmento de DIA** (`...\202606\08\chave.xml`,
1 em ~40): fora do padrão derivado; o conflito-check impede sobrescrita. Só
observar a prevalência no check-plans completo.

**12.5 imported_at à meia-noite (DATAROBO morta desde 2022) — CORREÇÃO
PROPOSTA, NÃO IMPLEMENTADA:** com a DATAROBO morta, a cascata do poller cai na
DATAINCLUSAO — que é a DATA DO SYNC (quando a linha entrou na tabela), à
meia-noite, e não a hora da importação. Dois artefatos: latências sync→import
negativas na UI, e o "importado no mesmo dia" do dashboard parcialmente
CIRCULAR (compara a data do sync com ela mesma). Proposta:
- imported_at = HORA DA DETECÇÃO do flip IMPORTADO 0→1 quando a nota estava em
  acompanhamento ativo (PollOnce; o erro é a latência da rotação, e o hot-window
  já prioriza as recém-sincronizadas — exatamente as que importam p/ a métrica);
- manter DATAINCLUSAO só nos caminhos de detecção sabidamente atrasada (sweep,
  repoll, backfill);
- correção retroativa possível: toda observação imported guarda ingested_at
  (hora em que o poller a emitiu) — teto honesto da hora de importação.
Fazer JUNTO com o shadow-sync: com o syncer, o synced_at vira preciso
(sync_moved real); sem este fix, a latência sync→import ficaria sempre negativa
e o F3 não conseguiria medir o ganho real.

**12.6 Backfill retroativo da nota_empresa + filtros por participação** (M0
follow-up): re-poll janelado/on-demand para popular participações do histórico;
depois migrar filtros de lista de notas.codigo_empresa para EXISTS na
nota_empresa.

## 12bis. F1 — diff plano×realidade (`repoll --check-plans`, 2026-07-14)

Rodado READ-ONLY do dev contra o Firebird de prod, comparando os 19.028 planos
do dry-run (`syncer-plans.jsonl`, gerado 10–13/07) com o que o DownloadXML
gravou de fato na TABLISTACHAVEACESSO. A ferramenta ganhou histograma por
segmento e quebra dos extras por empresa (commits `092b92a`, `3a71d35`).

Resultado: URL bate≈15.6k · **URL diverge=1.120** · planejada-sem-linha=3 ·
**linha-fora-do-plano=12.261** · já importadas≈9.35k · aindaNao=4.581. As 1.120
divergências eram DOIS problemas distintos, ambos **corrigidos**:

**12.7 Direção ignorava devolução (`tpNF`) — CORRIGIDO (commit `2d54fc8`):**
385 divergências eram SÓ o segmento ENTRADA/SAIDA, todas notas EMITIDAS pela
própria empresa (mesmo CNPJ) que o DownloadXML arquiva em ENTRADA — devolução
(`tpNF=0`). O `DirectionFromCNPJs` (model.go, usado tb. pelo poller/backfill que
não têm XML) marca emitente como saída sempre. Fix no syncer: `xmlparse` passou
a ler `<tpNF>` (`Result.TipoNF`) e o `PlanFile` faz emitente + `tpNF=0` → ENTRADA.
Corroborado pela origem SIEG (a nota já vem na pasta `\Entrada\` da emitente).
Testes: `xmlparse.TestParse_TipoNF`, `syncer.TestPlanFile_DevolucaoTpNF`.
Revê o "direção 100%" da F0 (SHADOW-SYNC-F0-ACHADOS.md §5).

**12.8 Nome da pasta não é "&"→"e" nem corta ponto — CORRIGIDO (commit
`53aaa5b`):** 735 divergências eram só o segmento da empresa. O `SanitizeSegment`
fazia `&`→`e` e cortava ponto final, mas o DownloadXML MANTÉM ambos (`MARIA
SELMA ... & CIA LTDA`, `CLW CHURRASCARIA LTDA.`). Experimento read-only sobre
TABFILIAL×URL: 13/13 filiais com `&` mantêm o `&` na pasta real (a 14ª usa nome
fantasia distinto, não `&`→`e`); as 2 com ponto mantêm o ponto. **Reverte a
hipótese `&`→`e` da F0** — não se sustentou nos dados. `SanitizeSegment` agora usa
o NOME verbatim, removendo só reservado-NTFS + controle e aparando espaço.
Resíduo (nome fantasia/edição cadastral, ex.: filial da ACCORDES cuja pasta real
é "LUCRO REAL") fica p/ go-forward: casar a pasta EXISTENTE por comparação
normalizada, criar nome canônico só quando não existir.

**Extras (12.261) — diagnóstico:** 12.222 (99,7%) são as contas-lixo SIEG
52/120/996 (§12.1), desvio deliberado. Os outros 39 (0,17%) NÃO são participação
real faltando: são tail de certificado SIEG compartilhado (36 de 3 notas do
emitente CNPJ 45230721000126 espalhadas p/ ~12 empresas não-parte). O syncer, por
derivar do XML, exclui todo esse lixo genericamente — a lista fixa 52/120/996 do
§12.1 é mais estreita que o tail real, mas isso é irrelevante na prática.

**Validação ponta-a-ponta PENDENTE:** os fixes 12.7/12.8 mudam a DERIVAÇÃO; o
`syncer-plans.jsonl` analisado foi gerado pelo syncer ANTIGO. Regenerar os planos
com o syncer novo (dry-run no SRVIMPORT) e re-rodar `--check-plans` deve zerar os
385 e os 735, sobrando só o resíduo de nome fantasia (+ os `aindaNao`, que o
DownloadXML ainda não tinha pego na janela).

**Segurança (achado colateral):** o `.env.example` versionado tem credencial
`SYSDBA` real (superusuário, não read-only) — a premissa de "credencial
read-only p/ poller/investigação" não é verdade; convém trocar por um usuário só
de leitura e tirar o segredo do git.
