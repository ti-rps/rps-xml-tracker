# Shadow-sync F0 — achados da investigação (dados reais)

> Coletado em 2026-07-09 rodando `repoll --profile-insert` / `--check-path` /
> `--watch-chave` contra o Firebird de produção do Athenas (192.168.10.160:3050,
> `e:\Athenas\rps.fdb`), tudo READ-ONLY. Janelas: fill-rate/multi-part 2 dias
> (122.979 linhas), check-path 5 dias (5.000 URLs). Este arquivo é o entregável
> da F0 que alimenta M0 e F1.

## 1. Mecanismo da PK (RESOLVIDO)

Não há trigger que gere a PK. A `CODIGO_CHAVEACESSO` (INTEGER) é preenchida
**client-side pelo app** via o generator **`GEN_CHAVEACESSOXML`**:

- `GEN_CHAVEACESSOXML` = 25.930.675 e `MAX(CODIGO_CHAVEACESSO)` = 25.930.675 —
  batem exatamente. (O `GEN_TABLISTACHAVEACESSO_ID` existe mas está em 0, não é usado.)

➡️ **O INSERT do syncer deve fazer** `SELECT GEN_ID(GEN_CHAVEACESSOXML, 1) FROM RDB$DATABASE`
e passar o valor em `CODIGO_CHAVEACESSO`. (Alternativa: passar a coluna nula NÃO
funciona — nenhuma trigger a resolve.)

## 2. Triggers ativas (uma delas é um GOTCHA)

- **`TABLISTACHAVEACESSO_BI1`**: `NEW.importado = 0` — **força IMPORTADO=0 em todo
  insert**. Ou seja, nosso IMPORTADO=0 é redundante (mas mantê-lo explícito é bom).
- **`CHECK_FORCAIMPORTACAO`** ⚠️ **GOTCHA**: se `ORIGEM=1 AND TIPODOCUMENTO='NFe'`
  E já existe lançamento efetivado na TABENTRADASAIDA para (chave, empresa, filial),
  a trigger **seta IMPORTADO=1 na hora**. Como o DownloadXML grava `TIPODOCUMENTO`
  quase sempre NULL (ver §4), na prática ela raramente dispara — mas se o syncer
  gravar `TIPODOCUMENTO='NFe'`, pode auto-importar uma nota já no livro. **Decisão:
  NÃO gravar TIPODOCUMENTO** (segue o comportamento do DownloadXML e evita a trigger).
- **`TABLISTACHAVEACESSO_BIU0`**: trata eventos (TPEVENTO 110111/110112 =
  cancelamento), SITUACAO=101 (cancelada), detecta CT-e por
  `substring(chaveacesso from 21 for 2) = '63'`, e faz `if DATAINCLUSAO is null then
  DATAINCLUSAO = current_date`. ➡️ DATAINCLUSAO pode ir nula que a trigger preenche;
  ainda assim vamos gravá-la explícita.
- **`TABLISTACHAVEACESSO_BEMP`**: normaliza `codigoempresa` negativo para positivo.
- **`TABLISTACHAVEACESSO_LOG`**: loga DELETE (usa GEN_LOG) — relevante pro rollback
  (nosso DELETE será logado).

## 3. INSERT mínimo (fill-rate, 122.979 linhas / 2 dias)

**SEMPRE preenchidas (100%)** — o conjunto que o DownloadXML sempre grava:
`CHAVEACESSO`, `CODIGO_CHAVEACESSO`, `CODIGOEMPRESA`, `CODIGOFILIAL`,
`CNPJEMITENTE`, `DATA`, `DATAEMISSAO`, `DATAINCLUSAO`, `ORIGEM`, `SERIE`,
`IMPORTADO`, `IMPORTACAOIGNORADA`, `CODIGOSITUACAOSAIDA`, `CODIGOTIPOMOVIMENTO`,
`SITUACAOMANIFESTO`.

**Quase sempre (98-99%)**: `URL` 99%, `EMITENTE` 99%, `VALORTOTAL` 99%,
`MENSAGEM` 99%, `NUMERODOCUMENTO` 98%, `CAMINHOORIGINAL` 98%, `DOWNLOAD` 98%,
`UPLOAD` 98%, `CCEPOSSUI` 98%, `NOTATRANSP` 98%, `CODIGOROTINA_AGEN` 98%.

**Parcial**: `CNPJDESTINATARIO` 31%, `DESTINATARIO` 24% (só entradas têm dest.).

**NUNCA (0%)**: `TIPO`, `TIPODOCUMENTO` (~0%, ver §4), `DATAROBO`, `DATASAIDA`,
`DATADEENTRADA`, `DATAVISUALIZADO`, `HORAEMISSAO`, `LOTEROBO`, `ORDEMATHENAS`,
`SEMDEPARA`, `OBSERVACOESIMPORTACAO`, `CHAVEACESSOSUBS`, `CODIGOTIPOCONTABIL`,
e todas as colunas de cartão/venda/contábil.

Valores concretos observados numa NFe de entrada recém-inserida (`--watch-chave`,
duas linhas irmãs emp 124 e 155, mesma chave):
`ORIGEM=1`, `IMPORTADO=0`, `IMPORTACAOIGNORADA=0`, `DOWNLOAD=1`, `UPLOAD=0`,
`CCEPOSSUI=0`, `NOTATRANSP=0`, `CODIGOROTINA_AGEN=0`, `CODIGOSITUACAOSAIDA=0`,
`CODIGOTIPOMOVIMENTO=0`, `SITUACAOMANIFESTO=0`, `SERIE='001'`,
`DATA=2026-06-01` (1º dia do mês da emissão), `DATAEMISSAO=2026-06-30`,
`DATAINCLUSAO=<data do sync>`, `CAMINHOORIGINAL` e `MENSAGEM` = caminho de origem
UNC (`\\srvdoc01\REDE\XML_ASINCRONIZAR\SIEG\...`), `URL` = caminho relativo (§5).

➡️ **`DATA` = 1º dia do mês da emissão** (não é a emissão nem a inclusão) — reparar
nisso ao montar o INSERT. `CAMINHOORIGINAL`/`MENSAGEM` recebem o caminho UNC de
origem na ASINCRONIZAR.

## 4. TIPODOCUMENTO e TIPO são inúteis (confirmado)

- `TIPO` (E/S): **NULL em 100%** das linhas. Não é como a direção é registrada.
- `TIPODOCUMENTO`: NULL em ~99% (121.627 NULL, 1.350 'NFS', 2 'NFe' em 2 dias).

Ou seja, o DownloadXML **classifica o tipo pelo XML** ao montar a URL, não pela
coluna. Confirma o comentário do `model.go` ("TIPODOCUMENTO is unreliable") e
valida a decisão do syncpath de usar o `DocType` do parse do tracker. **Não
gravar TIPODOCUMENTO** (ver também o gotcha da §2).

## 5. Derivação da URL (`internal/syncpath`) — validação em 5.000 URLs reais

Acerto por segmento (comparando derivação atual × URL real, NFe/NFCe/CTe):

| segmento | fonte | acerto | nota |
|---|---|---|---|
| empresa | ~~TABEMPRESAS.NOME~~ **`TABFILIAL.NOME`** | 98% → ~100% | **CORRIGIDO (2026-07-10, diff plano×realidade):** o 1º segmento é o NOME DA FILIAL — filiais da mesma empresa têm pastas com nomes DIFERENTES (caso JOAO BATISTA emp 369: fil 1 "FAZENDA CONJUNTO LINDOIA", fil 2 "JOAO BATISTA ... - FAZENDA CONJUNTO LINDOIA", ambas batendo com TABFILIAL.NOME). Os "2% de nome editado" eram na verdade FONTE ERRADA (TABEMPRESAS). Syncer/check-path corrigidos: TABFILIAL.NOME com fallback TABEMPRESAS.NOME. Regra `&`→`e` confirmada em pasta real (TRINDADE) |
| cnpj_filial | `TABFILIAL.CNPJ` (14 díg.) | **100%** | contraprova: CNPJ do emitente só casa 77% (cai nas entradas) — confirma que é da FILIAL |
| tipo_doc | classificação do XML | mapa aprendido | NULL→{CTe, CTeOS, NFCe, NFe}; ver §6 |
| direção | `DirectionFromCNPJs` | **100%** (NFe/NFCe/CTe) | os 27% de "erro" eram só NFSe (PRESTADO/TOMADO) — fora do piloto |
| competência | AAAAMM de `DATAEMISSAO` | **100%** | alternativa DATAINCLUSAO só 23% — competência é da EMISSÃO |
| arquivo | `<chave>.xml` | **100%** | |

Padrão confirmado: `\<NOME_EMPRESA>\<CNPJ_FILIAL_14>\<TIPODOC>\<ENTRADA|SAIDA>\<AAAAMM>\<CHAVE>.xml`

Meta de aceite (≥99% NFe/NFCe) atingida em cnpj/competência/arquivo/direção; o
segmento empresa fica ~98% por edições históricas de nome (piso inerente; a
derivação go-forward casa o NOME vigente, como o próprio DownloadXML faz).

## 5b. Diff plano×realidade do dry-run (2026-07-10, amostra dirigida de 22 chaves)

Primeiro dia de dry-run do syncer no SRVIMPORT; as 22 chaves amostradas do
`syncer-plans.jsonl` (casos difíceis: multi-participação, inter-filial, CPF,
`&`→`e`) TODAS já tinham linhas — os arquivos na ASINCRONIZAR são CÓPIAS
DUPLICADAS deixadas para trás (o DownloadXML sincronizou irmãs em 03-26/06).

**Acertos (participações e URLs exatas):** ITALUIZA 2-way (entrada e saída),
GEO+GREEN GOLD, MERCADO CAPIXABA+CHECON, ANA CAROL, DECAUTO inter-filial (fil 1
e fil 2, cada CNPJ na sua pasta, nas DUAS direções), PEDRA BRANCA+PHOENIX,
EUNAPOLIS+MODULO/+RPS, POSTO NORTE SUL+HENZO (CPF), JAILTON (CPF produtor
rural), TRINDADE (`&`→`e` confirmado na pasta real), WEDSON (2 empresas, mesmo
CPF → mesma pasta/arquivo, 2 linhas — nosso fluxo produz exatamente isso).

**Divergências e o que revelaram:**
- 1º segmento = TABFILIAL.NOME (ver §5) — CORRIGIDO no código;
- **linhas EXTRAS de não-participantes**: o DownloadXML cria linhas para RPS
  SERVICOS (52), ROSEMBERG (120) e EMPRESA TESTE (996) em notas de que NÃO são
  parte (contas SIEG que recebem cópia de tudo — RAZAOCERTIFICADO de todas as
  filiais é "RPS SERVICOS LTDA"). Todas ficam IMPORTADO=0 eternas — é a FONTE do
  caso CLW/ROSEMBERG e da poluição de pendentes. O syncer deriva do XML (partes
  reais) e NÃO cria essas linhas: desvio DELIBERADO e desejável;
- **linhas duplicadas idênticas** (PROCIT ×2, ITALUIZA ×2...): o DownloadXML
  insere de novo ao processar outra cópia do mesmo arquivo. O pre-check HasRow
  do syncer evita;
- 1 anomalia rara: URL com segmento extra de DIA (`...\202606\08\chave.xml`) em
  1 de ~40 URLs (AG RAMOS) — fora do padrão, não coberto (o conflito-check
  impede sobrescrita; a participação ganharia pasta padrão nova).

**Refinamento anotado p/ F2:** quando HasRow já é true ANTES de qualquer move
(nota já sincronizada por outra cópia), o syncer deveria PULAR a cópia da
participação (hoje copiaria para a pasta derivada se o destino não existir —
ex.: pasta com nome antigo — gerando arquivo duplicado). No modo real atual,
para esses backlogs duplicados, o comportamento é: destino ok/linha ok → só
remove a origem (faxina) — correto quando a URL bate.

## 6. Fora do escopo do piloto (documentado)

- **NFSe**: chave de **50 dígitos** (não 44), direção **PRESTADO/TOMADO** (não
  ENTRADA/SAIDA), e é a maioria das linhas com TIPODOCUMENTO='NFS'. O `syncpath`
  já a rejeita (exige 44 díg. e direção entrada/saida). Piloto não cobre.
- **CTeOS / BPe**: aparecem como segmento de tipo próprio nas URLs. Fora do piloto
  inicial (allowlist NFe/NFCe).
- **"NAO AUTORIZADO"**: ~5% das URLs têm um segmento extra
  `\...\SAIDA\NAO AUTORIZADO\AAAAMM\...` — notas rejeitadas/não autorizadas. O
  syncer não deve sincronizá-las (nem deveriam estar na ASINCRONIZAR); tratar como
  fora de padrão e pular.
- **Eventos (TPEVENTO) / substitutas (CHAVEACESSOSUBS)**: 0 na amostra recente com
  URL; padrão a confirmar se algum dia entrarem no escopo.

## 7. Multi-participação (dimensiona o M0 — §0 do plano)

Na janela de 2 dias: **12% das chaves (10.909 de 88.729) têm 2+ empresas.** As
combinações mais comuns incluem o ponto cego "importada + pendente":

- `2 importada + 2 pendente` — 2.712 chaves
- `1 importada + 2 pendente` — 1.796 chaves
- `1 importada + 3 pendente` — 1.658 chaves
- `1 importada + 1 pendente` — 656 chaves
- ... (chaves com até 17 participações; ex.: `6 importada + 11 pendente`)

➡️ Milhares de chaves por janela de 2 dias onde ≥1 empresa importou e outras
seguem pendentes — exatamente o ponto cego que o modelo colapsado (uma linha
representante) esconde hoje. **M0 (`nota_empresa`) é necessário.**

**Uma cópia física por (empresa, filial):** as URLs das linhas irmãs **divergem** —
cada CODIGOEMPRESA (e cada filial) tem sua própria pasta com o NOME daquela
empresa. Ex.: a mesma chave para "COMERCIAL ITALUIZA LTDA EPP" (emp 124) e
"COMERCIAL ITALUIZA LTDA" (emp 155). Chaves de produtor rural chegam a 9+ cópias
(mesmo CNPJ raiz, vários codigoempresa/filial, uma pasta cada). ➡️ A unidade de
trabalho do syncer é **(chave, empresa, filial)**: copiar N vezes, um INSERT por
participação, deletar a origem só quando todas completarem.

## 8. Achado colateral relevante ao tracker (fora do shadow-sync)

**`DATAROBO` está NULL em 100%** das linhas recentes (2 dias) e a janela de 180d
por DATAROBO voltou vazia. O `reader.go`/poller usam `DATAROBO` como "quando o
robô importou" (imported_at). Se o Athenas parou de preencher DATAROBO, a latência
sync→import e o imported_at derivado por essa coluna ficam cegos. **Investigar
separadamente** — pode ser mudança de versão do AthenasHorse. (Não bloqueia a F0
do shadow-sync, mas afeta métricas existentes.)

## 9. Decisões que sobem para F1 (o INSERT do syncer)

1. PK: `GEN_ID(GEN_CHAVEACESSOXML, 1)` client-side.
2. Colunas do INSERT: o conjunto SEMPRE da §3 + URL/EMITENTE/VALORTOTAL/
   NUMERODOCUMENTO/CAMINHOORIGINAL/MENSAGEM; `CNPJDESTINATARIO`/`DESTINATARIO` só
   quando entrada; `IMPORTADO=0`, `IMPORTACAOIGNORADA=0`, `ORIGEM=1`, `DOWNLOAD=1`,
   os `CODIGO*=0`, `SERIE`, `DATA`=1º dia do mês da emissão. **Sem TIPODOCUMENTO,
   sem TIPO.**
3. Marcador de autoria: `OBSERVACOES` está livre 99% do tempo e é VARCHAR(250) —
   candidato natural para `OBSERVACOES = 'sync rps-xml-tracker vX.Y'`.
   **RESOLVIDO (2026-07-10, investigação nos próprios dados):** os únicos valores
   existentes são NULL, '' e `'AutXml'` — ou seja, a coluna JÁ é usada como
   marcador de origem por ferramenta; o nosso marcador segue o mesmo padrão.
   Contexto: o banco é hospedado pela RPS (acesso total); o dono da Athenas deu a
   diretriz "observar como o DownloadXML preenche e fazer igual".
   Nota relacionada: DATAROBO está morta desde 2022 (e era DATE, não timestamp) —
   o fallback do poller (DATAINCLUSAO) é o caminho real há anos, não uma exceção.
   **RESPONDIDO pelo funcionário do Athenas (2026-07-10) — como o Horse escolhe:**
   - o XML precisa existir FÍSICO e VÁLIDO no caminho da URL (nosso
     copy→verify→rename antes do INSERT cobre exatamente isso);
   - não há filtro por coluna da TABLISTACHAVEACESSO além do IMPORTADO=0; o
     gate é o **CADASTRO DA EMPRESA**: lá se configura SE importa e QUAIS tipos
     de movimento (entradas/saídas/serviços). Linha de empresa/movimento não
     configurado fica IMPORTADO=0 para sempre — **esta é a causa (ou uma das
     causas) das "pendentes eternas dentro da janela"** que o perfil
     pendente×importada da F0 procurou em coluna e não achou;
   - ordem de processamento: EMPRESA A EMPRESA (pega uma, importa tudo que há
     pendente dela, passa à próxima; sem prioridade conhecida; existe config de
     empresa exclusiva). Explica variação de latência de import entre empresas.
   Implicações: (a) F2 — a cobaia deve ser de empresa CONFIGURADA para importar
   aquele tipo/direção; (b) o DownloadXML insere sem consultar o cadastro (daí
   as pendentes eternas) e o syncer, por paridade, também — MELHORIA FUTURA:
   syncer consultar o cadastro e pular participação não-importável (descobrir a
   tabela de config), casando com o status terminal novo p/ stale.
4. Charset: gravar EMITENTE/DESTINATARIO/URL/OBSERVACOES transcodificados
   UTF-8→Latin-1 (inverso do toUTF8), conexão charset=NONE.
5. Multi-participação (M0 primeiro): um INSERT + uma cópia por (empresa, filial).
