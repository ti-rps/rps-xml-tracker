-- rps-xml-tracker — Postgres schema (DRAFT para revisão, Fase 1 MVP)
-- Banco PRÓPRIO do tracker (NÃO o do maestro). Padrão: observação append-only + estado derivado.
-- Incorpora os achados da Fase 0 (2026-06-08). Migrações: goose (a confirmar).
--
-- Decisões da Fase 0 refletidas aqui:
--  - imported_at vem da transição IMPORTADO 0->1 detectada pelo poller (Firebird não tem timestamp
--    de importação confiável). Ver observations.event_type='imported'.
--  - doc_type é AUTORITATIVO do parse do agente (NFE/NFCE/CTE); Firebird TIPODOCUMENTO é NULL demais.
--  - import_ignored (IMPORTACAOIGNORADA=1) é estado TERMINAL legítimo, com motivo. NÃO é stuck/lost.
--  - NFSe (NFS) entra só pelo lado-importação do Firebird (source='firebird', sem arrived/synced).
--  - maestro_job_id fica nullable (LOTEROBO não é usável; correlação é fase posterior).

-- ───────────────────────── enums ─────────────────────────
CREATE TYPE doc_type AS ENUM ('NFE', 'NFCE', 'CTE', 'NFS', 'EVENTO', 'UNKNOWN');

-- 1=chegada, 2=sincronizado, 3=importado (mantém ordem dos spans)
CREATE TYPE stage AS ENUM ('arrival', 'sync', 'import');

-- Estado derivado de uma nota. Terminais: imported, import_ignored, lost.
CREATE TYPE nota_status AS ENUM (
  'arrived',         -- visto na chegada, ainda não sincronizado
  'synced',          -- já em XML_SINCRONIZADO, aguardando importação
  'imported',        -- IMPORTADO=1 (terminal, sucesso)
  'import_ignored',  -- IMPORTACAOIGNORADA=1 (terminal, esperado por config da empresa)
  'pending_import',  -- conhecido no Firebird, IMPORTADO=0 e não ignorado (NFSe/NFe aguardando)
  'stuck',           -- passou do SLA numa etapa (derivado, fase de alertas)
  'lost'             -- visto na chegada e sumiu antes de importar (derivado, fase de alertas)
);

-- ───────────────────────── empresas ─────────────────────────
-- Preenchida a partir do path (`<CODIGOEMPRESA>-<CODIGOFILIAL> NOME`) e/ou do Firebird.
CREATE TABLE empresas (
  id               BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  codigo_empresa   INTEGER     NOT NULL,         -- CODIGOEMPRESA do Athenas (ex.: 1203)
  codigo_filial    INTEGER     NOT NULL DEFAULT 1,-- CODIGOFILIAL (ex.: 1)
  nome             TEXT,
  cnpj             VARCHAR(20),
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (codigo_empresa, codigo_filial)
);
CREATE INDEX idx_empresas_cnpj ON empresas (cnpj);

-- ───────────────────────── observations (append-only) ─────────────────────────
-- Fonte da verdade imutável. Tudo que qualquer fonte observou. Permite re-scan idempotente
-- e investigação de "nota sumida". NUNCA UPDATE/DELETE em operação normal.
CREATE TABLE observations (
  id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  chave_acesso  VARCHAR(44),                     -- trace id (NULL para sinais sem chave, ex. NFSe agregado)
  stage         stage       NOT NULL,
  event_type    TEXT        NOT NULL,            -- 'file_seen','file_moved','imported','import_ignored',...
  observed_at   TIMESTAMPTZ NOT NULL,            -- quando o sinal ocorreu na origem
  ingested_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  source        TEXT        NOT NULL,            -- 'agent:SRVIMPORT','poller:firebird'
  doc_type      doc_type    NOT NULL DEFAULT 'UNKNOWN',
  file_path     TEXT,                            -- caminho no momento (só auditoria — NUNCA chave de join)
  file_hash     CHAR(32),                        -- md5 do conteúdo (dedup)
  codigo_empresa INTEGER,                        -- derivado do path quando disponível
  codigo_filial  INTEGER,
  payload       JSONB,                           -- metadados crus extras (motivo, situacao, etc.)
  dedup_key     TEXT        NOT NULL,            -- hash(source|stage|event|chave|file_hash) p/ idempotência
  UNIQUE (dedup_key)
);
CREATE INDEX idx_obs_chave   ON observations (chave_acesso);
CREATE INDEX idx_obs_stage   ON observations (stage, observed_at);
CREATE INDEX idx_obs_ingest  ON observations (ingested_at);

-- ───────────────────────── notas (estado derivado) ─────────────────────────
-- Recomputável 100% a partir de observations. Uma linha por chave (NFe/NFCe/CTe).
-- Para NFSe (sem chave), ver nfse_import (lado-importação agregado).
CREATE TABLE notas (
  chave_acesso      VARCHAR(44) PRIMARY KEY,
  doc_type          doc_type    NOT NULL,
  status            nota_status NOT NULL,
  empresa_id        BIGINT REFERENCES empresas(id),     -- normalizado (resolvido depois)
  codigo_empresa    INTEGER,                            -- denormalizado (do path), p/ filtro direto
  codigo_filial     INTEGER,
  cnpj_emitente     VARCHAR(20),
  cnpj_destinatario VARCHAR(20),
  maestro_job_id    UUID,                         -- correlação robô↔nota (fase posterior; nullable)
  arrived_at        TIMESTAMPTZ,
  synced_at         TIMESTAMPTZ,
  imported_at       TIMESTAMPTZ,                  -- ~= ciclo do poll que viu IMPORTADO 0->1
  import_ignored    BOOLEAN     NOT NULL DEFAULT false,
  motivo_ignorado   TEXT,                         -- MOTIVOIGNORADOIMPORTACAO
  situacao          INTEGER,                      -- SITUACAO do Firebird
  valor_total       NUMERIC(15,2),
  data_emissao      DATE,
  first_seen_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_update_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  -- latências derivadas (preenchidas quando os spans existem) — base do SLA/baseline F0.4
  lat_arrival_sync_s  BIGINT,                     -- synced_at - arrived_at (segundos)
  lat_sync_import_s   BIGINT                      -- imported_at - synced_at (segundos)
);
CREATE INDEX idx_notas_status     ON notas (status);
CREATE INDEX idx_notas_empresa    ON notas (codigo_empresa, codigo_filial);
CREATE INDEX idx_notas_emitente   ON notas (cnpj_emitente);
CREATE INDEX idx_notas_synced     ON notas (synced_at);
CREATE INDEX idx_notas_imported   ON notas (imported_at);
CREATE INDEX idx_notas_job        ON notas (maestro_job_id);
CREATE INDEX idx_notas_doc_type   ON notas (doc_type);

-- ───────────────────────── nfse_import (NFSe lado-importação) ─────────────────────────
-- NFSe não tem chave de 44 dígitos no filesystem; rastreamos só o estado de importação no
-- Athenas (source='firebird'), por empresa/período. Chave = CHAVEACESSO do Athenas (texto livre).
CREATE TABLE nfse_import (
  athenas_chave   VARCHAR(100) PRIMARY KEY,       -- CHAVEACESSO do Athenas (identificador da NFS)
  codigo_chave    INTEGER     NOT NULL,           -- CODIGO_CHAVEACESSO (PK no Firebird)
  empresa_id      BIGINT REFERENCES empresas(id),
  status          nota_status NOT NULL,           -- imported | import_ignored | pending_import
  importado       BOOLEAN     NOT NULL,
  import_ignored  BOOLEAN     NOT NULL DEFAULT false,
  motivo_ignorado TEXT,
  data_emissao    DATE,
  data_inclusao   DATE,                            -- DATAINCLUSAO (watermark de poll)
  valor_total     NUMERIC(15,2),
  first_seen_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_update_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_nfse_status   ON nfse_import (status);
CREATE INDEX idx_nfse_empresa  ON nfse_import (empresa_id);
CREATE INDEX idx_nfse_inclusao ON nfse_import (data_inclusao);

-- ───────────────────────── firebird_cursor (watermark do poller) ─────────────────────────
-- Polling incremental idempotente. Duas estratégias coexistem (Fase 0):
--   - 'nfse_aggregate': último DATAINCLUSAO varrido para o agregado NFS.
--   - 'inflight_nfe' não usa cursor (é chave-driven: consulta as chaves que o agente reportou).
CREATE TABLE firebird_cursor (
  poller_name     TEXT PRIMARY KEY,               -- ex.: 'nfse_aggregate'
  last_datainclusao DATE,
  last_codigo_chave INTEGER,
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
