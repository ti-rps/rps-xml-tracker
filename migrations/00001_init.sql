-- +goose Up
CREATE TYPE doc_type AS ENUM ('NFE', 'NFCE', 'CTE', 'NFS', 'EVENTO', 'UNKNOWN');
CREATE TYPE stage AS ENUM ('arrival', 'sync', 'import');
CREATE TYPE nota_status AS ENUM (
  'arrived','synced','imported','import_ignored','pending_import','stuck','lost'
);

CREATE TABLE empresas (
  id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  codigo_empresa INTEGER     NOT NULL,
  codigo_filial  INTEGER     NOT NULL DEFAULT 1,
  nome           TEXT,
  cnpj           VARCHAR(20),
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (codigo_empresa, codigo_filial)
);
CREATE INDEX idx_empresas_cnpj ON empresas (cnpj);

CREATE TABLE observations (
  id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  chave_acesso   VARCHAR(44),
  stage          stage       NOT NULL,
  event_type     TEXT        NOT NULL,
  observed_at    TIMESTAMPTZ NOT NULL,
  ingested_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  source         TEXT        NOT NULL,
  doc_type       doc_type    NOT NULL DEFAULT 'UNKNOWN',
  file_path      TEXT,
  file_hash      CHAR(32),
  codigo_empresa INTEGER,
  codigo_filial  INTEGER,
  payload        JSONB,
  dedup_key      TEXT        NOT NULL,
  UNIQUE (dedup_key)
);
CREATE INDEX idx_obs_chave  ON observations (chave_acesso);
CREATE INDEX idx_obs_stage  ON observations (stage, observed_at);
CREATE INDEX idx_obs_ingest ON observations (ingested_at);

CREATE TABLE notas (
  chave_acesso      VARCHAR(44) PRIMARY KEY,
  doc_type          doc_type    NOT NULL,
  status            nota_status NOT NULL,
  empresa_id        BIGINT REFERENCES empresas(id),
  codigo_empresa    INTEGER,
  codigo_filial     INTEGER,
  cnpj_emitente     VARCHAR(20),
  cnpj_destinatario VARCHAR(20),
  maestro_job_id    UUID,
  arrived_at        TIMESTAMPTZ,
  synced_at         TIMESTAMPTZ,
  imported_at       TIMESTAMPTZ,
  import_ignored    BOOLEAN     NOT NULL DEFAULT false,
  motivo_ignorado   TEXT,
  situacao          INTEGER,
  valor_total       NUMERIC(15,2),
  data_emissao      DATE,
  first_seen_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_update_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  lat_arrival_sync_s BIGINT,
  lat_sync_import_s  BIGINT
);
CREATE INDEX idx_notas_status   ON notas (status);
CREATE INDEX idx_notas_empresa  ON notas (codigo_empresa, codigo_filial);
CREATE INDEX idx_notas_emitente ON notas (cnpj_emitente);
CREATE INDEX idx_notas_synced   ON notas (synced_at);
CREATE INDEX idx_notas_imported ON notas (imported_at);
CREATE INDEX idx_notas_job      ON notas (maestro_job_id);
CREATE INDEX idx_notas_doc_type ON notas (doc_type);

CREATE TABLE nfse_import (
  athenas_chave   VARCHAR(100) PRIMARY KEY,
  codigo_chave    INTEGER     NOT NULL,
  empresa_id      BIGINT REFERENCES empresas(id),
  status          nota_status NOT NULL,
  importado       BOOLEAN     NOT NULL,
  import_ignored  BOOLEAN     NOT NULL DEFAULT false,
  motivo_ignorado TEXT,
  data_emissao    DATE,
  data_inclusao   DATE,
  valor_total     NUMERIC(15,2),
  first_seen_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_update_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_nfse_status   ON nfse_import (status);
CREATE INDEX idx_nfse_empresa  ON nfse_import (empresa_id);
CREATE INDEX idx_nfse_inclusao ON nfse_import (data_inclusao);

CREATE TABLE firebird_cursor (
  poller_name       TEXT PRIMARY KEY,
  last_datainclusao DATE,
  last_codigo_chave INTEGER,
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- +goose Down
DROP TABLE IF EXISTS firebird_cursor;
DROP TABLE IF EXISTS nfse_import;
DROP TABLE IF EXISTS notas;
DROP TABLE IF EXISTS observations;
DROP TABLE IF EXISTS empresas;
DROP TYPE IF EXISTS nota_status;
DROP TYPE IF EXISTS stage;
DROP TYPE IF EXISTS doc_type;
