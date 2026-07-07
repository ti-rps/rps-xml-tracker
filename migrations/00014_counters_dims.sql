-- +goose Up
-- P1.1: os contadores mantidos ganham as dimensões que hoje forçam recompute ao vivo
-- (GROUP BY sobre as 21M da notas):
--   - notas_counts (00008)  ganha direction            -> /metrics/overview?doc_type=
--   - empresa_counts (00011) ganha doc_type + direction -> /empresas?doc_type=&direction=
-- Com isso, só JANELA DE DATA (e empresa/filial no overview) continua indo ao vivo.
--
-- direction usa a SENTINELA '' para NULL (PK não aceita NULL; espelha o -1 do
-- empresa/filial). O read filtra por valor exato ('entrada'/'saida'), então a
-- sentinela nunca vaza para a API.
--
-- Rebuild: TRUNCATE + INSERT...GROUP BY — dois scans one-time das 21M no boot do
-- migrate (~1-2min cada). Cardinalidade final segue pequena (filiais × status ×
-- doc_type × direction ≈ dezenas de milhares de linhas; leitura instantânea).
-- Se os números divergirem (ex.: writes concorrentes durante a migração), basta
-- re-backfillar: TRUNCATE + o mesmo INSERT...GROUP BY com os triggers já armados.

-- ================================ notas_counts ================================
TRUNCATE notas_counts;
ALTER TABLE notas_counts ADD COLUMN direction TEXT NOT NULL DEFAULT '';
ALTER TABLE notas_counts DROP CONSTRAINT notas_counts_pkey;

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION notas_counts_sync() RETURNS trigger AS $$
BEGIN
  IF TG_OP IN ('DELETE', 'UPDATE') THEN
    UPDATE notas_counts SET n = n - 1
      WHERE doc_type = OLD.doc_type AND status = OLD.status
        AND direction = COALESCE(OLD.direction, '');
  END IF;
  IF TG_OP IN ('INSERT', 'UPDATE') THEN
    INSERT INTO notas_counts (doc_type, status, direction, n)
    VALUES (NEW.doc_type, NEW.status, COALESCE(NEW.direction, ''), 1)
      ON CONFLICT (doc_type, status, direction) DO UPDATE SET n = notas_counts.n + 1;
  END IF;
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

INSERT INTO notas_counts (doc_type, status, direction, n)
  SELECT doc_type, status, COALESCE(direction, ''), count(*)
  FROM notas GROUP BY 1, 2, 3;

ALTER TABLE notas_counts ADD PRIMARY KEY (doc_type, status, direction);

-- O trigger de UPDATE precisa disparar também quando a direção muda (ela pode ser
-- preenchida depois — ex.: chegada sem empresa, direção só no import).
DROP TRIGGER trg_notas_counts_upd ON notas;
CREATE TRIGGER trg_notas_counts_upd AFTER UPDATE ON notas
  FOR EACH ROW WHEN (OLD.doc_type  IS DISTINCT FROM NEW.doc_type
                  OR OLD.status    IS DISTINCT FROM NEW.status
                  OR OLD.direction IS DISTINCT FROM NEW.direction)
  EXECUTE FUNCTION notas_counts_sync();

-- =============================== empresa_counts ===============================
TRUNCATE empresa_counts;
ALTER TABLE empresa_counts ADD COLUMN doc_type doc_type NOT NULL DEFAULT 'UNKNOWN';
ALTER TABLE empresa_counts ADD COLUMN direction TEXT NOT NULL DEFAULT '';
ALTER TABLE empresa_counts DROP CONSTRAINT empresa_counts_pkey;

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION empresa_counts_sync() RETURNS trigger AS $$
BEGIN
  IF TG_OP IN ('DELETE', 'UPDATE') THEN
    UPDATE empresa_counts SET n = n - 1
      WHERE codigo_empresa = COALESCE(OLD.codigo_empresa, -1)
        AND codigo_filial  = CASE WHEN OLD.codigo_empresa IS NULL THEN -1
                                  ELSE COALESCE(OLD.codigo_filial, -1) END
        AND doc_type  = OLD.doc_type
        AND direction = COALESCE(OLD.direction, '')
        AND status = OLD.status;
  END IF;
  IF TG_OP IN ('INSERT', 'UPDATE') THEN
    INSERT INTO empresa_counts (codigo_empresa, codigo_filial, doc_type, direction, status, nome, n)
    VALUES (
      COALESCE(NEW.codigo_empresa, -1),
      CASE WHEN NEW.codigo_empresa IS NULL THEN -1 ELSE COALESCE(NEW.codigo_filial, -1) END,
      NEW.doc_type,
      COALESCE(NEW.direction, ''),
      NEW.status,
      COALESCE(NEW.empresa_nome, ''),
      1)
    ON CONFLICT (codigo_empresa, codigo_filial, doc_type, direction, status) DO UPDATE
      SET n = empresa_counts.n + 1,
          -- nome só é sobrescrito por um valor não-vazio (preserva o último conhecido)
          nome = CASE WHEN EXCLUDED.nome <> '' THEN EXCLUDED.nome ELSE empresa_counts.nome END;
  END IF;
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

INSERT INTO empresa_counts (codigo_empresa, codigo_filial, doc_type, direction, status, nome, n)
  SELECT COALESCE(codigo_empresa, -1),
         CASE WHEN codigo_empresa IS NULL THEN -1 ELSE COALESCE(codigo_filial, -1) END,
         doc_type,
         COALESCE(direction, ''),
         status,
         COALESCE(max(empresa_nome), ''),
         count(*)
  FROM notas
  GROUP BY 1, 2, 3, 4, 5;

ALTER TABLE empresa_counts ADD PRIMARY KEY (codigo_empresa, codigo_filial, doc_type, direction, status);

DROP TRIGGER trg_empresa_counts_upd ON notas;
CREATE TRIGGER trg_empresa_counts_upd AFTER UPDATE ON notas
  FOR EACH ROW WHEN (OLD.status         IS DISTINCT FROM NEW.status
                  OR OLD.codigo_empresa IS DISTINCT FROM NEW.codigo_empresa
                  OR OLD.codigo_filial  IS DISTINCT FROM NEW.codigo_filial
                  OR OLD.doc_type       IS DISTINCT FROM NEW.doc_type
                  OR OLD.direction      IS DISTINCT FROM NEW.direction
                  OR OLD.empresa_nome   IS DISTINCT FROM NEW.empresa_nome)
  EXECUTE FUNCTION empresa_counts_sync();

-- +goose Down
-- Volta às formas de 00008/00011 (sem as dimensões novas), rebackfillando.

TRUNCATE notas_counts;
ALTER TABLE notas_counts DROP CONSTRAINT notas_counts_pkey;
ALTER TABLE notas_counts DROP COLUMN direction;

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION notas_counts_sync() RETURNS trigger AS $$
BEGIN
  IF TG_OP IN ('DELETE', 'UPDATE') THEN
    UPDATE notas_counts SET n = n - 1
      WHERE doc_type = OLD.doc_type AND status = OLD.status;
  END IF;
  IF TG_OP IN ('INSERT', 'UPDATE') THEN
    INSERT INTO notas_counts (doc_type, status, n) VALUES (NEW.doc_type, NEW.status, 1)
      ON CONFLICT (doc_type, status) DO UPDATE SET n = notas_counts.n + 1;
  END IF;
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

INSERT INTO notas_counts (doc_type, status, n)
  SELECT doc_type, status, count(*) FROM notas GROUP BY doc_type, status;
ALTER TABLE notas_counts ADD PRIMARY KEY (doc_type, status);

DROP TRIGGER trg_notas_counts_upd ON notas;
CREATE TRIGGER trg_notas_counts_upd AFTER UPDATE ON notas
  FOR EACH ROW WHEN (OLD.doc_type IS DISTINCT FROM NEW.doc_type OR OLD.status IS DISTINCT FROM NEW.status)
  EXECUTE FUNCTION notas_counts_sync();

TRUNCATE empresa_counts;
ALTER TABLE empresa_counts DROP CONSTRAINT empresa_counts_pkey;
ALTER TABLE empresa_counts DROP COLUMN doc_type;
ALTER TABLE empresa_counts DROP COLUMN direction;

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION empresa_counts_sync() RETURNS trigger AS $$
BEGIN
  IF TG_OP IN ('DELETE', 'UPDATE') THEN
    UPDATE empresa_counts SET n = n - 1
      WHERE codigo_empresa = COALESCE(OLD.codigo_empresa, -1)
        AND codigo_filial  = CASE WHEN OLD.codigo_empresa IS NULL THEN -1
                                  ELSE COALESCE(OLD.codigo_filial, -1) END
        AND status = OLD.status;
  END IF;
  IF TG_OP IN ('INSERT', 'UPDATE') THEN
    INSERT INTO empresa_counts (codigo_empresa, codigo_filial, status, nome, n)
    VALUES (
      COALESCE(NEW.codigo_empresa, -1),
      CASE WHEN NEW.codigo_empresa IS NULL THEN -1 ELSE COALESCE(NEW.codigo_filial, -1) END,
      NEW.status,
      COALESCE(NEW.empresa_nome, ''),
      1)
    ON CONFLICT (codigo_empresa, codigo_filial, status) DO UPDATE
      SET n = empresa_counts.n + 1,
          nome = CASE WHEN EXCLUDED.nome <> '' THEN EXCLUDED.nome ELSE empresa_counts.nome END;
  END IF;
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

INSERT INTO empresa_counts (codigo_empresa, codigo_filial, status, nome, n)
  SELECT COALESCE(codigo_empresa, -1),
         CASE WHEN codigo_empresa IS NULL THEN -1 ELSE COALESCE(codigo_filial, -1) END,
         status,
         COALESCE(max(empresa_nome), ''),
         count(*)
  FROM notas
  GROUP BY 1, 2, 3;
ALTER TABLE empresa_counts ADD PRIMARY KEY (codigo_empresa, codigo_filial, status);

DROP TRIGGER trg_empresa_counts_upd ON notas;
CREATE TRIGGER trg_empresa_counts_upd AFTER UPDATE ON notas
  FOR EACH ROW WHEN (OLD.status         IS DISTINCT FROM NEW.status
                  OR OLD.codigo_empresa IS DISTINCT FROM NEW.codigo_empresa
                  OR OLD.codigo_filial  IS DISTINCT FROM NEW.codigo_filial
                  OR OLD.empresa_nome   IS DISTINCT FROM NEW.empresa_nome)
  EXECUTE FUNCTION empresa_counts_sync();
