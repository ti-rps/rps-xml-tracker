-- +goose Up
-- Contador mantido por (doc_type, status): elimina o GROUP BY sobre as 14M linhas da
-- notas no GET /metrics/overview (contagem por status, era ~13s) e no
-- GET /metrics/doctypes (era ~10s). Mantido por trigger; leitura instantânea (~35 linhas).
-- NÃO cobre as latências do overview (percentis, seguem ao vivo) nem o /empresas (que
-- precisaria do nome da empresa — segue no cache). Se os números divergirem, basta
-- re-backfillar: TRUNCATE notas_counts; INSERT ... SELECT ... GROUP BY (com o trigger já
-- armado e sem writes, fica consistente).
CREATE TABLE notas_counts (
  doc_type doc_type    NOT NULL,
  status   nota_status NOT NULL,
  n        BIGINT      NOT NULL DEFAULT 0,
  PRIMARY KEY (doc_type, status)
);

-- +goose StatementBegin
CREATE FUNCTION notas_counts_sync() RETURNS trigger AS $$
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

-- Backfill ANTES de armar o trigger (a migração roda sem writes concorrentes na notas,
-- pois api/poller só sobem depois do migrate). Como o INSERT escreve em notas_counts e o
-- trigger é na notas, não há dupla contagem.
INSERT INTO notas_counts (doc_type, status, n)
  SELECT doc_type, status, count(*) FROM notas GROUP BY doc_type, status;

CREATE TRIGGER trg_notas_counts_ins AFTER INSERT ON notas
  FOR EACH ROW EXECUTE FUNCTION notas_counts_sync();
CREATE TRIGGER trg_notas_counts_del AFTER DELETE ON notas
  FOR EACH ROW EXECUTE FUNCTION notas_counts_sync();
-- Só dispara quando doc_type/status realmente muda -> re-emissões (que não mudam o
-- status) não tocam o contador, evitando contenção na enxurrada do backfill.
CREATE TRIGGER trg_notas_counts_upd AFTER UPDATE ON notas
  FOR EACH ROW WHEN (OLD.doc_type IS DISTINCT FROM NEW.doc_type OR OLD.status IS DISTINCT FROM NEW.status)
  EXECUTE FUNCTION notas_counts_sync();

-- +goose Down
DROP TRIGGER IF EXISTS trg_notas_counts_upd ON notas;
DROP TRIGGER IF EXISTS trg_notas_counts_del ON notas;
DROP TRIGGER IF EXISTS trg_notas_counts_ins ON notas;
DROP FUNCTION IF EXISTS notas_counts_sync();
DROP TABLE IF EXISTS notas_counts;
