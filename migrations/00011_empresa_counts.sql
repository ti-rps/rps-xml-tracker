-- +goose Up
-- Contador mantido por (empresa, filial, status): elimina o GROUP BY codigo_empresa
-- sobre as 14M linhas da notas no GET /empresas (era ~30s, só escondido pelo cache).
-- Complementa a notas_counts (00008), que cobre overview/doctypes mas não /empresas
-- (precisa do nome da empresa, denormalizado aqui). Leitura instantânea (alguns
-- milhares de linhas = empresas × status).
--
-- Chaves NULL (notas sem empresa, ou empresa sem filial) usam a SENTINELA -1 para o
-- PK/ON CONFLICT funcionarem (NULLs são distintos em índice único). Os códigos do
-- Athenas (CODIGOEMPRESA/CODIGOFILIAL) são positivos, então -1 nunca colide. O read
-- traduz de volta com NULLIF(coluna,-1). A filial colapsa para -1 quando a empresa é
-- NULL (espelha o "Sem empresa" do dashboard, que não fragmenta por filial).
CREATE TABLE empresa_counts (
  codigo_empresa INT         NOT NULL,  -- -1 = sem empresa
  codigo_filial  INT         NOT NULL,  -- -1 = sem filial / colapsada
  status         nota_status NOT NULL,
  nome           TEXT        NOT NULL DEFAULT '',  -- denormalizado (último não-vazio vence)
  n              BIGINT      NOT NULL DEFAULT 0,
  PRIMARY KEY (codigo_empresa, codigo_filial, status)
);

-- +goose StatementBegin
CREATE FUNCTION empresa_counts_sync() RETURNS trigger AS $$
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
          -- nome só é sobrescrito por um valor não-vazio (preserva o último conhecido)
          nome = CASE WHEN EXCLUDED.nome <> '' THEN EXCLUDED.nome ELSE empresa_counts.nome END;
  END IF;
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

-- Backfill ANTES de armar o trigger (migração roda sem writes concorrentes na notas —
-- api/poller só sobem depois do migrate). Lê notas, escreve empresa_counts: o trigger é
-- na notas, então não há dupla contagem. Este é o ÚNICO scan de 14M (one-time no boot).
INSERT INTO empresa_counts (codigo_empresa, codigo_filial, status, nome, n)
  SELECT COALESCE(codigo_empresa, -1),
         CASE WHEN codigo_empresa IS NULL THEN -1 ELSE COALESCE(codigo_filial, -1) END,
         status,
         COALESCE(max(empresa_nome), ''),
         count(*)
  FROM notas
  GROUP BY 1, 2, 3;

CREATE TRIGGER trg_empresa_counts_ins AFTER INSERT ON notas
  FOR EACH ROW EXECUTE FUNCTION empresa_counts_sync();
CREATE TRIGGER trg_empresa_counts_del AFTER DELETE ON notas
  FOR EACH ROW EXECUTE FUNCTION empresa_counts_sync();
-- Dispara quando empresa/filial/status/nome muda -> re-emissões que não mexem nesses
-- campos (a enxurrada do backfill) não tocam o contador, evitando contenção.
CREATE TRIGGER trg_empresa_counts_upd AFTER UPDATE ON notas
  FOR EACH ROW WHEN (OLD.status         IS DISTINCT FROM NEW.status
                  OR OLD.codigo_empresa IS DISTINCT FROM NEW.codigo_empresa
                  OR OLD.codigo_filial  IS DISTINCT FROM NEW.codigo_filial
                  OR OLD.empresa_nome   IS DISTINCT FROM NEW.empresa_nome)
  EXECUTE FUNCTION empresa_counts_sync();

-- +goose Down
DROP TRIGGER IF EXISTS trg_empresa_counts_upd ON notas;
DROP TRIGGER IF EXISTS trg_empresa_counts_del ON notas;
DROP TRIGGER IF EXISTS trg_empresa_counts_ins ON notas;
DROP FUNCTION IF EXISTS empresa_counts_sync();
DROP TABLE IF EXISTS empresa_counts;
