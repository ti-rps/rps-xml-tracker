-- M0 (shadow-sync §0): participação por empresa. Uma chave pode envolver 2+
-- empresas clientes (emitente=saída p/ A, destinatário=entrada p/ B; medido em
-- F0: 12% das chaves), cada uma com seu PRÓPRIO ciclo de importação no Athenas —
-- a `notas` colapsava tudo numa linha representante e parava de acompanhar a
-- segunda participação assim que a primeira importava (ponto cego CLW/ROSEMBERG).
--
-- `nota_empresa` é MATERIALIZADA das observações (como a `notas`): recomputada a
-- cada recompute da chave. Backfill é GO-FORWARD — as 14,3M notas históricas
-- ganham participações quando forem tocadas por observação nova (rotação do
-- poller) ou por re-poll on-demand; NENHUM backfill pesado roda no boot (gotcha
-- de deploy: migração longa segura a API por ~30min).
--
-- synced_at/sync_url ficam prontos para o syncer (F1): o SINCRONIZADO tem UMA
-- CÓPIA FÍSICA POR PARTICIPAÇÃO (confirmado em F0 — URLs irmãs divergem), então
-- o sync também é fato da participação, não da nota.
-- +goose Up
CREATE TABLE IF NOT EXISTS nota_empresa (
  chave_acesso    VARCHAR(44) NOT NULL,
  codigo_empresa  INTEGER     NOT NULL,
  codigo_filial   INTEGER     NOT NULL,  -- 0 = filial desconhecida na linha do Athenas
  empresa_nome    TEXT,
  papel           TEXT,                  -- emitente | destinatario | NULL (indeterminado)
  direction       TEXT,                  -- saida | entrada | NULL
  status          nota_status NOT NULL,  -- pending_import | imported | import_ignored
  motivo_ignorado TEXT,
  pending_at      TIMESTAMPTZ,
  imported_at     TIMESTAMPTZ,
  synced_at       TIMESTAMPTZ,           -- F1: quando a CÓPIA desta empresa foi posicionada
  sync_url        TEXT,                  -- F1: URL relativa da cópia desta empresa
  first_seen_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_update_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (chave_acesso, codigo_empresa, codigo_filial)
);
-- visão por empresa ("o que está pendente da empresa X"), inclusive das
-- participações que a `notas` não representa (a nota é atribuída a outra empresa)
CREATE INDEX IF NOT EXISTS idx_nota_empresa_emp
  ON nota_empresa (codigo_empresa, codigo_filial, status);

-- +goose Down
DROP TABLE IF EXISTS nota_empresa;
