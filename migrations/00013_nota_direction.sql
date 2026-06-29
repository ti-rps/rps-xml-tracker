-- Direção da nota relativa à empresa monitorada: 'saida' (empresa é emitente) |
-- 'entrada' (empresa é destinatária) | NULL (indeterminada / sem empresa). Populada
-- pelo poller (compara a raiz do CNPJ da filial — TABFILIAL no Athenas — com o
-- emitente/destinatário) e, retroativamente, pelo one-off `repoll --backfill-direction`.
-- Coluna nullable sem default: ALTER é metadata-only (não reescreve a tabela de 21M).
-- +goose Up
ALTER TABLE notas ADD COLUMN IF NOT EXISTS direction TEXT;

-- +goose Down
ALTER TABLE notas DROP COLUMN IF EXISTS direction;
