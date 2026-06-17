-- +goose Up
-- Índices de busca: acelerar consultas por nome de empresa, CNPJ e chave parcial
-- (hoje LIKE/ILIKE '%...%' = curinga à esquerda = seq scan da tabela inteira) e a
-- ordenação padrão da listagem (ORDER BY last_update_at DESC, sem índice).
--
-- Trigram (pg_trgm) torna ILIKE/LIKE '%x%' indexável via GIN. Índices criados de
-- forma transacional (não-CONCURRENTLY): no volume atual (dezenas de milhares de
-- linhas) o build é rápido e o lock de escrita breve é aceitável, e mantém a
-- auto-migração no boot robusta — CONCURRENTLY não roda em transação e deixa um
-- índice INVALID se falhar no meio.
CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE INDEX IF NOT EXISTS idx_notas_empresa_nome_trgm ON notas USING gin (empresa_nome gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_notas_cnpj_emit_trgm    ON notas USING gin (cnpj_emitente gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_notas_cnpj_dest_trgm    ON notas USING gin (cnpj_destinatario gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_notas_chave_trgm        ON notas USING gin (chave_acesso gin_trgm_ops);

-- Ordenação padrão da listagem (GET /notas ... ORDER BY last_update_at DESC).
CREATE INDEX IF NOT EXISTS idx_notas_last_update ON notas (last_update_at DESC);

-- +goose Down
DROP INDEX IF EXISTS idx_notas_last_update;
DROP INDEX IF EXISTS idx_notas_chave_trgm;
DROP INDEX IF EXISTS idx_notas_cnpj_dest_trgm;
DROP INDEX IF EXISTS idx_notas_cnpj_emit_trgm;
DROP INDEX IF EXISTS idx_notas_empresa_nome_trgm;
-- pg_trgm fica instalado (barato e pode ser reusado); remover manualmente se quiser.
