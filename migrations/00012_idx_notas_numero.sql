-- +goose NO TRANSACTION
-- Índice de expressão para a busca por número da nota (param `numero` do GET /notas).
-- O numero_nota (nNF) é derivado da chave (9 dígitos nas posições 26–34, sem zeros à
-- esquerda) e NÃO é coluna — evita reescrever a tabela de 21M (uma coluna STORED
-- gerada exigiria ACCESS EXCLUSIVE + rewrite, travando a API por minutos no boot).
-- Em vez disso, indexa a MESMA expressão usada na query (store.numeroExpr) com
-- text_pattern_ops, que habilita o LIKE 'prefixo%' a usar o índice. CONCURRENTLY (e
-- por isso NO TRANSACTION) p/ não travar escritas durante a criação na tabela grande.
-- +goose Up
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_notas_numero
  ON notas (ltrim(substring(chave_acesso from 26 for 9), '0') text_pattern_ops);

-- +goose Down
DROP INDEX CONCURRENTLY IF EXISTS idx_notas_numero;
