-- +goose Up
-- Nome da empresa (cliente no Athenas), vindo do JOIN TABEMPRESAS.CODIGO no poller.
ALTER TABLE observations ADD COLUMN empresa_nome TEXT;
ALTER TABLE notas        ADD COLUMN empresa_nome TEXT;

-- +goose Down
ALTER TABLE notas        DROP COLUMN IF EXISTS empresa_nome;
ALTER TABLE observations DROP COLUMN IF EXISTS empresa_nome;
