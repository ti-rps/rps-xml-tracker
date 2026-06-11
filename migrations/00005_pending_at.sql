-- +goose Up
-- pending_at: instante em que a chave foi vista na TABLISTACHAVEACESSO do Athenas
-- ainda NÃO importada (IMPORTADO=0, não ignorada) — o sinal real de "Aguardando
-- Importação", distinto de "Sincronizado" (arquivo posicionado pelo agent). O
-- valor 'pending_import' já existe no enum nota_status desde 00001; aqui só falta
-- o timestamp que dispara o status na derivação.
ALTER TABLE notas ADD COLUMN pending_at TIMESTAMPTZ;

-- +goose Down
ALTER TABLE notas DROP COLUMN IF EXISTS pending_at;
