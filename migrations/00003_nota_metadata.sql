-- +goose Up
-- Metadados da nota extraídos do XML (agente) ou da linha do Firebird (poller):
-- emitente/destinatário (CNPJ + nome), data de emissão e valor. Ficam também em
-- observations (append-only) pra a re-derivação reproduzir o estado.
ALTER TABLE observations ADD COLUMN cnpj_emitente     VARCHAR(20);
ALTER TABLE observations ADD COLUMN nome_emitente     TEXT;
ALTER TABLE observations ADD COLUMN cnpj_destinatario VARCHAR(20);
ALTER TABLE observations ADD COLUMN nome_destinatario TEXT;
ALTER TABLE observations ADD COLUMN data_emissao      DATE;
ALTER TABLE observations ADD COLUMN valor_total       NUMERIC(15,2);

-- notas já tem cnpj_emitente, cnpj_destinatario, data_emissao, valor_total.
ALTER TABLE notas ADD COLUMN emitente_nome     TEXT;
ALTER TABLE notas ADD COLUMN destinatario_nome TEXT;

CREATE INDEX idx_notas_cnpj_emit     ON notas (cnpj_emitente);
CREATE INDEX idx_notas_cnpj_dest     ON notas (cnpj_destinatario);
CREATE INDEX idx_notas_data_emissao  ON notas (data_emissao);

-- +goose Down
DROP INDEX IF EXISTS idx_notas_data_emissao;
DROP INDEX IF EXISTS idx_notas_cnpj_dest;
DROP INDEX IF EXISTS idx_notas_cnpj_emit;
ALTER TABLE notas DROP COLUMN IF EXISTS destinatario_nome;
ALTER TABLE notas DROP COLUMN IF EXISTS emitente_nome;
ALTER TABLE observations DROP COLUMN IF EXISTS valor_total;
ALTER TABLE observations DROP COLUMN IF EXISTS data_emissao;
ALTER TABLE observations DROP COLUMN IF EXISTS nome_destinatario;
ALTER TABLE observations DROP COLUMN IF EXISTS cnpj_destinatario;
ALTER TABLE observations DROP COLUMN IF EXISTS nome_emitente;
ALTER TABLE observations DROP COLUMN IF EXISTS cnpj_emitente;
