-- +goose Up
-- Rotação do poller: marca quando cada nota foi checada no Firebird pela última
-- vez, pra varrer TODAS as em trânsito ao longo do tempo (não só as mais antigas).
ALTER TABLE notas ADD COLUMN last_polled_at TIMESTAMPTZ;
CREATE INDEX idx_notas_inflight_poll ON notas (last_polled_at)
  WHERE status IN ('arrived', 'synced');

-- +goose Down
DROP INDEX IF EXISTS idx_notas_inflight_poll;
ALTER TABLE notas DROP COLUMN IF EXISTS last_polled_at;
