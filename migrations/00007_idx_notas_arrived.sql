-- +goose Up
-- Índice em arrived_at: acelera o GROUP BY por dia/semana do GET /metrics/timeseries
-- e as subqueries de latência chegada->sync do /metrics/overview (que filtram
-- arrived_at >= now()-interval e hoje fazem seq scan). synced_at e imported_at já
-- têm índice (00001); arrived_at faltava.
CREATE INDEX IF NOT EXISTS idx_notas_arrived ON notas (arrived_at);

-- +goose Down
DROP INDEX IF EXISTS idx_notas_arrived;
