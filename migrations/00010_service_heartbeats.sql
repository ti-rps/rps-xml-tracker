-- +goose Up
CREATE TABLE service_heartbeats (
    service   TEXT PRIMARY KEY,
    last_beat TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    payload   JSONB NOT NULL DEFAULT '{}'
);

-- +goose Down
DROP TABLE IF EXISTS service_heartbeats;
