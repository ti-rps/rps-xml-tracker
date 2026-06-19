-- +goose Up
ALTER TABLE notas ADD COLUMN IF NOT EXISTS via_robo boolean NOT NULL DEFAULT false;

-- +goose Down
ALTER TABLE notas DROP COLUMN IF EXISTS via_robo;
