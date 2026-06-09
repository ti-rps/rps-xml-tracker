// Package store abstracts persistence behind an interface so the API/worker can
// run against an in-memory store (tests, local smoke runs) or Postgres (prod)
// without code changes. The Postgres (pgx) implementation lands behind this same
// interface in the next slice.
package store

import (
	"context"

	"github.com/EnzzoHosaki/rps-xml-tracker/internal/model"
)

// Store persists observations (append-only, idempotent) and serves derived notas.
type Store interface {
	// AppendObservations stores a batch idempotently (dedup by DedupKey).
	// Returns how many were newly accepted vs. skipped as duplicates.
	AppendObservations(ctx context.Context, obs []model.Observation) (accepted, rejected int, err error)

	// GetNota returns the derived nota + its span timeline, or ok=false if unknown.
	GetNota(ctx context.Context, chave string) (model.NotaDetail, bool, error)

	// ListNotas returns derived notas matching the filter (limit/offset paging).
	ListNotas(ctx context.Context, f NotaFilter) (items []model.Nota, total int, err error)

	// ListInflightChaves returns chaves still in flight (status arrived/synced —
	// not yet imported/import_ignored), for the chave-driven Firebird poller.
	ListInflightChaves(ctx context.Context, limit int) ([]string, error)
}

// NotaFilter holds the supported list filters.
type NotaFilter struct {
	Status        model.NotaStatus
	DocType       model.DocType
	CodigoEmpresa *int
	ChaveQuery    string // partial/full chave
	Limit         int
	Offset        int
}

// DedupKey is the idempotency key for an observation: same source+stage+event+
// chave+file_hash never stored twice.
func DedupKey(o model.Observation) string {
	return o.Source + "|" + string(o.Stage) + "|" + o.EventType + "|" + o.ChaveAcesso + "|" + o.FileHash
}
