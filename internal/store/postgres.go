package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/EnzzoHosaki/rps-xml-tracker/internal/derive"
	"github.com/EnzzoHosaki/rps-xml-tracker/internal/model"
)

// Postgres implements Store on top of Postgres (pgx). Observations are stored
// append-only and idempotently (ON CONFLICT dedup_key DO NOTHING); on each
// accepted batch the affected chaves' derived state is recomputed (in Go, via
// derive.Nota) and UPSERTed into the notas table, so reads hit notas directly.
type Postgres struct {
	pool *pgxpool.Pool
}

func NewPostgres(ctx context.Context, dsn string) (*Postgres, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}
	return &Postgres{pool: pool}, nil
}

func (p *Postgres) Close() { p.pool.Close() }

func (p *Postgres) AppendObservations(ctx context.Context, obs []model.Observation) (int, int, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return 0, 0, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op after Commit

	accepted, rejected := 0, 0
	affected := map[string]struct{}{}

	for _, o := range obs {
		var payload any
		if len(o.Payload) > 0 {
			b, _ := json.Marshal(o.Payload)
			payload = string(b)
		}
		tag, err := tx.Exec(ctx, `
			INSERT INTO observations
			  (chave_acesso, stage, event_type, observed_at, ingested_at, source,
			   doc_type, file_path, file_hash, codigo_empresa, codigo_filial, payload, dedup_key)
			VALUES ($1,$2::stage,$3,$4,$5,$6,$7::doc_type,$8,$9,$10,$11,$12::jsonb,$13)
			ON CONFLICT (dedup_key) DO NOTHING`,
			o.ChaveAcesso, string(o.Stage), o.EventType, o.ObservedAt, o.IngestedAt, o.Source,
			docTypeOrDefault(o.DocType), nullStr(o.FilePath), nullStr(o.FileHash),
			o.CodigoEmpresa, o.CodigoFilial, payload, DedupKey(o))
		if err != nil {
			return 0, 0, err
		}
		if tag.RowsAffected() == 1 {
			accepted++
			if o.ChaveAcesso != "" {
				affected[o.ChaveAcesso] = struct{}{}
			}
		} else {
			rejected++
		}
	}

	for chave := range affected {
		spans, err := loadObservations(ctx, tx, chave)
		if err != nil {
			return 0, 0, err
		}
		if err := upsertNota(ctx, tx, derive.Nota(chave, spans)); err != nil {
			return 0, 0, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, 0, err
	}
	return accepted, rejected, nil
}

func (p *Postgres) GetNota(ctx context.Context, chave string) (model.NotaDetail, bool, error) {
	n, err := scanNota(p.pool.QueryRow(ctx, notaSelect+` WHERE chave_acesso = $1`, chave))
	if errors.Is(err, pgx.ErrNoRows) {
		return model.NotaDetail{}, false, nil
	}
	if err != nil {
		return model.NotaDetail{}, false, err
	}
	spans, err := loadObservations(ctx, p.pool, chave)
	if err != nil {
		return model.NotaDetail{}, false, err
	}
	return model.NotaDetail{Nota: n, Spans: spans}, true, nil
}

func (p *Postgres) ListNotas(ctx context.Context, f NotaFilter) ([]model.Nota, int, error) {
	var where []string
	var args []any
	add := func(cond string, val any) {
		args = append(args, val)
		where = append(where, fmt.Sprintf(cond, len(args)))
	}
	if f.Status != "" {
		add("status = $%d::nota_status", string(f.Status))
	}
	if f.DocType != "" {
		add("doc_type = $%d::doc_type", string(f.DocType))
	}
	if f.CodigoEmpresa != nil {
		add("codigo_empresa = $%d", *f.CodigoEmpresa)
	}
	if f.ChaveQuery != "" {
		add("chave_acesso LIKE $%d", "%"+f.ChaveQuery+"%")
	}
	clause := ""
	if len(where) > 0 {
		clause = " WHERE " + strings.Join(where, " AND ")
	}

	var total int
	if err := p.pool.QueryRow(ctx, `SELECT count(*) FROM notas`+clause, args...).Scan(&total); err != nil {
		return nil, 0, err
	}

	limit := f.Limit
	if limit <= 0 {
		limit = 50
	}
	args = append(args, limit, f.Offset)
	q := notaSelect + clause + fmt.Sprintf(" ORDER BY last_update_at DESC LIMIT $%d OFFSET $%d", len(args)-1, len(args))
	rows, err := p.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	items := []model.Nota{}
	for rows.Next() {
		n, err := scanNota(rows)
		if err != nil {
			return nil, 0, err
		}
		items = append(items, n)
	}
	return items, total, rows.Err()
}

// ---- helpers ----

// rowScanner unifies pgx.Row and pgx.Rows for scanNota.
type rowScanner interface{ Scan(dest ...any) error }

const notaSelect = `
	SELECT chave_acesso, doc_type, status, codigo_empresa, codigo_filial,
	       arrived_at, synced_at, imported_at, import_ignored, motivo_ignorado,
	       first_seen_at, last_update_at, lat_arrival_sync_s, lat_sync_import_s
	FROM notas`

func scanNota(r rowScanner) (model.Nota, error) {
	var n model.Nota
	var motivo *string
	err := r.Scan(&n.ChaveAcesso, &n.DocType, &n.Status, &n.CodigoEmpresa, &n.CodigoFilial,
		&n.ArrivedAt, &n.SyncedAt, &n.ImportedAt, &n.ImportIgnored, &motivo,
		&n.FirstSeenAt, &n.LastUpdateAt, &n.LatArrivalSyncS, &n.LatSyncImportS)
	if motivo != nil {
		n.MotivoIgnorado = *motivo
	}
	return n, err
}

func upsertNota(ctx context.Context, tx pgx.Tx, n model.Nota) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO notas
		  (chave_acesso, doc_type, status, codigo_empresa, codigo_filial,
		   arrived_at, synced_at, imported_at, import_ignored, motivo_ignorado,
		   first_seen_at, last_update_at, lat_arrival_sync_s, lat_sync_import_s)
		VALUES ($1,$2::doc_type,$3::nota_status,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
		ON CONFLICT (chave_acesso) DO UPDATE SET
		  doc_type=EXCLUDED.doc_type, status=EXCLUDED.status,
		  codigo_empresa=EXCLUDED.codigo_empresa, codigo_filial=EXCLUDED.codigo_filial,
		  arrived_at=EXCLUDED.arrived_at, synced_at=EXCLUDED.synced_at,
		  imported_at=EXCLUDED.imported_at, import_ignored=EXCLUDED.import_ignored,
		  motivo_ignorado=EXCLUDED.motivo_ignorado, last_update_at=EXCLUDED.last_update_at,
		  lat_arrival_sync_s=EXCLUDED.lat_arrival_sync_s, lat_sync_import_s=EXCLUDED.lat_sync_import_s`,
		n.ChaveAcesso, docTypeOrDefault(n.DocType), string(n.Status), n.CodigoEmpresa, n.CodigoFilial,
		n.ArrivedAt, n.SyncedAt, n.ImportedAt, n.ImportIgnored, nullStr(n.MotivoIgnorado),
		n.FirstSeenAt, n.LastUpdateAt, n.LatArrivalSyncS, n.LatSyncImportS)
	return err
}

func loadObservations(ctx context.Context, q querier, chave string) ([]model.Observation, error) {
	rows, err := q.Query(ctx, `
		SELECT id, chave_acesso, stage, event_type, observed_at, ingested_at, source,
		       doc_type, file_path, codigo_empresa, codigo_filial, payload
		FROM observations WHERE chave_acesso = $1 ORDER BY observed_at`, chave)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.Observation
	for rows.Next() {
		var o model.Observation
		var filePath *string
		var payload []byte
		if err := rows.Scan(&o.ID, &o.ChaveAcesso, &o.Stage, &o.EventType, &o.ObservedAt,
			&o.IngestedAt, &o.Source, &o.DocType, &filePath, &o.CodigoEmpresa, &o.CodigoFilial, &payload); err != nil {
			return nil, err
		}
		if filePath != nil {
			o.FilePath = *filePath
		}
		if len(payload) > 0 {
			_ = json.Unmarshal(payload, &o.Payload)
		}
		out = append(out, o)
	}
	return out, rows.Err()
}

// querier is satisfied by *pgxpool.Pool and pgx.Tx.
type querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

func docTypeOrDefault(d model.DocType) string {
	if d == "" {
		return string(model.DocUnknown)
	}
	return string(d)
}

func nullStr(s string) any {
	if s == "" {
		return nil
	}
	return s
}
