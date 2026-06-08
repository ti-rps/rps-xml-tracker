package store

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/EnzzoHosaki/rps-xml-tracker/internal/model"
)

// TestPostgresStore runs only when TRACKER_TEST_PG_DSN is set (e.g. against the
// docker-compose.dev.yml database). It is self-contained: it applies the
// migration's "+goose Up" section, then exercises append/idempotency/get/list.
// Without the env var it is skipped, so `go test ./...` stays green offline.
func TestPostgresStore(t *testing.T) {
	dsn := os.Getenv("TRACKER_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("set TRACKER_TEST_PG_DSN to run the Postgres integration test")
	}
	ctx := context.Background()

	applyMigration(t, ctx, dsn)
	pg, err := NewPostgres(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer pg.Close()

	emp := 1203
	chave := "35250712345678000190550010000001231000001234"
	batch := []model.Observation{
		{ChaveAcesso: chave, Stage: model.StageArrival, EventType: model.EventFileSeen,
			ObservedAt: time.Date(2026, 6, 8, 9, 0, 0, 0, time.UTC), DocType: model.DocNFe,
			Source: "agent:test", CodigoEmpresa: &emp},
		{ChaveAcesso: chave, Stage: model.StageSync, EventType: model.EventFileMoved,
			ObservedAt: time.Date(2026, 6, 8, 9, 30, 0, 0, time.UTC), DocType: model.DocNFe,
			Source: "agent:test", CodigoEmpresa: &emp},
	}

	acc, rej, err := pg.AppendObservations(ctx, batch)
	if err != nil || acc != 2 || rej != 0 {
		t.Fatalf("append: acc=%d rej=%d err=%v (want 2/0)", acc, rej, err)
	}
	// idempotency
	acc, rej, err = pg.AppendObservations(ctx, batch)
	if err != nil || acc != 0 || rej != 2 {
		t.Fatalf("re-append: acc=%d rej=%d err=%v (want 0/2)", acc, rej, err)
	}

	d, ok, err := pg.GetNota(ctx, chave)
	if err != nil || !ok {
		t.Fatalf("get: ok=%v err=%v", ok, err)
	}
	if d.Status != model.StatusSynced {
		t.Errorf("status=%s want synced", d.Status)
	}
	if d.LatArrivalSyncS == nil || *d.LatArrivalSyncS != 1800 {
		t.Errorf("lat=%v want 1800", d.LatArrivalSyncS)
	}
	if len(d.Spans) != 2 {
		t.Errorf("spans=%d want 2", len(d.Spans))
	}

	items, total, err := pg.ListNotas(ctx, NotaFilter{Status: model.StatusSynced, CodigoEmpresa: &emp})
	if err != nil || total != 1 || len(items) != 1 {
		t.Fatalf("list: total=%d len=%d err=%v (want 1/1)", total, len(items), err)
	}
}

func applyMigration(t *testing.T, ctx context.Context, dsn string) {
	t.Helper()
	sql, err := os.ReadFile("../../migrations/00001_init.sql")
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}
	up := string(sql)
	if i := strings.Index(up, "-- +goose Down"); i >= 0 {
		up = up[:i]
	}
	up = strings.ReplaceAll(up, "-- +goose Up", "")

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	defer pool.Close()
	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer conn.Release()

	// clean slate + migration via the SIMPLE protocol (allows multi-statement).
	clean := `DROP TABLE IF EXISTS firebird_cursor, nfse_import, notas, observations, empresas CASCADE;
		DROP TYPE IF EXISTS nota_status, stage, doc_type;`
	if err := conn.Conn().PgConn().Exec(ctx, clean).Close(); err != nil {
		t.Fatalf("clean: %v", err)
	}
	if err := conn.Conn().PgConn().Exec(ctx, up).Close(); err != nil {
		t.Fatalf("apply migration: %v", err)
	}
}
