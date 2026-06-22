package store

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/EnzzoHosaki/rps-xml-tracker/internal/model"
)

// TestEmpresaCounts valida o contador mantido empresa_counts (migração 00011) contra
// o GROUP BY ao vivo na notas: insere notas variadas (empresas/filiais/status +
// bucket "Sem empresa"), exercita o trigger de UPDATE (mudança de status move o
// bucket) e checa (a) reconciliação contador-vs-live e (b) a saída de Empresas().
// Roda só com TRACKER_TEST_PG_DSN setado (ex.: o docker-compose.dev.yml).
func TestEmpresaCounts(t *testing.T) {
	dsn := os.Getenv("TRACKER_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("set TRACKER_TEST_PG_DSN to run the Postgres integration test")
	}
	ctx := context.Background()
	applyAllMigrations(t, ctx, dsn)

	pg, err := NewPostgres(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer pg.Close()

	emp1203, fil1 := 1203, 1
	emp77 := 77
	at := time.Date(2026, 6, 8, 9, 0, 0, 0, time.UTC)
	mk := func(chave string, stage model.Stage, ev string, emp, fil *int, nome string) model.Observation {
		return model.Observation{ChaveAcesso: chave, Stage: stage, EventType: ev,
			ObservedAt: at, DocType: model.DocNFe, Source: "test",
			CodigoEmpresa: emp, CodigoFilial: fil, NomeEmpresa: nome}
	}

	// emp1203/fil1: uma synced (chegada+sync) + uma arrived (só chegada)
	// emp77: uma arrived
	// sem empresa: uma arrived
	ch := func(s string) string { return s }
	batch := []model.Observation{
		mk(ch("35250712345678000190550010000001231000001001"), model.StageArrival, model.EventFileSeen, &emp1203, &fil1, "ACME LTDA"),
		mk(ch("35250712345678000190550010000001231000001001"), model.StageSync, model.EventFileMoved, &emp1203, &fil1, "ACME LTDA"),
		mk(ch("35250712345678000190550010000001231000001002"), model.StageArrival, model.EventFileSeen, &emp1203, &fil1, "ACME LTDA"),
		mk(ch("35250712345678000190550010000001231000002001"), model.StageArrival, model.EventFileSeen, &emp77, nil, "BETA SA"),
		mk(ch("35250712345678000190550010000001231000009001"), model.StageArrival, model.EventFileSeen, nil, nil, ""),
	}
	if _, _, err := pg.AppendObservations(ctx, batch); err != nil {
		t.Fatalf("append: %v", err)
	}

	// exercita o trigger de UPDATE: a nota 1002 sincroniza (arrived -> synced),
	// movendo o bucket de status dentro de emp1203/fil1.
	upd := []model.Observation{
		mk(ch("35250712345678000190550010000001231000001002"), model.StageSync, model.EventFileMoved, &emp1203, &fil1, "ACME LTDA"),
	}
	if _, _, err := pg.AppendObservations(ctx, upd); err != nil {
		t.Fatalf("append upd: %v", err)
	}

	reconcile(t, ctx, dsn)

	// Empresas() ordenado por código: ACME(1203), BETA(77)? ordenação é por código
	// asc com NULL por último -> 77, 1203, Sem empresa.
	got, total, err := pg.Empresas(ctx, EmpresaFilter{Sort: "codigo"})
	if err != nil {
		t.Fatalf("empresas: %v", err)
	}
	if total != 3 {
		t.Fatalf("total=%d want 3 (emp77, emp1203, sem-empresa)", total)
	}
	byNome := map[string]model.EmpresaAgg{}
	var semEmpresa *model.EmpresaAgg
	for i := range got {
		a := got[i]
		if a.CodigoEmpresa == nil {
			semEmpresa = &got[i]
			continue
		}
		byNome[a.NomeEmpresa] = a
	}
	acme := byNome["ACME LTDA"]
	if acme.CodigoEmpresa == nil || *acme.CodigoEmpresa != 1203 || acme.CodigoFilial == nil || *acme.CodigoFilial != 1 {
		t.Errorf("ACME chaves erradas: %+v", acme)
	}
	if acme.Synced != 2 || acme.Arrived != 0 {
		t.Errorf("ACME synced=%d arrived=%d want 2/0 (ambas sincronizaram)", acme.Synced, acme.Arrived)
	}
	beta := byNome["BETA SA"]
	if beta.Arrived != 1 {
		t.Errorf("BETA arrived=%d want 1", beta.Arrived)
	}
	if semEmpresa == nil {
		t.Fatalf("bucket Sem empresa ausente")
	}
	if semEmpresa.CodigoFilial != nil {
		t.Errorf("Sem empresa: filial deveria ser NULL, veio %v", *semEmpresa.CodigoFilial)
	}
	if semEmpresa.Arrived != 1 {
		t.Errorf("Sem empresa arrived=%d want 1", semEmpresa.Arrived)
	}

	// filtro por nome (ILIKE) deve isolar a ACME
	q, qt, err := pg.Empresas(ctx, EmpresaFilter{Query: "acme"})
	if err != nil || qt != 1 || len(q) != 1 || q[0].NomeEmpresa != "ACME LTDA" {
		t.Fatalf("filtro nome: qt=%d len=%d err=%v", qt, len(q), err)
	}

	// PendentesOnly: ACME tem 2 synced (pendentes>0), BETA 1 arrived, Sem empresa 1
	// arrived -> as 3 entram (nenhuma é só terminal).
	_, pt, err := pg.Empresas(ctx, EmpresaFilter{PendentesOnly: true})
	if err != nil || pt != 3 {
		t.Fatalf("pendentes: pt=%d want 3 err=%v", pt, err)
	}
}

// TestEmpresasDateFilter valida o caminho ao vivo (janela date_field from/to) do
// Empresas: só notas cujo campo escolhido cai no período entram nos agregados.
func TestEmpresasDateFilter(t *testing.T) {
	dsn := os.Getenv("TRACKER_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("set TRACKER_TEST_PG_DSN")
	}
	ctx := context.Background()
	applyAllMigrations(t, ctx, dsn)
	pg, err := NewPostgres(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer pg.Close()

	emp := 1203
	mkImported := func(chave string, day int) []model.Observation {
		at := time.Date(2026, 6, day, 10, 0, 0, 0, time.UTC)
		return []model.Observation{
			{ChaveAcesso: chave, Stage: model.StageArrival, EventType: model.EventFileSeen,
				ObservedAt: at, DocType: model.DocNFe, Source: "agent:test", CodigoEmpresa: &emp, NomeEmpresa: "ACME LTDA"},
			{ChaveAcesso: chave, Stage: model.StageImport, EventType: model.EventImported,
				ObservedAt: at, DocType: model.DocNFe, Source: "poller:firebird", CodigoEmpresa: &emp, NomeEmpresa: "ACME LTDA"},
		}
	}
	// 3 notas importadas em dias diferentes: 05, 15, 25 de junho.
	for _, c := range []struct {
		chave string
		day   int
	}{
		{"35250712345678000190550010000001231000000005", 5},
		{"35250712345678000190550010000001231000000015", 15},
		{"35250712345678000190550010000001231000000025", 25},
	} {
		if _, _, err := pg.AppendObservations(ctx, mkImported(c.chave, c.day)); err != nil {
			t.Fatalf("append %s: %v", c.chave, err)
		}
	}

	// Sem janela (caminho do contador): 3 importadas.
	all, _, err := pg.Empresas(ctx, EmpresaFilter{})
	if err != nil || len(all) != 1 || all[0].Imported != 3 {
		t.Fatalf("sem janela: len=%d imported=%v err=%v (want 1/3)", len(all), all, err)
	}

	// Janela 10..20 de junho por imported (caminho ao vivo): só a nota do dia 15.
	win, _, err := pg.Empresas(ctx, EmpresaFilter{DateField: "imported", From: "2026-06-10", To: "2026-06-20"})
	if err != nil || len(win) != 1 {
		t.Fatalf("janela: len=%d err=%v", len(win), err)
	}
	if win[0].Imported != 1 {
		t.Errorf("janela 10..20: imported=%d want 1 (só o dia 15)", win[0].Imported)
	}

	// Janela que não pega nenhuma: vazio.
	none, _, err := pg.Empresas(ctx, EmpresaFilter{DateField: "imported", From: "2026-07-01", To: "2026-07-31"})
	if err != nil {
		t.Fatalf("janela vazia: %v", err)
	}
	if len(none) != 0 {
		t.Errorf("janela jul: len=%d want 0", len(none))
	}
}

// reconcile compara, por (empresa,filial,status), a soma em empresa_counts com o
// count(*) ao vivo na notas. Qualquer divergência indica bug no trigger.
func reconcile(t *testing.T, ctx context.Context, dsn string) {
	t.Helper()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	defer pool.Close()
	rows, err := pool.Query(ctx, `
		WITH live AS (
		  SELECT COALESCE(codigo_empresa,-1) e,
		         CASE WHEN codigo_empresa IS NULL THEN -1 ELSE COALESCE(codigo_filial,-1) END f,
		         status::text s, count(*) n
		  FROM notas GROUP BY 1,2,3
		),
		ctr AS (SELECT codigo_empresa e, codigo_filial f, status::text s, n FROM empresa_counts WHERE n <> 0)
		SELECT COALESCE(live.e,ctr.e), COALESCE(live.f,ctr.f), COALESCE(live.s,ctr.s),
		       COALESCE(live.n,0), COALESCE(ctr.n,0)
		FROM live FULL OUTER JOIN ctr ON live.e=ctr.e AND live.f=ctr.f AND live.s=ctr.s
		WHERE COALESCE(live.n,0) <> COALESCE(ctr.n,0)`)
	if err != nil {
		t.Fatalf("reconcile query: %v", err)
	}
	defer rows.Close()
	diffs := 0
	for rows.Next() {
		var e, f int
		var s string
		var ln, cn int64
		if err := rows.Scan(&e, &f, &s, &ln, &cn); err != nil {
			t.Fatalf("scan: %v", err)
		}
		t.Errorf("divergência (emp=%d fil=%d status=%s): live=%d counter=%d", e, f, s, ln, cn)
		diffs++
	}
	if diffs == 0 {
		t.Logf("reconciliação OK: contador bate com o GROUP BY ao vivo")
	}
}

// applyAllMigrations limpa o schema e aplica TODAS as migrations (00001..) em ordem,
// via protocolo simples (aceita multi-statement e corpos $$ de função).
func applyAllMigrations(t *testing.T, ctx context.Context, dsn string) {
	t.Helper()
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

	clean := `DROP TABLE IF EXISTS empresa_counts, notas_counts, service_heartbeats,
		    firebird_cursor, nfse_import, notas, observations, empresas CASCADE;
		DROP TYPE IF EXISTS nota_status, stage, doc_type CASCADE;
		DROP FUNCTION IF EXISTS notas_counts_sync, empresa_counts_sync CASCADE;`
	if err := conn.Conn().PgConn().Exec(ctx, clean).Close(); err != nil {
		t.Fatalf("clean: %v", err)
	}

	files, err := filepath.Glob("../../migrations/*.sql")
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	sort.Strings(files)
	for _, fp := range files {
		sql, err := os.ReadFile(fp)
		if err != nil {
			t.Fatalf("read %s: %v", fp, err)
		}
		up := string(sql)
		if i := strings.Index(up, "-- +goose Down"); i >= 0 {
			up = up[:i]
		}
		up = strings.ReplaceAll(up, "-- +goose Up", "")
		up = strings.ReplaceAll(up, "-- +goose StatementBegin", "")
		up = strings.ReplaceAll(up, "-- +goose StatementEnd", "")
		if strings.TrimSpace(up) == "" {
			continue
		}
		if err := conn.Conn().PgConn().Exec(ctx, up).Close(); err != nil {
			t.Fatalf("apply %s: %v", filepath.Base(fp), err)
		}
	}
}
