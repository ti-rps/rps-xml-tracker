package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

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
			   doc_type, file_path, file_hash, codigo_empresa, codigo_filial, payload, dedup_key,
			   cnpj_emitente, nome_emitente, cnpj_destinatario, nome_destinatario, data_emissao, valor_total,
			   empresa_nome)
			VALUES ($1,$2::stage,$3,$4,$5,$6,$7::doc_type,$8,$9,$10,$11,$12::jsonb,$13,
			        $14,$15,$16,$17,$18::date,$19,$20)
			ON CONFLICT (dedup_key) DO NOTHING`,
			o.ChaveAcesso, string(o.Stage), o.EventType, o.ObservedAt, o.IngestedAt, o.Source,
			docTypeOrDefault(o.DocType), nullStr(o.FilePath), nullStr(o.FileHash),
			o.CodigoEmpresa, o.CodigoFilial, payload, DedupKey(o),
			nullStr(o.CnpjEmitente), nullStr(o.NomeEmitente), nullStr(o.CnpjDestinatario),
			nullStr(o.NomeDestinatario), nullStr(o.DataEmissao), o.ValorTotal, nullStr(o.NomeEmpresa))
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
	if f.CodigoFilial != nil {
		add("codigo_filial = $%d", *f.CodigoFilial)
	}
	if f.SemEmpresa {
		where = append(where, "codigo_empresa IS NULL")
	}
	if f.EmpresaQuery != "" {
		add("empresa_nome ILIKE $%d", "%"+f.EmpresaQuery+"%")
	}
	if f.Cnpj != "" {
		// casa emitente OU destinatário num único placeholder
		args = append(args, "%"+f.Cnpj+"%")
		where = append(where, fmt.Sprintf("(cnpj_emitente LIKE $%d OR cnpj_destinatario LIKE $%d)", len(args), len(args)))
	}
	if f.ChaveQuery != "" {
		if isCompleteChave(f.ChaveQuery) {
			// Chave completa (44 dígitos) -> match exato na PRIMARY KEY (instantâneo),
			// em vez de LIKE '%...%', cujo curinga à esquerda forçaria seq scan.
			add("chave_acesso = $%d", f.ChaveQuery)
		} else {
			add("chave_acesso LIKE $%d", "%"+f.ChaveQuery+"%")
		}
	}
	if col := dateColumn(f.DateField); col != "" {
		if f.From != "" {
			add(col+" >= $%d::date", f.From)
		}
		if f.To != "" {
			add(col+" <= $%d::date", f.To)
		}
	}
	clause := ""
	if len(where) > 0 {
		clause = " WHERE " + strings.Join(where, " AND ")
	}

	// total: na `notas` (14M linhas) o count(*) exato sem filtro é ~7s (seq/index
	// scan da tabela inteira), pago a cada listagem. Sem filtro -> ESTIMATIVA via
	// pg_class.reltuples (instantânea; o número exato não importa pra paginação da
	// UI). Com filtro -> count exato (o índice/filtro corta o conjunto). Roda em
	// PARALELO com a query da página (leituras independentes; pgxpool é concorrente),
	// então o tempo de parede vira max(total, página) em vez da soma — antes eram
	// dois scans sequenciais do mesmo conjunto.
	noFilter := len(where) == 0
	countArgs := append([]any(nil), args...) // snapshot antes do append de limit/offset (evita race)
	type totalRes struct {
		n   int
		err error
	}
	totalCh := make(chan totalRes, 1)
	go func() {
		var t int
		var e error
		if noFilter {
			e = p.pool.QueryRow(ctx, `SELECT reltuples::bigint FROM pg_class WHERE relname = 'notas'`).Scan(&t)
		} else {
			e = p.pool.QueryRow(ctx, `SELECT count(*) FROM notas`+clause, countArgs...).Scan(&t)
		}
		if t < 0 { // reltuples = -1 quando a tabela nunca foi ANALYZEd
			t = 0
		}
		totalCh <- totalRes{t, e}
	}()

	limit := f.Limit
	if limit <= 0 {
		limit = 50
	}
	args = append(args, limit, f.Offset)
	q := notaSelect + clause + fmt.Sprintf(" ORDER BY last_update_at DESC LIMIT $%d OFFSET $%d", len(args)-1, len(args))
	rows, err := p.pool.Query(ctx, q, args...)
	if err != nil {
		<-totalCh // drena o goroutine antes de sair
		return nil, 0, err
	}
	defer rows.Close()
	items := []model.Nota{}
	for rows.Next() {
		n, err := scanNota(rows)
		if err != nil {
			<-totalCh
			return nil, 0, err
		}
		items = append(items, n)
	}
	if err := rows.Err(); err != nil {
		<-totalCh
		return nil, 0, err
	}
	tr := <-totalCh
	if tr.err != nil {
		return nil, 0, tr.err
	}
	return items, tr.n, nil
}

// isCompleteChave reports whether s looks like a full 44-digit access key. When it
// does, ListNotas hits the PRIMARY KEY with `=` instead of a leading-wildcard LIKE
// (which would seq-scan the whole table), making the common "paste a chave" search
// instantaneous. Partial input still falls back to the trigram-backed LIKE.
func isCompleteChave(s string) bool {
	if len(s) != 44 {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// ListInflightChaves returns the LEAST-recently-polled in-flight chaves and
// stamps them as polled now — so successive cycles rotate through ALL in-flight
// notas instead of re-checking the same oldest batch forever (Fase 1 fix).
func (p *Postgres) ListInflightChaves(ctx context.Context, limit int) ([]string, error) {
	if limit <= 0 {
		limit = 1000
	}
	rows, err := p.pool.Query(ctx, `
		WITH picked AS (
		  SELECT chave_acesso FROM notas
		  WHERE status IN ('arrived','synced','pending_import')
		  ORDER BY last_polled_at ASC NULLS FIRST
		  LIMIT $1
		  FOR UPDATE SKIP LOCKED
		)
		UPDATE notas n SET last_polled_at = now()
		FROM picked WHERE n.chave_acesso = picked.chave_acesso
		RETURNING n.chave_acesso`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func (p *Postgres) ListChavesByStatus(ctx context.Context, status model.NotaStatus, limit, offset int) ([]string, error) {
	q := `SELECT chave_acesso FROM notas WHERE status = $1::nota_status ORDER BY chave_acesso`
	args := []any{string(status)}
	if limit > 0 {
		args = append(args, limit, offset)
		q += fmt.Sprintf(" LIMIT $%d OFFSET $%d", len(args)-1, len(args))
	}
	rows, err := p.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func (p *Postgres) DeleteImportIgnoredObs(ctx context.Context, chave string) (int, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op after Commit

	tag, err := tx.Exec(ctx,
		`DELETE FROM observations WHERE chave_acesso=$1 AND stage='import'::stage AND event_type=$2`,
		chave, model.EventImportIgnored)
	if err != nil {
		return 0, err
	}
	n := int(tag.RowsAffected())
	if n > 0 {
		// recomputa a nota a partir das observações restantes (volta a synced).
		spans, err := loadObservations(ctx, tx, chave)
		if err != nil {
			return 0, err
		}
		if len(spans) > 0 {
			if err := upsertNota(ctx, tx, derive.Nota(chave, spans)); err != nil {
				return 0, err
			}
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return n, nil
}

func (p *Postgres) ListChavesImportedSince(ctx context.Context, since time.Time) ([]string, error) {
	rows, err := p.pool.Query(ctx,
		`SELECT chave_acesso FROM notas
		 WHERE status='imported'::nota_status AND imported_at >= $1
		 ORDER BY imported_at`, since)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func (p *Postgres) UpdateImportedObservedAt(ctx context.Context, chave string, observedAt time.Time) (bool, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op after Commit

	// só atualiza se o valor mudar (idempotente; re-rodar não reescreve nada).
	tag, err := tx.Exec(ctx,
		`UPDATE observations SET observed_at=$2
		 WHERE chave_acesso=$1 AND stage='import'::stage AND event_type=$3
		   AND observed_at IS DISTINCT FROM $2`,
		chave, observedAt, model.EventImported)
	if err != nil {
		return false, err
	}
	if tag.RowsAffected() == 0 {
		return false, tx.Commit(ctx) // nada a corrigir
	}
	// re-deriva a nota (imported_at acompanha o novo observed_at).
	spans, err := loadObservations(ctx, tx, chave)
	if err != nil {
		return false, err
	}
	if len(spans) > 0 {
		if err := upsertNota(ctx, tx, derive.Nota(chave, spans)); err != nil {
			return false, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return true, nil
}

// latencyWindow é a janela móvel dos percentis de latência do overview. Recorta
// fora o backfill histórico (arrived_at = ModTime antigo) e dá uma leitura de SLA
// "atual" em vez de all-time. Ajuste aqui se o produto quiser outro horizonte.
const latencyWindow = "30 days"

// tzSaoPaulo é o fuso de bucketização da série temporal (mesma decisão do dashboard
// do maestro). Constante — interpolada direto no SQL sem risco de injeção.
const tzSaoPaulo = "America/Sao_Paulo"

func (p *Postgres) Overview(ctx context.Context) (model.Overview, error) {
	var ov model.Overview
	// Contagem por status: do contador mantido (notas_counts) — instantâneo, sem
	// escanear as 14M da notas (migração 00008).
	rows, err := p.pool.Query(ctx, `SELECT status, sum(n)::bigint FROM notas_counts GROUP BY status`)
	if err != nil {
		return ov, err
	}
	for rows.Next() {
		var s string
		var c int
		if err := rows.Scan(&s, &c); err != nil {
			rows.Close()
			return ov, err
		}
		addStatusN(&ov.StatusCounts, model.NotaStatus(s), c)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return ov, err
	}

	// Importadas hoje: range em imported_at (usa idx_notas_imported) em vez de
	// imported_at::date (que forçava seq scan das 14M).
	if err := p.pool.QueryRow(ctx,
		`SELECT count(*) FROM notas WHERE imported_at >= current_date AND imported_at < current_date + interval '1 day'`).Scan(&ov.ImportedToday); err != nil {
		return ov, err
	}

	// Percentis sobre uma JANELA MÓVEL (últimos 30 dias) do campo de início de
	// cada métrica. Isso exclui o backfill histórico, cujo arrived_at é o ModTime
	// antigo do arquivo (não uma transição chegada->sync real) e inflava o p50/p95.
	// percentile_cont é ordered-set agg e não aceita FILTER, então cada um é uma
	// subquery com seu próprio recorte (arrived_at p/ chegada->sync, synced_at p/
	// sync->import). Janela tunável via latencyWindow.
	var a50, a95, s50, s95 *float64
	if err := p.pool.QueryRow(ctx, `SELECT
		(SELECT percentile_cont(0.5)  WITHIN GROUP (ORDER BY lat_arrival_sync_s)
		   FROM notas WHERE arrived_at >= now() - $1::interval),
		(SELECT percentile_cont(0.95) WITHIN GROUP (ORDER BY lat_arrival_sync_s)
		   FROM notas WHERE arrived_at >= now() - $1::interval),
		(SELECT percentile_cont(0.5)  WITHIN GROUP (ORDER BY lat_sync_import_s)
		   FROM notas WHERE synced_at  >= now() - $1::interval),
		(SELECT percentile_cont(0.95) WITHIN GROUP (ORDER BY lat_sync_import_s)
		   FROM notas WHERE synced_at  >= now() - $1::interval)`,
		latencyWindow).Scan(&a50, &a95, &s50, &s95); err != nil {
		return ov, err
	}
	ov.InTransit = ov.Arrived + ov.Synced
	ov.LatArrivalSyncP50S, ov.LatArrivalSyncP95S = f2i(a50), f2i(a95)
	ov.LatSyncImportP50S, ov.LatSyncImportP95S = f2i(s50), f2i(s95)
	return ov, nil
}

func (p *Postgres) Timeseries(ctx context.Context, f TimeseriesFilter) (model.Timeseries, error) {
	ts := model.Timeseries{
		Range:   fmt.Sprintf("%dd", f.RangeDays),
		Bucket:  f.Bucket,
		TZ:      tzSaoPaulo,
		Buckets: []model.TimeseriesBucket{},
	}
	// unit/step vêm de um whitelist (handler valida) -> seguro interpolar no SQL.
	unit, step := "day", "1 day"
	if f.Bucket == "week" {
		unit, step = "week", "1 week"
	}
	// spine = todos os buckets do range (date_trunc no fuso local), LEFT JOIN com os
	// agregados -> série contínua. Contagens por evento (arrived_at/synced_at/imported_at);
	// import_ignored = notas com status atual import_ignored, datadas pelo observed_at do
	// evento de ignore. Latências = percentis por coorte (chegada->sync por quem chegou no
	// bucket; sync->import por quem sincronizou). $1=dias, $2=event_type do ignore.
	q := fmt.Sprintf(`
WITH spine AS (
  SELECT generate_series(
           date_trunc('%[1]s', (now() AT TIME ZONE '%[3]s') - make_interval(days => $1 - 1)),
           date_trunc('%[1]s', (now() AT TIME ZONE '%[3]s')),
           interval '%[2]s'
         )::date AS b
),
arr AS (
  SELECT date_trunc('%[1]s', (arrived_at AT TIME ZONE '%[3]s'))::date AS b,
         count(*) AS n,
         percentile_cont(0.5)  WITHIN GROUP (ORDER BY lat_arrival_sync_s) AS p50,
         percentile_cont(0.95) WITHIN GROUP (ORDER BY lat_arrival_sync_s) AS p95
  FROM notas WHERE arrived_at >= now() - make_interval(days => $1 + 7) GROUP BY 1
),
syn AS (
  SELECT date_trunc('%[1]s', (synced_at AT TIME ZONE '%[3]s'))::date AS b,
         count(*) AS n,
         percentile_cont(0.5)  WITHIN GROUP (ORDER BY lat_sync_import_s) AS p50,
         percentile_cont(0.95) WITHIN GROUP (ORDER BY lat_sync_import_s) AS p95
  FROM notas WHERE synced_at >= now() - make_interval(days => $1 + 7) GROUP BY 1
),
imp AS (
  SELECT date_trunc('%[1]s', (imported_at AT TIME ZONE '%[3]s'))::date AS b, count(*) AS n
  FROM notas WHERE imported_at >= now() - make_interval(days => $1 + 7) GROUP BY 1
),
ign AS (
  SELECT date_trunc('%[1]s', (o.observed_at AT TIME ZONE '%[3]s'))::date AS b,
         count(DISTINCT o.chave_acesso) AS n
  FROM observations o
  JOIN notas n ON n.chave_acesso = o.chave_acesso AND n.status = 'import_ignored'::nota_status
  WHERE o.stage = 'import'::stage AND o.event_type = $2
    AND o.observed_at >= now() - make_interval(days => $1 + 7)
  GROUP BY 1
)
SELECT s.b,
       COALESCE(arr.n,0), COALESCE(syn.n,0), COALESCE(imp.n,0), COALESCE(ign.n,0),
       arr.p50, arr.p95, syn.p50, syn.p95
FROM spine s
LEFT JOIN arr ON arr.b = s.b
LEFT JOIN syn ON syn.b = s.b
LEFT JOIN imp ON imp.b = s.b
LEFT JOIN ign ON ign.b = s.b
ORDER BY s.b`, unit, step, tzSaoPaulo)

	rows, err := p.pool.Query(ctx, q, f.RangeDays, model.EventImportIgnored)
	if err != nil {
		return ts, err
	}
	defer rows.Close()
	for rows.Next() {
		var b time.Time
		var arrived, synced, imported, ignored int
		var a50, a95, s50, s95 *float64
		if err := rows.Scan(&b, &arrived, &synced, &imported, &ignored, &a50, &a95, &s50, &s95); err != nil {
			return ts, err
		}
		ts.Buckets = append(ts.Buckets, model.TimeseriesBucket{
			Date:               b.Format("2006-01-02"),
			Arrived:            arrived,
			Synced:             synced,
			Imported:           imported,
			ImportIgnored:      ignored,
			LatArrivalSyncP50S: f2i(a50),
			LatArrivalSyncP95S: f2i(a95),
			LatSyncImportP50S:  f2i(s50),
			LatSyncImportP95S:  f2i(s95),
		})
	}
	return ts, rows.Err()
}

func (p *Postgres) DocTypes(ctx context.Context) ([]model.DocTypeCount, error) {
	// Do contador mantido (notas_counts) — instantâneo (migração 00008).
	rows, err := p.pool.Query(ctx,
		`SELECT doc_type, sum(n)::bigint AS total FROM notas_counts GROUP BY doc_type ORDER BY total DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []model.DocTypeCount{}
	for rows.Next() {
		var d model.DocTypeCount
		if err := rows.Scan(&d.DocType, &d.Count); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

func (p *Postgres) BacklogAge(ctx context.Context) ([]model.BacklogBucket, error) {
	// Idade desde a chegada das notas pendentes (não-terminais). COALESCE com
	// first_seen_at p/ notas sem arrived_at (ex.: vistas só pelo poller como pendente).
	rows, err := p.pool.Query(ctx, `
		SELECT CASE
		         WHEN age < interval '1 hour'   THEN '<1h'
		         WHEN age < interval '6 hours'  THEN '1-6h'
		         WHEN age < interval '24 hours' THEN '6-24h'
		         WHEN age < interval '3 days'   THEN '1-3d'
		         WHEN age < interval '7 days'   THEN '3-7d'
		         ELSE '>7d'
		       END AS label, count(*)
		FROM (SELECT now() - COALESCE(arrived_at, first_seen_at) AS age
		      FROM notas WHERE status IN ('arrived','synced','pending_import')) s
		GROUP BY 1`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	counts := map[string]int{}
	for rows.Next() {
		var label string
		var n int
		if err := rows.Scan(&label, &n); err != nil {
			return nil, err
		}
		counts[label] = n
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return orderedBacklog(counts), nil
}

// orderedBacklog monta o slice na ordem canônica das faixas, incluindo as vazias (0).
func orderedBacklog(counts map[string]int) []model.BacklogBucket {
	out := make([]model.BacklogBucket, 0, len(model.BacklogBuckets))
	for _, label := range model.BacklogBuckets {
		out = append(out, model.BacklogBucket{Label: label, Count: counts[label]})
	}
	return out
}

func (p *Postgres) Empresas(ctx context.Context, f EmpresaFilter) ([]model.EmpresaAgg, int, error) {
	// Sem janela de data -> lê do contador (instantâneo). Com date_field+from/to ->
	// recomputa ao vivo da notas (o contador não tem dimensão temporal). Ambos os
	// caminhos produzem as MESMAS colunas, na mesma ordem, p/ o scan ser compartilhado.
	var q string
	var args []any
	if col := dateColumn(f.DateField); col != "" && (f.From != "" || f.To != "") {
		q, args = empresasFilteredQuery(f, col)
	} else {
		q, args = empresasCounterQuery(f)
	}
	rows, err := p.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	out := []model.EmpresaAgg{}
	total := 0
	for rows.Next() {
		var a model.EmpresaAgg
		var c model.StatusCounts
		if err := rows.Scan(&a.CodigoEmpresa, &a.CodigoFilial, &a.NomeEmpresa,
			&c.Arrived, &c.Synced, &c.Imported, &c.ImportIgnored, &c.PendingImport, &c.Stuck, &c.Lost,
			&total); err != nil {
			return nil, 0, err
		}
		a.StatusCounts = c
		a.InTransit = c.Arrived + c.Synced
		out = append(out, a)
	}
	return out, total, rows.Err()
}

// empresasCounterQuery lê do contador mantido empresa_counts (migração 00011) —
// instantâneo, sem o GROUP BY codigo_empresa sobre as 14M da notas (era ~30s, só
// cacheado). As chaves usam sentinela -1 p/ NULL (empresa/filial); o read traduz com
// NULLIF(coluna,-1). pendentes = itens não-terminais (espelha pendentes() do store em
// memória); como FILTER sobre sum() pode dar NULL, COALESCE p/ 0.
func empresasCounterQuery(f EmpresaFilter) (string, []any) {
	const pend = "COALESCE(sum(n) FILTER (WHERE status IN ('arrived','synced','pending_import','stuck')),0)"
	having := ""
	if f.PendentesOnly {
		having = "HAVING " + pend + " > 0"
	}
	// Ordena por código com NULL (sentinela -1 -> NULLIF) por último, espelhando o
	// NULLS LAST / codigoLess do store em memória.
	order := "NULLIF(codigo_empresa,-1) NULLS LAST, NULLIF(codigo_filial,-1) NULLS LAST"
	if f.Sort == "pendentes" {
		order = pend + " DESC, " + order
	}
	args := []any{}
	where := ""
	if f.Query != "" {
		args = append(args, "%"+f.Query+"%")
		// empresa_counts tem poucos milhares de linhas -> ILIKE direto é instantâneo.
		where = fmt.Sprintf("WHERE nome ILIKE $%d", len(args))
	}
	limit := ""
	if f.Limit > 0 {
		args = append(args, f.Limit, f.Offset)
		limit = fmt.Sprintf("LIMIT $%d OFFSET $%d", len(args)-1, len(args))
	}
	// count(*) OVER () é avaliado após GROUP BY/HAVING e antes do LIMIT, então dá
	// o total de empresas que casam o filtro (para paginação).
	return fmt.Sprintf(`
		SELECT NULLIF(codigo_empresa,-1),
		  NULLIF(codigo_filial,-1) AS fil,
		  COALESCE(max(nome), ''),
		  COALESCE(sum(n) FILTER (WHERE status='arrived'),0),
		  COALESCE(sum(n) FILTER (WHERE status='synced'),0),
		  COALESCE(sum(n) FILTER (WHERE status='imported'),0),
		  COALESCE(sum(n) FILTER (WHERE status='import_ignored'),0),
		  COALESCE(sum(n) FILTER (WHERE status='pending_import'),0),
		  COALESCE(sum(n) FILTER (WHERE status='stuck'),0),
		  COALESCE(sum(n) FILTER (WHERE status='lost'),0),
		  count(*) OVER ()
		FROM empresa_counts
		%s
		GROUP BY codigo_empresa, codigo_filial
		%s
		ORDER BY %s
		%s`, where, having, order, limit), args
}

// empresasFilteredQuery agrega por empresa direto da notas, restringindo a janela
// `col` (date_field) BETWEEN from/to. O contador empresa_counts não tem dimensão
// temporal, então a janela obriga o caminho ao vivo; o filtro de data usa o índice
// da coluna (ex.: idx_notas_imported) e corta o conjunto, então o GROUP BY roda sobre
// uma fração das 14M. Mesma semântica de data do GET /notas (>= from::date, <= to::date).
func empresasFilteredQuery(f EmpresaFilter, col string) (string, []any) {
	const pend = "count(*) FILTER (WHERE status IN ('arrived','synced','pending_import','stuck'))"
	having := ""
	if f.PendentesOnly {
		having = "HAVING " + pend + " > 0"
	}
	order := "codigo_empresa NULLS LAST, fil NULLS LAST"
	if f.Sort == "pendentes" {
		order = pend + " DESC, " + order
	}
	where := []string{}
	args := []any{}
	add := func(cond string, val any) {
		args = append(args, val)
		where = append(where, fmt.Sprintf(cond, len(args)))
	}
	if f.Query != "" {
		add("empresa_nome ILIKE $%d", "%"+f.Query+"%")
	}
	if f.From != "" {
		add(col+" >= $%d::date", f.From)
	}
	if f.To != "" {
		add(col+" <= $%d::date", f.To)
	}
	clause := ""
	if len(where) > 0 {
		clause = "WHERE " + strings.Join(where, " AND ")
	}
	limit := ""
	if f.Limit > 0 {
		args = append(args, f.Limit, f.Offset)
		limit = fmt.Sprintf("LIMIT $%d OFFSET $%d", len(args)-1, len(args))
	}
	// Notas sem empresa colapsam numa única linha "Sem empresa": fil forçada a NULL.
	return fmt.Sprintf(`
		SELECT codigo_empresa,
		  CASE WHEN codigo_empresa IS NULL THEN NULL ELSE codigo_filial END AS fil,
		  COALESCE(max(empresa_nome), ''),
		  count(*) FILTER (WHERE status='arrived'),
		  count(*) FILTER (WHERE status='synced'),
		  count(*) FILTER (WHERE status='imported'),
		  count(*) FILTER (WHERE status='import_ignored'),
		  count(*) FILTER (WHERE status='pending_import'),
		  count(*) FILTER (WHERE status='stuck'),
		  count(*) FILTER (WHERE status='lost'),
		  count(*) OVER ()
		FROM notas
		%s
		GROUP BY codigo_empresa, fil
		%s
		ORDER BY %s
		%s`, clause, having, order, limit), args
}

func (p *Postgres) ListNfseImport(ctx context.Context, f NfseFilter) ([]model.NfseImport, int, error) {
	where, args := []string{}, []any{}
	if f.Status != "" {
		args = append(args, string(f.Status))
		where = append(where, fmt.Sprintf("status = $%d::nota_status", len(args)))
	}
	clause := ""
	if len(where) > 0 {
		clause = " WHERE " + strings.Join(where, " AND ")
	}
	var total int
	if err := p.pool.QueryRow(ctx, `SELECT count(*) FROM nfse_import`+clause, args...).Scan(&total); err != nil {
		return nil, 0, err
	}
	limit := f.Limit
	if limit <= 0 {
		limit = 50
	}
	args = append(args, limit, f.Offset)
	q := `SELECT athenas_chave, status, motivo_ignorado, data_emissao FROM nfse_import` + clause +
		fmt.Sprintf(" ORDER BY data_inclusao DESC NULLS LAST LIMIT $%d OFFSET $%d", len(args)-1, len(args))
	rows, err := p.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	items := []model.NfseImport{}
	for rows.Next() {
		var it model.NfseImport
		var motivo *string
		var emissao *time.Time
		if err := rows.Scan(&it.AthenasChave, &it.Status, &motivo, &emissao); err != nil {
			return nil, 0, err
		}
		if motivo != nil {
			it.MotivoIgnorado = *motivo
		}
		if emissao != nil {
			s := emissao.Format("2006-01-02")
			it.DataEmissao = &s
		}
		items = append(items, it)
	}
	return items, total, rows.Err()
}

// ---- helpers ----

// addStatusN adds n to the counter for status s.
func addStatusN(c *model.StatusCounts, s model.NotaStatus, n int) {
	switch s {
	case model.StatusArrived:
		c.Arrived += n
	case model.StatusSynced:
		c.Synced += n
	case model.StatusImported:
		c.Imported += n
	case model.StatusImportIgnored:
		c.ImportIgnored += n
	case model.StatusPendingImport:
		c.PendingImport += n
	case model.StatusStuck:
		c.Stuck += n
	case model.StatusLost:
		c.Lost += n
	}
}

func f2i(f *float64) *int64 {
	if f == nil {
		return nil
	}
	v := int64(*f + 0.5)
	return &v
}

// rowScanner unifies pgx.Row and pgx.Rows for scanNota.
type rowScanner interface{ Scan(dest ...any) error }

const notaSelect = `
	SELECT chave_acesso, doc_type, status, codigo_empresa, codigo_filial,
	       arrived_at, synced_at, pending_at, imported_at, import_ignored, motivo_ignorado,
	       first_seen_at, last_update_at, lat_arrival_sync_s, lat_sync_import_s,
	       cnpj_emitente, emitente_nome, cnpj_destinatario, destinatario_nome, data_emissao, valor_total,
	       empresa_nome
	FROM notas`

func scanNota(r rowScanner) (model.Nota, error) {
	var n model.Nota
	var motivo, cnpjE, nomeE, cnpjD, nomeD, empNome *string
	var emissao *time.Time
	err := r.Scan(&n.ChaveAcesso, &n.DocType, &n.Status, &n.CodigoEmpresa, &n.CodigoFilial,
		&n.ArrivedAt, &n.SyncedAt, &n.PendingAt, &n.ImportedAt, &n.ImportIgnored, &motivo,
		&n.FirstSeenAt, &n.LastUpdateAt, &n.LatArrivalSyncS, &n.LatSyncImportS,
		&cnpjE, &nomeE, &cnpjD, &nomeD, &emissao, &n.ValorTotal, &empNome)
	if empNome != nil {
		n.NomeEmpresa = *empNome
	}
	if motivo != nil {
		n.MotivoIgnorado = *motivo
	}
	if cnpjE != nil {
		n.CnpjEmitente = *cnpjE
	}
	if nomeE != nil {
		n.NomeEmitente = *nomeE
	}
	if cnpjD != nil {
		n.CnpjDestinatario = *cnpjD
	}
	if nomeD != nil {
		n.NomeDestinatario = *nomeD
	}
	if emissao != nil {
		n.DataEmissao = emissao.Format("2006-01-02")
	}
	n.NumeroNota = model.NumeroNota(n.ChaveAcesso)
	return n, err
}

func upsertNota(ctx context.Context, tx pgx.Tx, n model.Nota) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO notas
		  (chave_acesso, doc_type, status, codigo_empresa, codigo_filial,
		   arrived_at, synced_at, imported_at, import_ignored, motivo_ignorado,
		   first_seen_at, last_update_at, lat_arrival_sync_s, lat_sync_import_s,
		   cnpj_emitente, emitente_nome, cnpj_destinatario, destinatario_nome, data_emissao, valor_total,
		   empresa_nome, pending_at)
		VALUES ($1,$2::doc_type,$3::nota_status,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,
		        $15,$16,$17,$18,$19::date,$20,$21,$22)
		ON CONFLICT (chave_acesso) DO UPDATE SET
		  pending_at=EXCLUDED.pending_at,
		  doc_type=EXCLUDED.doc_type, status=EXCLUDED.status,
		  codigo_empresa=COALESCE(EXCLUDED.codigo_empresa, notas.codigo_empresa),
		  codigo_filial=COALESCE(EXCLUDED.codigo_filial, notas.codigo_filial),
		  arrived_at=EXCLUDED.arrived_at, synced_at=EXCLUDED.synced_at,
		  imported_at=EXCLUDED.imported_at, import_ignored=EXCLUDED.import_ignored,
		  motivo_ignorado=EXCLUDED.motivo_ignorado, last_update_at=EXCLUDED.last_update_at,
		  lat_arrival_sync_s=EXCLUDED.lat_arrival_sync_s, lat_sync_import_s=EXCLUDED.lat_sync_import_s,
		  cnpj_emitente=COALESCE(EXCLUDED.cnpj_emitente, notas.cnpj_emitente),
		  emitente_nome=COALESCE(EXCLUDED.emitente_nome, notas.emitente_nome),
		  cnpj_destinatario=COALESCE(EXCLUDED.cnpj_destinatario, notas.cnpj_destinatario),
		  destinatario_nome=COALESCE(EXCLUDED.destinatario_nome, notas.destinatario_nome),
		  data_emissao=COALESCE(EXCLUDED.data_emissao, notas.data_emissao),
		  valor_total=COALESCE(EXCLUDED.valor_total, notas.valor_total),
		  empresa_nome=COALESCE(EXCLUDED.empresa_nome, notas.empresa_nome)`,
		n.ChaveAcesso, docTypeOrDefault(n.DocType), string(n.Status), n.CodigoEmpresa, n.CodigoFilial,
		n.ArrivedAt, n.SyncedAt, n.ImportedAt, n.ImportIgnored, nullStr(n.MotivoIgnorado),
		n.FirstSeenAt, n.LastUpdateAt, n.LatArrivalSyncS, n.LatSyncImportS,
		nullStr(n.CnpjEmitente), nullStr(n.NomeEmitente), nullStr(n.CnpjDestinatario),
		nullStr(n.NomeDestinatario), nullStr(n.DataEmissao), n.ValorTotal, nullStr(n.NomeEmpresa),
		n.PendingAt)
	return err
}

func (p *Postgres) UpsertHeartbeat(ctx context.Context, service string, payload map[string]any) error {
	b, _ := json.Marshal(payload)
	_, err := p.pool.Exec(ctx, `
		INSERT INTO service_heartbeats (service, last_beat, payload)
		VALUES ($1, NOW(), $2::jsonb)
		ON CONFLICT (service) DO UPDATE
		  SET last_beat = NOW(), payload = EXCLUDED.payload`,
		service, string(b))
	return err
}

func (p *Postgres) GetStatus(ctx context.Context) ([]model.ServiceStatus, error) {
	rows, err := p.pool.Query(ctx,
		`SELECT service, last_beat, payload FROM service_heartbeats ORDER BY service`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	now := time.Now()
	var out []model.ServiceStatus
	for rows.Next() {
		var s model.ServiceStatus
		var rawPayload []byte
		if err := rows.Scan(&s.Service, &s.LastBeat, &rawPayload); err != nil {
			return nil, err
		}
		s.SecondsAgo = int64(now.Sub(s.LastBeat).Seconds())
		s.Online = s.SecondsAgo < 300
		_ = json.Unmarshal(rawPayload, &s.Payload)
		out = append(out, s)
	}
	return out, rows.Err()
}

func loadObservations(ctx context.Context, q querier, chave string) ([]model.Observation, error) {
	rows, err := q.Query(ctx, `
		SELECT id, chave_acesso, stage, event_type, observed_at, ingested_at, source,
		       doc_type, file_path, codigo_empresa, codigo_filial, payload,
		       cnpj_emitente, nome_emitente, cnpj_destinatario, nome_destinatario, data_emissao, valor_total,
		       empresa_nome
		FROM observations WHERE chave_acesso = $1 ORDER BY observed_at`, chave)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.Observation
	for rows.Next() {
		var o model.Observation
		var filePath, cnpjE, nomeE, cnpjD, nomeD, empNome *string
		var emissao *time.Time
		var payload []byte
		if err := rows.Scan(&o.ID, &o.ChaveAcesso, &o.Stage, &o.EventType, &o.ObservedAt,
			&o.IngestedAt, &o.Source, &o.DocType, &filePath, &o.CodigoEmpresa, &o.CodigoFilial, &payload,
			&cnpjE, &nomeE, &cnpjD, &nomeD, &emissao, &o.ValorTotal, &empNome); err != nil {
			return nil, err
		}
		if empNome != nil {
			o.NomeEmpresa = *empNome
		}
		if filePath != nil {
			o.FilePath = *filePath
		}
		if cnpjE != nil {
			o.CnpjEmitente = *cnpjE
		}
		if nomeE != nil {
			o.NomeEmitente = *nomeE
		}
		if cnpjD != nil {
			o.CnpjDestinatario = *cnpjD
		}
		if nomeD != nil {
			o.NomeDestinatario = *nomeD
		}
		if emissao != nil {
			o.DataEmissao = emissao.Format("2006-01-02")
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
