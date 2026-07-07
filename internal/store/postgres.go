package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
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
	// Prioridade da rotação do poller (ListInflightChaves): uma fração do lote vai
	// para as notas QUENTES (synced_at recente — as com chance real de importar já),
	// o resto rotaciona o backlog frio por LRU. hotWindow<=0 ou hotFraction<=0
	// desliga (LRU puro, comportamento antigo).
	hotWindow   time.Duration
	hotFraction float64
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
	return &Postgres{pool: pool, hotWindow: 48 * time.Hour, hotFraction: 0.7}, nil
}

// SetPollPriority ajusta a priorização da rotação do poller: window define o que é
// "quente" (synced_at nos últimos window) e fraction a fatia do lote reservada a
// elas (0..1). window==0 ou fraction<=0 desliga a priorização (LRU puro).
func (p *Postgres) SetPollPriority(window time.Duration, fraction float64) {
	p.hotWindow = window
	if fraction > 1 {
		fraction = 1
	}
	p.hotFraction = fraction
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

// notaWhere monta as condições WHERE (numeradas $1..$N) a partir do NotaFilter.
// Compartilhado por ListNotas e SummaryNotas para os filtros ficarem SEMPRE idênticos.
func notaWhere(f NotaFilter) (where []string, args []any) {
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
	if f.Numero != "" {
		// Match por PREFIXO do número da nota (nNF). A expressão é idêntica à do índice
		// idx_notas_numero (migração 00012, text_pattern_ops) -> LIKE 'prefixo%' usa o
		// índice em vez de varrer as 21M. Curinga só à direita (prefixo), não no meio.
		add(numeroExpr+" LIKE $%d", f.Numero+"%")
	}
	if f.Direction != "" {
		add("direction = $%d", f.Direction)
	}
	if col := dateColumn(f.DateField); col != "" {
		if f.From != "" {
			add(col+" >= $%d::date", f.From)
		}
		if f.To != "" {
			add(col+" <= $%d::date", f.To)
		}
	}
	return where, args
}

// SummaryNotas devolve, para o MESMO filtro de ListNotas, quantas notas casam e a soma
// de valor_total (para apuração — ex.: total de NFC-e do período). Um count(*)+sum() em
// vez da lista paginada.
func (p *Postgres) SummaryNotas(ctx context.Context, f NotaFilter) (model.NotaSummary, error) {
	var s model.NotaSummary
	where, args := notaWhere(f)
	clause := ""
	if len(where) > 0 {
		clause = " WHERE " + strings.Join(where, " AND ")
	}
	err := p.pool.QueryRow(ctx,
		`SELECT count(*), COALESCE(sum(valor_total),0)::float8 FROM notas`+clause, args...).
		Scan(&s.Count, &s.ValorTotal)
	return s, err
}

func (p *Postgres) ListNotas(ctx context.Context, f NotaFilter) ([]model.Nota, int, error) {
	where, args := notaWhere(f)
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

// ListInflightChaves returns in-flight chaves to poll and stamps them as polled now.
// Com priorização ativa (hotWindow/hotFraction), o lote é dividido em duas filas:
// QUENTE = synced_at recente (chance real de importar/ignorar agora — detecta a
// transição em 1-2 ciclos em vez de esperar a rotação dos ~2M) e FRIA = o resto do
// backlog, por LRU (garante que TUDO continua sendo revisitado — nada morre de fome:
// a fatia fria é fixa por ciclo). Cota quente não usada transborda pra fria.
// Sem priorização, LRU puro sobre tudo (comportamento antigo).
func (p *Postgres) ListInflightChaves(ctx context.Context, limit int) ([]string, error) {
	if limit <= 0 {
		limit = 1000
	}
	hotQuota := int(float64(limit) * p.hotFraction)
	if p.hotWindow <= 0 || hotQuota <= 0 {
		return p.pickInflight(ctx, `TRUE`, limit, nil) // LRU puro
	}
	hot, err := p.pickInflight(ctx, `synced_at >= now() - $2::interval`, hotQuota, &p.hotWindow)
	if err != nil {
		return nil, err
	}
	cold, err := p.pickInflight(ctx, `(synced_at IS NULL OR synced_at < now() - $2::interval)`,
		limit-len(hot), &p.hotWindow)
	if err != nil {
		return nil, err
	}
	return append(hot, cold...), nil
}

// pickInflight seleciona até limit chaves in-flight que casam cond (LRU por
// last_polled_at) e as carimba como recém-polladas. cond usa $2 quando window!=nil.
// FOR UPDATE SKIP LOCKED evita corrida entre pollers concorrentes. As duas chamadas
// (quente/fria) usam condições disjuntas, então não há dupla seleção no mesmo ciclo.
func (p *Postgres) pickInflight(ctx context.Context, cond string, limit int, window *time.Duration) ([]string, error) {
	if limit <= 0 {
		return nil, nil
	}
	q := fmt.Sprintf(`
		WITH picked AS (
		  SELECT chave_acesso FROM notas
		  WHERE status IN ('arrived','synced','pending_import') AND %s
		  ORDER BY last_polled_at ASC NULLS FIRST
		  LIMIT $1
		  FOR UPDATE SKIP LOCKED
		)
		UPDATE notas n SET last_polled_at = now()
		FROM picked WHERE n.chave_acesso = picked.chave_acesso
		RETURNING n.chave_acesso`, cond)
	args := []any{limit}
	if window != nil {
		// interval em sintaxe do Postgres ("172800 seconds") — o String() do Go
		// ("48h0m0s") não é um interval válido.
		args = append(args, fmt.Sprintf("%d seconds", int64(window.Seconds())))
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

// overviewWhere monta o WHERE do recompute ao vivo do overview a partir da janela de
// data (date_field BETWEEN from/to) + filtros de empresa/filial/doc_type. Sempre
// retorna ao menos "TRUE" para compor com segurança.
func overviewWhere(f OverviewFilter) (string, []any) {
	where := []string{}
	args := []any{}
	add := func(cond string, val any) {
		args = append(args, val)
		where = append(where, fmt.Sprintf(cond, len(args)))
	}
	if col := dateColumn(f.DateField); col != "" {
		if f.From != "" {
			add(col+" >= $%d::date", f.From)
		}
		if f.To != "" {
			add(col+" <= $%d::date", f.To)
		}
	}
	if f.CodigoEmpresa != nil {
		add("codigo_empresa = $%d", *f.CodigoEmpresa)
	}
	if f.CodigoFilial != nil {
		add("codigo_filial = $%d", *f.CodigoFilial)
	}
	if f.DocType != "" {
		add("doc_type = $%d::doc_type", string(f.DocType))
	}
	if len(where) == 0 {
		return "TRUE", nil
	}
	return strings.Join(where, " AND "), args
}

func (p *Postgres) Overview(ctx context.Context, f OverviewFilter) (model.Overview, error) {
	var ov model.Overview
	// Contagem por status. Sem janela/empresa: do contador mantido (notas_counts,
	// migrações 00008/00014) — instantâneo, sem escanear as 21M; doc_type é dimensão
	// do contador, então filtra ali mesmo. Com janela de data e/ou empresa/filial:
	// recompute ao vivo das notas no recorte (dimensões que o contador não tem),
	// agrupando pelo status ATUAL. mode="flow" sinaliza ao front que é recorte por
	// janela, não estoque global.
	countSQL := `SELECT status, sum(n)::bigint FROM notas_counts GROUP BY status`
	var countArgs []any
	switch {
	case f.live():
		where, args := overviewWhere(f)
		countSQL = `SELECT status, count(*) FROM notas WHERE ` + where + ` GROUP BY status`
		countArgs = args
		if f.windowed() {
			ov.Mode = "flow"
		}
	case f.DocType != "":
		countSQL = `SELECT status, sum(n)::bigint FROM notas_counts WHERE doc_type = $1::doc_type GROUP BY status`
		countArgs = []any{string(f.DocType)}
	}
	rows, err := p.pool.Query(ctx, countSQL, countArgs...)
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

	// Latências (p50/p95) foram REMOVIDAS deste card. Nesta base o percentile_cont não é
	// computável de forma barata E confiável: quase tudo foi sincronizado nos últimos 30d
	// (backfill), então a janela casa dezenas de milhões e o percentil ordena tudo (medido:
	// 3m49s censurado, 5m36s só-concluídas, >180s timeout -> o /metrics/overview estourava
	// e o painel ficava VAZIO); amostrar (LIMIT) também não resolveu porque o sync->import
	// fica majoritariamente NEGATIVO (imported_at < synced_at: o Athenas importou antes de
	// o agente observar o sync, típico do backfill). A "espera real / backlog travado" é
	// mostrada, rápida e honesta, pelo GET /metrics/aging (contagem por faixa). Os campos
	// Lat*P50/P95S ficam nil (omitempty). Reintroduzir depois barato (agregado mantido, ou
	// pós-backfill excluindo lat<0).
	ov.InTransit = ov.Arrived + ov.Synced
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

// Aging separa o backlog pendente em duas esperas, cada uma datada pelo evento que
// iniciou a espera atual: to_sync (status arrived, idade desde arrived_at) e to_import
// (status synced/pending_import, idade desde synced_at). COALESCE com pending_at/
// first_seen_at cobre notas vistas só pelo poller (sem arrived/synced gravado).
func (p *Postgres) Aging(ctx context.Context, f AgingFilter) (model.Aging, error) {
	out := model.Aging{
		AnchorToSync:   "arrived_at",
		AnchorToImport: "synced_at",
		ToSync:         orderedAging(nil),
		ToImport:       orderedAging(nil),
	}
	toSync, err := p.agingBuckets(ctx, f,
		"status = 'arrived'::nota_status",
		"now() - COALESCE(arrived_at, first_seen_at)")
	if err != nil {
		return out, err
	}
	toImport, err := p.agingBuckets(ctx, f,
		"status IN ('synced','pending_import')",
		"now() - COALESCE(synced_at, pending_at, first_seen_at)")
	if err != nil {
		return out, err
	}
	out.ToSync, out.ToImport = orderedAging(toSync), orderedAging(toImport)
	return out, nil
}

// agingBuckets conta as notas que casam statusCond + filtros, agrupadas pela faixa de
// idade de ageExpr (ambos interpolados de literais internos — nunca de input).
func (p *Postgres) agingBuckets(ctx context.Context, f AgingFilter, statusCond, ageExpr string) (map[string]int, error) {
	where := []string{statusCond}
	args := []any{}
	add := func(cond string, val any) {
		args = append(args, val)
		where = append(where, fmt.Sprintf(cond, len(args)))
	}
	if f.CodigoEmpresa != nil {
		add("codigo_empresa = $%d", *f.CodigoEmpresa)
	}
	if f.CodigoFilial != nil {
		add("codigo_filial = $%d", *f.CodigoFilial)
	}
	if f.DocType != "" {
		add("doc_type = $%d::doc_type", string(f.DocType))
	}
	if f.Direction != "" {
		add("direction = $%d", f.Direction)
	}
	q := fmt.Sprintf(`
		SELECT CASE
		         WHEN age < interval '1 day'   THEN '<1d'
		         WHEN age < interval '3 days'  THEN '1-3d'
		         WHEN age < interval '7 days'  THEN '3-7d'
		         WHEN age < interval '30 days' THEN '7-30d'
		         ELSE '>30d'
		       END AS label, count(*)
		FROM (SELECT %s AS age FROM notas WHERE %s) s
		GROUP BY 1`, ageExpr, strings.Join(where, " AND "))
	rows, err := p.pool.Query(ctx, q, args...)
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
	return counts, rows.Err()
}

// FilialCNPJ associa (codigo_empresa, codigo_filial) ao CNPJ da filial (vindo do
// Athenas/TABFILIAL), para o backfill retroativo da direção.
type FilialCNPJ struct {
	CodigoEmpresa int
	CodigoFilial  int
	Cnpj          string
}

// BackfillDirection preenche notas.direction (onde ainda é NULL) a partir do mapa de
// CNPJ por filial: compara a raiz-8 do CNPJ da filial com a do emitente/destinatário
// já gravados na nota — 'saida' se casa o emitente, 'entrada' se casa o destinatário
// (mesma precedência de model.DirectionFromCNPJs). One-off, idempotente (só toca
// direction IS NULL) e não dispara os triggers de contador (não mexe em status/empresa).
// O root8 é computado em SQL nos dois lados (uma única fonte da verdade). Retorna
// quantas notas foram classificadas.
// ctidBlockChunk é quantas páginas de 8KB cada lote do backfill cobre. ~10k páginas
// ≈ 80MB de heap por lote (alguns 100k de notas) — granularidade boa de progresso/commit.
const ctidBlockChunk = 10000

func (p *Postgres) BackfillDirection(ctx context.Context, filiais []FilialCNPJ, onProgress func(done, total int, affected int64)) (int64, error) {
	// Estratégia: UMA passada pelo heap em faixas de ctid (Tid Range Scan), juntando uma
	// tabelinha temporária de filiais (raiz-8 do CNPJ). Por que não "um UPDATE por filial":
	// para as filiais GRANDES o planner ignora idx_notas_empresa e faz seq scan das 21M,
	// então N filiais grandes viravam N varreduras completas. Varrer por ctid percorre
	// cada página UMA vez e commita por lote (sem transação longa, resumível pois só toca
	// direction IS NULL, observável via onProgress). A raiz-8 da filial é calculada em Go;
	// a do emitente/destinatário em SQL (mesma normalização).
	conn, err := p.pool.Acquire(ctx) // conexão fixa: a TEMP table vive enquanto ela existir
	if err != nil {
		return 0, err
	}
	defer conn.Release()

	emp := make([]int32, 0, len(filiais))
	fil := make([]int32, 0, len(filiais))
	root := make([]string, 0, len(filiais))
	for _, f := range filiais {
		r := digitsPrefix(f.Cnpj, 8)
		if len(r) < 8 {
			continue // sem raiz válida -> não casa
		}
		emp = append(emp, int32(f.CodigoEmpresa))
		fil = append(fil, int32(f.CodigoFilial))
		root = append(root, r)
	}
	// TEMP sem ON COMMIT DROP: precisa sobreviver aos commits de cada lote (os Exec abaixo
	// autocommitam). É dropada ao soltar a conexão.
	if _, err := conn.Exec(ctx,
		`CREATE TEMP TABLE _fil (codigo_empresa int, codigo_filial int, root8 text)`); err != nil {
		return 0, err
	}
	if _, err := conn.Exec(ctx,
		`INSERT INTO _fil SELECT * FROM unnest($1::int[], $2::int[], $3::text[])`, emp, fil, root); err != nil {
		return 0, err
	}

	// Nº real de páginas (tamanho do arquivo / block_size) — sempre atual, não depende de
	// ANALYZE (relpages do pg_class poderia estar defasado e cortar o fim da tabela).
	var pages int64
	if err := conn.QueryRow(ctx,
		`SELECT pg_relation_size('notas') / current_setting('block_size')::bigint`).Scan(&pages); err != nil {
		return 0, err
	}

	const q = `
		UPDATE notas n SET direction = CASE
		    WHEN left(regexp_replace(coalesce(n.cnpj_emitente,''), '[^0-9]', '', 'g'), 8) = f.root8 THEN 'saida'
		    ELSE 'entrada' END
		FROM _fil f
		WHERE n.ctid >= $1::tid AND n.ctid < $2::tid
		  AND n.codigo_empresa = f.codigo_empresa AND n.codigo_filial = f.codigo_filial
		  AND n.direction IS NULL
		  AND ( left(regexp_replace(coalesce(n.cnpj_emitente,''), '[^0-9]', '', 'g'), 8) = f.root8
		     OR left(regexp_replace(coalesce(n.cnpj_destinatario,''), '[^0-9]', '', 'g'), 8) = f.root8 )`
	var total int64
	for lo := int64(0); lo <= pages; lo += ctidBlockChunk {
		hi := lo + ctidBlockChunk
		tag, err := conn.Exec(ctx, q, fmt.Sprintf("(%d,0)", lo), fmt.Sprintf("(%d,0)", hi))
		if err != nil {
			return total, fmt.Errorf("bloco de páginas [%d,%d): %w", lo, hi, err)
		}
		total += tag.RowsAffected()
		if onProgress != nil {
			done := hi
			if done > pages {
				done = pages
			}
			onProgress(int(done), int(pages), total)
		}
	}
	return total, nil
}

// StatusForChaves retorna o status derivado atual de cada chave dada (as ausentes na
// notas simplesmente não aparecem no mapa). Chunka o IN para não estourar parâmetros.
// Usado pelo reconcile para rotular por que uma chave "faltou" (arrived/synced/...).
func (p *Postgres) StatusForChaves(ctx context.Context, chaves []string) (map[string]model.NotaStatus, error) {
	out := make(map[string]model.NotaStatus, len(chaves))
	const chunk = 1000
	for start := 0; start < len(chaves); start += chunk {
		end := start + chunk
		if end > len(chaves) {
			end = len(chaves)
		}
		rows, err := p.pool.Query(ctx,
			`SELECT chave_acesso, status FROM notas WHERE chave_acesso = ANY($1)`, chaves[start:end])
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var c string
			var s model.NotaStatus
			if err := rows.Scan(&c, &s); err != nil {
				rows.Close()
				return nil, err
			}
			out[c] = s
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, err
		}
		rows.Close()
	}
	return out, nil
}

// ImportedChavesBetween retorna as chaves com status=imported cujo imported_at cai na
// janela [since, until), opcionalmente de uma empresa. É o lado "tracker" do reconcile
// por TABLISTACHAVEACESSO (mesma janela de DATAINCLUSAO no Athenas).
func (p *Postgres) ImportedChavesBetween(ctx context.Context, since, until time.Time, codigoEmpresa, codigoFilial *int) ([]string, error) {
	q := `SELECT chave_acesso FROM notas WHERE status='imported' AND imported_at >= $1 AND imported_at < $2`
	args := []any{since, until}
	if codigoEmpresa != nil {
		args = append(args, *codigoEmpresa)
		q += fmt.Sprintf(" AND codigo_empresa = $%d", len(args))
	}
	if codigoFilial != nil {
		args = append(args, *codigoFilial)
		q += fmt.Sprintf(" AND codigo_filial = $%d", len(args))
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

// digitsPrefix retorna os primeiros n dígitos de s (ignorando não-dígitos). Usado p/
// extrair a raiz do CNPJ da filial no backfill da direção.
func digitsPrefix(s string, n int) string {
	b := make([]byte, 0, n)
	for i := 0; i < len(s) && len(b) < n; i++ {
		if s[i] >= '0' && s[i] <= '9' {
			b = append(b, s[i])
		}
	}
	return string(b)
}

// Latency implementa o GET /metrics/latency. Duas medições sobre a janela dos últimos
// `days` dias (limites por current_date, alinhados aos valores date-only):
//
//   - chegada→sync: percentis de lat_arrival_sync_s (timestamps reais do agente) das
//     notas com synced_at na janela, geral + por dia BRT. Percentil por janela de dias
//     é barato (~30-100k linhas/dia via índice) — o desastre do overview (#35) era
//     percentil sobre TODA a base. lat<0 é excluída (artefato de backfill).
//   - sync→import: distribuição em DIAS (mesmo dia/D+1/D+2+) das notas com imported_at
//     na janela. NUNCA percentil em segundos aqui: imported_at é date-only (meia-noite
//     BRT), então "mesmo dia" daria negativo. diff<=0 conta como mesmo dia.
func (p *Postgres) Latency(ctx context.Context, days int) (model.Latency, error) {
	out := model.Latency{Days: days, TZ: tzSaoPaulo, ArrivalToSync: model.LatencyArrivalSync{Daily: []model.LatencyDaily{}}}

	// chegada→sync: por dia BRT do synced_at.
	daily, err := p.pool.Query(ctx, fmt.Sprintf(`
		SELECT (synced_at AT TIME ZONE '%s')::date::text,
		       count(*),
		       percentile_cont(0.5)  WITHIN GROUP (ORDER BY lat_arrival_sync_s),
		       percentile_cont(0.95) WITHIN GROUP (ORDER BY lat_arrival_sync_s)
		FROM notas
		WHERE synced_at >= current_date - $1::int
		  AND lat_arrival_sync_s IS NOT NULL AND lat_arrival_sync_s >= 0
		GROUP BY 1 ORDER BY 1`, tzSaoPaulo), days)
	if err != nil {
		return out, err
	}
	for daily.Next() {
		var d model.LatencyDaily
		if err := daily.Scan(&d.Date, &d.Count, &d.P50S, &d.P95S); err != nil {
			daily.Close()
			return out, err
		}
		out.ArrivalToSync.Daily = append(out.ArrivalToSync.Daily, d)
		out.ArrivalToSync.Count += d.Count
	}
	daily.Close()
	if err := daily.Err(); err != nil {
		return out, err
	}
	if out.ArrivalToSync.Count > 0 {
		// geral da janela (percentil não agrega de percentis diários — segunda query).
		var p50, p95 float64
		if err := p.pool.QueryRow(ctx, `
			SELECT percentile_cont(0.5)  WITHIN GROUP (ORDER BY lat_arrival_sync_s),
			       percentile_cont(0.95) WITHIN GROUP (ORDER BY lat_arrival_sync_s)
			FROM notas
			WHERE synced_at >= current_date - $1::int
			  AND lat_arrival_sync_s IS NOT NULL AND lat_arrival_sync_s >= 0`, days).
			Scan(&p50, &p95); err != nil {
			return out, err
		}
		out.ArrivalToSync.P50S, out.ArrivalToSync.P95S = &p50, &p95
	}

	// sync→import: dias corridos entre os dias BRT de sync e import.
	si := &out.SyncToImport
	if err := p.pool.QueryRow(ctx, fmt.Sprintf(`
		SELECT count(*),
		       count(*) FILTER (WHERE dd <= 0),
		       count(*) FILTER (WHERE dd  = 1),
		       count(*) FILTER (WHERE dd >= 2)
		FROM (
		  SELECT (imported_at AT TIME ZONE '%[1]s')::date - (synced_at AT TIME ZONE '%[1]s')::date AS dd
		  FROM notas
		  WHERE imported_at >= current_date - $1::int AND synced_at IS NOT NULL
		) t`, tzSaoPaulo), days).
		Scan(&si.Count, &si.SameDay, &si.D1, &si.D2Plus); err != nil {
		return out, err
	}
	if si.Count > 0 {
		pct := func(n int) float64 { return math.Round(10000*float64(n)/float64(si.Count)) / 100 }
		si.SameDayPct, si.D1Pct, si.D2PlusPct = pct(si.SameDay), pct(si.D1), pct(si.D2Plus)
	}
	return out, nil
}

// orderedAging monta as faixas do aging na ordem canônica (incluindo vazias=0), com
// MaxDays no limite superior (nil na faixa aberta ">30d").
func orderedAging(counts map[string]int) []model.AgingBucket {
	out := make([]model.AgingBucket, 0, len(model.AgingBuckets))
	for _, b := range model.AgingBuckets {
		ab := model.AgingBucket{Label: b.Label, Count: counts[b.Label]}
		if b.MaxDays > 0 {
			md := b.MaxDays
			ab.MaxDays = &md
		}
		out = append(out, ab)
	}
	return out
}

func (p *Postgres) Empresas(ctx context.Context, f EmpresaFilter) ([]model.EmpresaAgg, int, error) {
	// Sem janela de data -> lê do contador (instantâneo). Com date_field+from/to ->
	// recomputa ao vivo da notas (o contador não tem dimensão temporal). Ambos os
	// caminhos produzem as MESMAS colunas, na mesma ordem, p/ o scan ser compartilhado.
	var q string
	var args []any
	// Recompute ao vivo da notas SÓ quando há janela de data — a única dimensão que o
	// contador não tem. doc_type e direction são dimensões do empresa_counts desde a
	// migração 00014, então filtram no próprio contador (instantâneo).
	hasWindow := dateColumn(f.DateField) != "" && (f.From != "" || f.To != "")
	if hasWindow {
		q, args = empresasFilteredQuery(f)
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

// empresasCounterQuery lê do contador mantido empresa_counts (migrações 00011/00014)
// — instantâneo, sem o GROUP BY codigo_empresa sobre as 21M da notas (era ~30s, só
// cacheado). As chaves usam sentinela -1 p/ NULL (empresa/filial) e '' p/ direção
// ausente; o read traduz empresa/filial com NULLIF(coluna,-1), e doc_type/direction
// filtram por valor exato (a sentinela '' nunca casa 'entrada'/'saida'). pendentes =
// itens não-terminais (espelha pendentes() do store em memória); como FILTER sobre
// sum() pode dar NULL, COALESCE p/ 0.
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
	conds := []string{}
	if f.Query != "" {
		args = append(args, "%"+f.Query+"%")
		// empresa_counts tem dezenas de milhares de linhas -> ILIKE direto é instantâneo.
		conds = append(conds, fmt.Sprintf("nome ILIKE $%d", len(args)))
	}
	if f.DocType != "" {
		args = append(args, string(f.DocType))
		conds = append(conds, fmt.Sprintf("doc_type = $%d::doc_type", len(args)))
	}
	if f.Direction != "" {
		args = append(args, f.Direction)
		conds = append(conds, fmt.Sprintf("direction = $%d", len(args)))
	}
	where := ""
	if len(conds) > 0 {
		where = "WHERE " + strings.Join(conds, " AND ")
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

// empresasFilteredQuery agrega por empresa direto da notas, restringindo por janela
// de data (date_field BETWEEN from/to) e/ou doc_type. O contador empresa_counts não
// tem dimensão temporal nem de tipo, então qualquer um desses obriga o caminho ao vivo;
// o filtro de data usa o índice da coluna (ex.: idx_notas_imported) e corta o conjunto,
// então o GROUP BY roda sobre uma fração das 14M. Mesma semântica de data do GET /notas
// (>= from::date, <= to::date).
func empresasFilteredQuery(f EmpresaFilter) (string, []any) {
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
	if f.DocType != "" {
		add("doc_type = $%d::doc_type", string(f.DocType))
	}
	if f.Direction != "" {
		add("direction = $%d", f.Direction)
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

// numeroExpr extrai o número da nota (nNF) da chave: 9 dígitos nas posições 26–34,
// sem zeros à esquerda (espelha model.NumeroNota). DEVE ser idêntica à expressão do
// índice idx_notas_numero (migração 00012) p/ o planner usar o índice no LIKE de prefixo.
const numeroExpr = `ltrim(substring(chave_acesso from 26 for 9), '0')`

const notaSelect = `
	SELECT chave_acesso, doc_type, status, codigo_empresa, codigo_filial,
	       arrived_at, synced_at, pending_at, imported_at, import_ignored, motivo_ignorado,
	       first_seen_at, last_update_at, lat_arrival_sync_s, lat_sync_import_s,
	       cnpj_emitente, emitente_nome, cnpj_destinatario, destinatario_nome, data_emissao, valor_total,
	       empresa_nome, direction
	FROM notas`

func scanNota(r rowScanner) (model.Nota, error) {
	var n model.Nota
	var motivo, cnpjE, nomeE, cnpjD, nomeD, empNome, dir *string
	var emissao *time.Time
	err := r.Scan(&n.ChaveAcesso, &n.DocType, &n.Status, &n.CodigoEmpresa, &n.CodigoFilial,
		&n.ArrivedAt, &n.SyncedAt, &n.PendingAt, &n.ImportedAt, &n.ImportIgnored, &motivo,
		&n.FirstSeenAt, &n.LastUpdateAt, &n.LatArrivalSyncS, &n.LatSyncImportS,
		&cnpjE, &nomeE, &cnpjD, &nomeD, &emissao, &n.ValorTotal, &empNome, &dir)
	if empNome != nil {
		n.NomeEmpresa = *empNome
	}
	if dir != nil {
		n.Direction = *dir
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
		   empresa_nome, pending_at, direction)
		VALUES ($1,$2::doc_type,$3::nota_status,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,
		        $15,$16,$17,$18,$19::date,$20,$21,$22,$23)
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
		  empresa_nome=COALESCE(EXCLUDED.empresa_nome, notas.empresa_nome),
		  direction=COALESCE(EXCLUDED.direction, notas.direction)`,
		n.ChaveAcesso, docTypeOrDefault(n.DocType), string(n.Status), n.CodigoEmpresa, n.CodigoFilial,
		n.ArrivedAt, n.SyncedAt, n.ImportedAt, n.ImportIgnored, nullStr(n.MotivoIgnorado),
		n.FirstSeenAt, n.LastUpdateAt, n.LatArrivalSyncS, n.LatSyncImportS,
		nullStr(n.CnpjEmitente), nullStr(n.NomeEmitente), nullStr(n.CnpjDestinatario),
		nullStr(n.NomeDestinatario), nullStr(n.DataEmissao), n.ValorTotal, nullStr(n.NomeEmpresa),
		n.PendingAt, nullStr(n.Direction))
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
