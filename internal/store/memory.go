package store

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/EnzzoHosaki/rps-xml-tracker/internal/derive"
	"github.com/EnzzoHosaki/rps-xml-tracker/internal/model"
)

// Memory is an in-memory Store for tests and local smoke runs. It keeps the
// append-only observations and derives notas on read — mirroring how the
// Postgres impl will behave (observations are the source of truth).
type Memory struct {
	mu         sync.RWMutex
	obs        []model.Observation
	seen       map[string]struct{}  // dedup keys
	lastPolled map[string]time.Time // chave -> última vez checada pelo poller
	nextID     int64
}

func NewMemory() *Memory {
	return &Memory{seen: map[string]struct{}{}, lastPolled: map[string]time.Time{}}
}

func (m *Memory) AppendObservations(_ context.Context, obs []model.Observation) (int, int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	accepted, rejected := 0, 0
	for _, o := range obs {
		k := DedupKey(o)
		if _, dup := m.seen[k]; dup {
			rejected++
			continue
		}
		m.seen[k] = struct{}{}
		m.nextID++
		o.ID = m.nextID
		if o.IngestedAt.IsZero() {
			o.IngestedAt = time.Now()
		}
		m.obs = append(m.obs, o)
		accepted++
	}
	return accepted, rejected, nil
}

func (m *Memory) GetNota(_ context.Context, chave string) (model.NotaDetail, bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	spans := m.spansFor(chave)
	if len(spans) == 0 {
		return model.NotaDetail{}, false, nil
	}
	return model.NotaDetail{Nota: derive.Nota(chave, spans), Spans: spans}, true, nil
}

func (m *Memory) ListNotas(_ context.Context, f NotaFilter) ([]model.Nota, int, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// group observations by chave, derive each
	byChave := map[string][]model.Observation{}
	for _, o := range m.obs {
		byChave[o.ChaveAcesso] = append(byChave[o.ChaveAcesso], o)
	}
	var all []model.Nota
	for chave, spans := range byChave {
		n := derive.Nota(chave, spans)
		if !matches(n, f) {
			continue
		}
		all = append(all, n)
	}
	// stable order: most-recently-updated first
	sort.SliceStable(all, func(i, j int) bool {
		return all[i].LastUpdateAt.After(all[j].LastUpdateAt)
	})

	total := len(all)
	lo := clamp(f.Offset, 0, total)
	hi := total
	if f.Limit > 0 {
		hi = clamp(lo+f.Limit, lo, total)
	}
	return all[lo:hi], total, nil
}

func (m *Memory) ListInflightChaves(_ context.Context, limit int) ([]string, error) {
	m.mu.Lock() // Lock (não RLock): também atualiza lastPolled (rotação)
	defer m.mu.Unlock()
	byChave := map[string][]model.Observation{}
	for _, o := range m.obs {
		byChave[o.ChaveAcesso] = append(byChave[o.ChaveAcesso], o)
	}
	var inflight []string
	for chave, spans := range byChave {
		s := derive.Nota(chave, spans).Status
		if s == model.StatusArrived || s == model.StatusSynced || s == model.StatusPendingImport {
			inflight = append(inflight, chave)
		}
	}
	// menos recentemente checadas primeiro (zero-value = nunca checada = primeiro)
	sort.Slice(inflight, func(i, j int) bool {
		return m.lastPolled[inflight[i]].Before(m.lastPolled[inflight[j]])
	})
	if limit > 0 && len(inflight) > limit {
		inflight = inflight[:limit]
	}
	now := time.Now()
	for _, c := range inflight {
		m.lastPolled[c] = now
	}
	return inflight, nil
}

func (m *Memory) ListChavesByStatus(_ context.Context, status model.NotaStatus, limit, offset int) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []string
	for _, n := range m.allNotas() {
		if n.Status == status {
			out = append(out, n.ChaveAcesso)
		}
	}
	sort.Strings(out) // ordem estável (allNotas itera um map)
	if offset >= len(out) {
		return []string{}, nil
	}
	out = out[offset:]
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (m *Memory) DeleteImportIgnoredObs(_ context.Context, chave string) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	out := m.obs[:0]
	for _, o := range m.obs {
		if o.ChaveAcesso == chave && o.Stage == model.StageImport && o.EventType == model.EventImportIgnored {
			delete(m.seen, DedupKey(o)) // mantém o set de dedup coerente
			n++
			continue
		}
		out = append(out, o)
	}
	m.obs = out
	return n, nil // notas são derivadas na leitura — nada a recomputar
}

func (m *Memory) allNotas() []model.Nota {
	byChave := map[string][]model.Observation{}
	for _, o := range m.obs {
		byChave[o.ChaveAcesso] = append(byChave[o.ChaveAcesso], o)
	}
	out := make([]model.Nota, 0, len(byChave))
	for chave, spans := range byChave {
		out = append(out, derive.Nota(chave, spans))
	}
	return out
}

func (m *Memory) Overview(_ context.Context) (model.Overview, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var ov model.Overview
	today := time.Now().Format("2006-01-02")
	var arr, syn []int64
	for _, n := range m.allNotas() {
		addStatus(&ov.StatusCounts, n.Status)
		if n.ImportedAt != nil && n.ImportedAt.Format("2006-01-02") == today {
			ov.ImportedToday++
		}
		if n.LatArrivalSyncS != nil {
			arr = append(arr, *n.LatArrivalSyncS)
		}
		if n.LatSyncImportS != nil {
			syn = append(syn, *n.LatSyncImportS)
		}
	}
	ov.InTransit = ov.Arrived + ov.Synced
	ov.LatArrivalSyncP50S, ov.LatArrivalSyncP95S = pctl(arr, 0.50), pctl(arr, 0.95)
	ov.LatSyncImportP50S, ov.LatSyncImportP95S = pctl(syn, 0.50), pctl(syn, 0.95)
	return ov, nil
}

func (m *Memory) Timeseries(_ context.Context, f TimeseriesFilter) (model.Timeseries, error) {
	ts := model.Timeseries{
		Range:   fmt.Sprintf("%dd", f.RangeDays),
		Bucket:  f.Bucket,
		TZ:      "America/Sao_Paulo",
		Buckets: []model.TimeseriesBucket{},
	}
	loc, err := time.LoadLocation("America/Sao_Paulo")
	if err != nil {
		loc = time.UTC
	}
	week := f.Bucket == "week"
	key := func(t time.Time) string { return bucketStart(t, week, loc).Format("2006-01-02") }

	m.mu.RLock()
	defer m.mu.RUnlock()
	arrN, synN, impN, ignN := map[string]int{}, map[string]int{}, map[string]int{}, map[string]int{}
	arrLat, synLat := map[string][]int64{}, map[string][]int64{}
	for _, n := range m.allNotas() {
		if n.ArrivedAt != nil {
			k := key(*n.ArrivedAt)
			arrN[k]++
			if n.LatArrivalSyncS != nil {
				arrLat[k] = append(arrLat[k], *n.LatArrivalSyncS)
			}
		}
		if n.SyncedAt != nil {
			k := key(*n.SyncedAt)
			synN[k]++
			if n.LatSyncImportS != nil {
				synLat[k] = append(synLat[k], *n.LatSyncImportS)
			}
		}
		if n.ImportedAt != nil {
			impN[key(*n.ImportedAt)]++
		}
		// import_ignored: status atual ignored, datado pelo observed_at do evento de ignore.
		if n.Status == model.StatusImportIgnored {
			for _, o := range m.spansFor(n.ChaveAcesso) {
				if o.Stage == model.StageImport && o.EventType == model.EventImportIgnored {
					ignN[key(o.ObservedAt)]++ // conta a chave 1x
					break
				}
			}
		}
	}

	// spine contínua: do bucket mais antigo do range até o de hoje (fuso local).
	now := time.Now().In(loc)
	start := bucketStart(now.AddDate(0, 0, -(f.RangeDays - 1)), week, loc)
	end := bucketStart(now, week, loc)
	step := func(t time.Time) time.Time { return t.AddDate(0, 0, 1) }
	if week {
		step = func(t time.Time) time.Time { return t.AddDate(0, 0, 7) }
	}
	for b := start; !b.After(end); b = step(b) {
		k := b.Format("2006-01-02")
		ts.Buckets = append(ts.Buckets, model.TimeseriesBucket{
			Date:               k,
			Arrived:            arrN[k],
			Synced:             synN[k],
			Imported:           impN[k],
			ImportIgnored:      ignN[k],
			LatArrivalSyncP50S: pctl(append([]int64(nil), arrLat[k]...), 0.50),
			LatArrivalSyncP95S: pctl(append([]int64(nil), arrLat[k]...), 0.95),
			LatSyncImportP50S:  pctl(append([]int64(nil), synLat[k]...), 0.50),
			LatSyncImportP95S:  pctl(append([]int64(nil), synLat[k]...), 0.95),
		})
	}
	return ts, nil
}

// bucketStart trunca t para o início do bucket (dia ou semana ISO/segunda) no fuso loc.
// Espelha date_trunc('day'|'week', ...) do Postgres, que usa segunda como início de semana.
func bucketStart(t time.Time, week bool, loc *time.Location) time.Time {
	t = t.In(loc)
	y, mo, d := t.Date()
	day := time.Date(y, mo, d, 0, 0, 0, 0, loc)
	if week {
		wd := int(day.Weekday()) // domingo=0
		if wd == 0 {
			wd = 7
		}
		day = day.AddDate(0, 0, -(wd - 1)) // recua até segunda
	}
	return day
}

func (m *Memory) DocTypes(_ context.Context) ([]model.DocTypeCount, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	counts := map[model.DocType]int{}
	for _, n := range m.allNotas() {
		counts[n.DocType]++
	}
	out := make([]model.DocTypeCount, 0, len(counts))
	for d, c := range counts {
		out = append(out, model.DocTypeCount{DocType: d, Count: c})
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Count > out[j].Count })
	return out, nil
}

func (m *Memory) BacklogAge(_ context.Context) ([]model.BacklogBucket, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	now := time.Now()
	counts := map[string]int{}
	for _, n := range m.allNotas() {
		switch n.Status {
		case model.StatusArrived, model.StatusSynced, model.StatusPendingImport:
		default:
			continue
		}
		ref := n.ArrivedAt
		if ref == nil {
			ref = &n.FirstSeenAt
		}
		counts[model.BacklogBucketOf(now.Sub(*ref))]++
	}
	return orderedBacklog(counts), nil
}

func (m *Memory) Empresas(_ context.Context, f EmpresaFilter) ([]model.EmpresaAgg, int, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	type key struct {
		emp, fil       int
		hasEmp, hasFil bool
	}
	agg := map[key]*model.EmpresaAgg{}
	for _, n := range m.allNotas() {
		// janela de data (date_field from/to): só conta notas cujo campo escolhido cai
		// no período. Sem janela, conta todas (paridade com o caminho do contador).
		if !inDateWindow(n, f.DateField, f.From, f.To) {
			continue
		}
		// Notas sem empresa colapsam numa única linha "Sem empresa" (emp/fil NULL).
		var k key
		var a *model.EmpresaAgg
		if n.CodigoEmpresa == nil {
			k = key{}
			if agg[k] == nil {
				agg[k] = &model.EmpresaAgg{}
			}
			a = agg[k]
		} else {
			k = key{emp: *n.CodigoEmpresa, hasEmp: true}
			if n.CodigoFilial != nil {
				k.fil, k.hasFil = *n.CodigoFilial, true
			}
			a = agg[k]
			if a == nil {
				emp := *n.CodigoEmpresa
				a = &model.EmpresaAgg{CodigoEmpresa: &emp}
				if n.CodigoFilial != nil {
					fl := *n.CodigoFilial
					a.CodigoFilial = &fl
				}
				agg[k] = a
			}
		}
		if a.NomeEmpresa == "" && n.NomeEmpresa != "" {
			a.NomeEmpresa = n.NomeEmpresa
		}
		addStatus(&a.StatusCounts, n.Status)
	}
	out := make([]model.EmpresaAgg, 0, len(agg))
	for _, a := range agg {
		if f.PendentesOnly && pendentes(a.StatusCounts) == 0 {
			continue
		}
		if f.Query != "" && !containsFold(a.NomeEmpresa, f.Query) {
			continue
		}
		a.InTransit = a.Arrived + a.Synced
		out = append(out, *a)
	}
	if f.Sort == "pendentes" {
		sort.SliceStable(out, func(i, j int) bool {
			if pi, pj := pendentes(out[i].StatusCounts), pendentes(out[j].StatusCounts); pi != pj {
				return pi > pj
			}
			return codigoLess(out[i].CodigoEmpresa, out[j].CodigoEmpresa)
		})
	} else {
		sort.SliceStable(out, func(i, j int) bool {
			return codigoLess(out[i].CodigoEmpresa, out[j].CodigoEmpresa)
		})
	}
	total := len(out)
	if f.Offset >= len(out) {
		return []model.EmpresaAgg{}, total, nil
	}
	out = out[f.Offset:]
	if f.Limit > 0 && len(out) > f.Limit {
		out = out[:f.Limit]
	}
	return out, total, nil
}

// ListNfseImport: a impl em memória não tem dados NFSe (vem do Firebird, só no Postgres).
func (m *Memory) ListNfseImport(_ context.Context, _ NfseFilter) ([]model.NfseImport, int, error) {
	return []model.NfseImport{}, 0, nil
}

func (m *Memory) UpsertHeartbeat(_ context.Context, _ string, _ map[string]any) error {
	return nil
}

func (m *Memory) GetStatus(_ context.Context) ([]model.ServiceStatus, error) {
	return nil, nil
}

func addStatus(c *model.StatusCounts, s model.NotaStatus) {
	switch s {
	case model.StatusArrived:
		c.Arrived++
	case model.StatusSynced:
		c.Synced++
	case model.StatusImported:
		c.Imported++
	case model.StatusImportIgnored:
		c.ImportIgnored++
	case model.StatusPendingImport:
		c.PendingImport++
	case model.StatusStuck:
		c.Stuck++
	case model.StatusLost:
		c.Lost++
	}
}

func pendentes(c model.StatusCounts) int {
	return c.Arrived + c.Synced + c.Stuck + c.PendingImport
}

// codigoLess ordena códigos com nil por último (espelha NULLS LAST do Postgres),
// pondo a linha "Sem empresa" no fim.
func codigoLess(a, b *int) bool {
	if a == nil {
		return false
	}
	if b == nil {
		return true
	}
	return *a < *b
}

func pctl(v []int64, p float64) *int64 {
	if len(v) == 0 {
		return nil
	}
	sort.Slice(v, func(i, j int) bool { return v[i] < v[j] })
	idx := int(p * float64(len(v)))
	if idx >= len(v) {
		idx = len(v) - 1
	}
	out := v[idx]
	return &out
}

func (m *Memory) spansFor(chave string) []model.Observation {
	var spans []model.Observation
	for _, o := range m.obs {
		if o.ChaveAcesso == chave {
			spans = append(spans, o)
		}
	}
	return spans
}

func matches(n model.Nota, f NotaFilter) bool {
	if f.Status != "" && n.Status != f.Status {
		return false
	}
	if f.DocType != "" && n.DocType != f.DocType {
		return false
	}
	if f.CodigoEmpresa != nil && (n.CodigoEmpresa == nil || *n.CodigoEmpresa != *f.CodigoEmpresa) {
		return false
	}
	if f.CodigoFilial != nil && (n.CodigoFilial == nil || *n.CodigoFilial != *f.CodigoFilial) {
		return false
	}
	if f.SemEmpresa && n.CodigoEmpresa != nil {
		return false
	}
	if f.EmpresaQuery != "" && !containsFold(n.NomeEmpresa, f.EmpresaQuery) {
		return false
	}
	if f.Cnpj != "" && !strings.Contains(n.CnpjEmitente, f.Cnpj) && !strings.Contains(n.CnpjDestinatario, f.Cnpj) {
		return false
	}
	if f.ChaveQuery != "" && !strings.Contains(n.ChaveAcesso, f.ChaveQuery) {
		return false
	}
	if !inDateWindow(n, f.DateField, f.From, f.To) {
		return false
	}
	return true
}

// inDateWindow reports whether n cai na janela [from,to] do date_field (calendar-day,
// yyyy-mm-dd). Sem date_field/janela -> sempre true. Espelha o filtro de data do
// GET /notas; compartilhado por ListNotas e Empresas.
func inDateWindow(n model.Nota, field, from, to string) bool {
	if col := dateColumn(field); col == "" || (from == "" && to == "") {
		return true
	}
	d := notaDate(n, field)
	if d == "" {
		return false
	}
	if from != "" && d < from {
		return false
	}
	if to != "" && d > to {
		return false
	}
	return true
}

func containsFold(s, sub string) bool {
	return strings.Contains(strings.ToLower(s), strings.ToLower(sub))
}

// notaDate returns the yyyy-mm-dd string of the chosen date field, or "".
func notaDate(n model.Nota, field string) string {
	switch field {
	case "emissao":
		return n.DataEmissao
	case "arrived":
		return tsDate(n.ArrivedAt)
	case "synced":
		return tsDate(n.SyncedAt)
	case "imported":
		return tsDate(n.ImportedAt)
	}
	return ""
}

func tsDate(t *time.Time) string {
	if t == nil {
		return ""
	}
	return t.Format("2006-01-02")
}

func clamp(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}
