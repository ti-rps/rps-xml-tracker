package store

import (
	"context"
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
		if s == model.StatusArrived || s == model.StatusSynced {
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

func (m *Memory) Empresas(_ context.Context, pendentesOnly bool) ([]model.EmpresaAgg, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	type key struct{ emp, fil int }
	agg := map[key]*model.EmpresaAgg{}
	for _, n := range m.allNotas() {
		if n.CodigoEmpresa == nil {
			continue
		}
		fil := 0
		if n.CodigoFilial != nil {
			fil = *n.CodigoFilial
		}
		k := key{*n.CodigoEmpresa, fil}
		a := agg[k]
		if a == nil {
			emp, f := *n.CodigoEmpresa, fil
			a = &model.EmpresaAgg{CodigoEmpresa: &emp, CodigoFilial: &f}
			agg[k] = a
		}
		addStatus(&a.StatusCounts, n.Status)
	}
	out := make([]model.EmpresaAgg, 0, len(agg))
	for _, a := range agg {
		if pendentesOnly && pendentes(a.StatusCounts) == 0 {
			continue
		}
		out = append(out, *a)
	}
	sort.SliceStable(out, func(i, j int) bool { return *out[i].CodigoEmpresa < *out[j].CodigoEmpresa })
	return out, nil
}

// ListNfseImport: a impl em memória não tem dados NFSe (vem do Firebird, só no Postgres).
func (m *Memory) ListNfseImport(_ context.Context, _ NfseFilter) ([]model.NfseImport, int, error) {
	return []model.NfseImport{}, 0, nil
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
	if f.EmpresaQuery != "" && !containsFold(n.NomeEmpresa, f.EmpresaQuery) {
		return false
	}
	if f.Cnpj != "" && !strings.Contains(n.CnpjEmitente, f.Cnpj) && !strings.Contains(n.CnpjDestinatario, f.Cnpj) {
		return false
	}
	if f.ChaveQuery != "" && !strings.Contains(n.ChaveAcesso, f.ChaveQuery) {
		return false
	}
	if col := dateColumn(f.DateField); col != "" && (f.From != "" || f.To != "") {
		d := notaDate(n, f.DateField)
		if d == "" {
			return false
		}
		if f.From != "" && d < f.From {
			return false
		}
		if f.To != "" && d > f.To {
			return false
		}
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
