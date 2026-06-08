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
	mu     sync.RWMutex
	obs    []model.Observation
	seen   map[string]struct{} // dedup keys
	nextID int64
}

func NewMemory() *Memory {
	return &Memory{seen: map[string]struct{}{}}
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
	if f.ChaveQuery != "" && !strings.Contains(n.ChaveAcesso, f.ChaveQuery) {
		return false
	}
	return true
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
