package store

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/EnzzoHosaki/rps-xml-tracker/internal/model"
)

// Cached envolve um Store e memoiza os 3 endpoints de leitura do dashboard
// (Overview, Empresas, Timeseries) com TTL curto + single-flight. Esses queries
// agregam sobre milhões de linhas (~segundos) e o dashboard dispara os três a cada
// carga; sem cache, refreshs repetidos EMPILHAM queries idênticas que competem por
// IO e se arrastam por minutos. Com cache: a query roda no máximo 1x por TTL por
// chave, e refreshs concorrentes esperam UMA computação (single-flight) em vez de
// abrir N. Os demais métodos do Store passam direto (embed da interface).
type Cached struct {
	Store
	ttl time.Duration
	mu  sync.Mutex // guarda o mapa
	m   map[string]*cacheItem
}

type cacheItem struct {
	mu      sync.Mutex // single-flight: serializa o recompute desta chave
	val     any
	expires time.Time
	has     bool
}

// NewCached embrulha s com um cache de TTL para os agregados do dashboard.
func NewCached(s Store, ttl time.Duration) *Cached {
	return &Cached{Store: s, ttl: ttl, m: map[string]*cacheItem{}}
}

// get devolve o valor em cache se fresco; senão recomputa (apenas UMA goroutine por
// chave de cada vez — as demais esperam no it.mu e pegam o resultado fresco). Erros
// não são cacheados (próxima chamada tenta de novo). O compute roda num contexto
// destacado (com timeout) para não ser cancelado se o cliente que disparou
// desconectar — o resultado é compartilhado.
func (c *Cached) get(key string, compute func(context.Context) (any, error)) (any, error) {
	c.mu.Lock()
	it := c.m[key]
	if it == nil {
		it = &cacheItem{}
		c.m[key] = it
	}
	c.mu.Unlock()

	it.mu.Lock()
	defer it.mu.Unlock()
	if it.has && time.Now().Before(it.expires) {
		return it.val, nil
	}
	cctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	val, err := compute(cctx)
	if err != nil {
		return val, err // não cacheia erro
	}
	it.val, it.expires, it.has = val, time.Now().Add(c.ttl), true
	return val, nil
}

func (c *Cached) Overview(ctx context.Context) (model.Overview, error) {
	v, err := c.get("overview", func(cctx context.Context) (any, error) {
		return c.Store.Overview(cctx)
	})
	if err != nil {
		return model.Overview{}, err
	}
	return v.(model.Overview), nil
}

type empresasResult struct {
	items []model.EmpresaAgg
	total int
}

func (c *Cached) Empresas(ctx context.Context, f EmpresaFilter) ([]model.EmpresaAgg, int, error) {
	key := fmt.Sprintf("empresas|%t|%s|%d|%d", f.PendentesOnly, f.Sort, f.Limit, f.Offset)
	v, err := c.get(key, func(cctx context.Context) (any, error) {
		items, total, e := c.Store.Empresas(cctx, f)
		if e != nil {
			return nil, e
		}
		return empresasResult{items, total}, nil
	})
	if err != nil {
		return nil, 0, err
	}
	r := v.(empresasResult)
	return r.items, r.total, nil
}

func (c *Cached) Timeseries(ctx context.Context, f TimeseriesFilter) (model.Timeseries, error) {
	key := fmt.Sprintf("ts|%d|%s", f.RangeDays, f.Bucket)
	v, err := c.get(key, func(cctx context.Context) (any, error) {
		return c.Store.Timeseries(cctx, f)
	})
	if err != nil {
		return model.Timeseries{}, err
	}
	return v.(model.Timeseries), nil
}
