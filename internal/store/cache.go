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
	sem chan struct{} // serializa os recálculos pesados (1 por vez) p/ não afogar o IO
}

type cacheItem struct {
	mu         sync.Mutex // single-flight: serializa o recompute desta chave
	val        any
	expires    time.Time
	has        bool
	refreshing bool // já existe um refresh em background em andamento
}

// NewCached embrulha s com um cache de TTL para os agregados do dashboard.
func NewCached(s Store, ttl time.Duration) *Cached {
	return &Cached{Store: s, ttl: ttl, m: map[string]*cacheItem{}, sem: make(chan struct{}, 1)}
}

// Warm pré-computa os agregados do dashboard em background ao subir a API, para que
// o primeiro acesso já encontre tudo em cache (sem o cold-start lento). Roda os
// recálculos serializados pelo sem, então não afogam o banco. Erros são ignorados
// (o acesso normal recomputa). Chaves cobrem o que o Painel do maestro consome.
func (c *Cached) Warm(ctx context.Context) {
	_, _ = c.Overview(ctx)
	for _, r := range []int{7, 30, 90} {
		_, _ = c.Timeseries(ctx, TimeseriesFilter{RangeDays: r, Bucket: "day"})
	}
	_, _, _ = c.Empresas(ctx, EmpresaFilter{}) // tab Empresas (todas)
	_, _, _ = c.Empresas(ctx, EmpresaFilter{PendentesOnly: true, Sort: "pendentes"})
	_, _ = c.BacklogAge(ctx)
	// DocTypes não é aquecido nem cacheado: lê do contador (notas_counts), instantâneo.
}

func (c *Cached) BacklogAge(ctx context.Context) ([]model.BacklogBucket, error) {
	v, err := c.get("backlog_age", func(cctx context.Context) (any, error) {
		return c.Store.BacklogAge(cctx)
	})
	if err != nil {
		return nil, err
	}
	return v.([]model.BacklogBucket), nil
}

// get serve o dashboard sem nunca bloquear (exceto o primeiríssimo cálculo por
// chave, por boot). Estratégia stale-while-revalidate:
//   - cache fresco -> devolve na hora.
//   - cache velho mas presente -> devolve o velho NA HORA e dispara um refresh em
//     BACKGROUND (uma só goroutine por chave). A query lenta sai do caminho do request.
//   - cache vazio (cold start) -> calcula bloqueando, sob it.mu (single-flight: chamadas
//     concorrentes esperam e pegam o resultado, não abrem N queries).
// Erros não são cacheados. O compute roda em contexto destacado (com timeout) para não
// ser cancelado se o cliente que disparou desconectar — o resultado é compartilhado.
func (c *Cached) get(key string, compute func(context.Context) (any, error)) (any, error) {
	c.mu.Lock()
	it := c.m[key]
	if it == nil {
		it = &cacheItem{}
		c.m[key] = it
	}
	c.mu.Unlock()

	it.mu.Lock()
	if it.has {
		val := it.val
		if !time.Now().Before(it.expires) && !it.refreshing { // velho e ninguém atualizando
			it.refreshing = true
			go c.refreshAsync(it, compute)
		}
		it.mu.Unlock()
		return val, nil // devolve fresco OU velho, sempre instantâneo
	}
	// cold start: calcula bloqueando, segurando it.mu (single-flight).
	defer it.mu.Unlock()
	val, err := c.computeDetached(compute)
	if err != nil {
		return val, err // não cacheia erro
	}
	it.val, it.expires, it.has = val, time.Now().Add(c.ttl), true
	return val, nil
}

// refreshAsync recalcula em background e atualiza o cache; roda enquanto o request
// já devolveu o valor velho.
func (c *Cached) refreshAsync(it *cacheItem, compute func(context.Context) (any, error)) {
	val, err := c.computeDetached(compute)
	it.mu.Lock()
	if err == nil {
		it.val, it.expires, it.has = val, time.Now().Add(c.ttl), true
	}
	it.refreshing = false
	it.mu.Unlock()
}

// computeDetached roda o compute serializado pelo sem (1 query pesada por vez, para
// não afogar o IO do banco com a manada de cold-starts do dashboard) e num contexto
// destacado com timeout generoso (queries de agregação sobre 14M chegam a ~1min).
func (c *Cached) computeDetached(compute func(context.Context) (any, error)) (any, error) {
	c.sem <- struct{}{}        // adquire o slot (espera se outro recálculo está rodando)
	defer func() { <-c.sem }() // libera
	cctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	return compute(cctx)
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
	// Busca por nome é rápida (trigram corta o conjunto) e tem cardinalidade alta de
	// chaves — passa direto, sem cachear (evita inflar o mapa do cache).
	if f.Query != "" {
		return c.Store.Empresas(ctx, f)
	}
	key := fmt.Sprintf("empresas|%t|%s|%s|%d|%d", f.PendentesOnly, f.Query, f.Sort, f.Limit, f.Offset)
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
