package store

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/EnzzoHosaki/rps-xml-tracker/internal/model"
)

// countingStore conta quantas vezes Overview chega ao store de baixo.
type countingStore struct {
	Store
	calls int32
}

func (c *countingStore) Overview(context.Context) (model.Overview, error) {
	atomic.AddInt32(&c.calls, 1)
	time.Sleep(10 * time.Millisecond) // simula query lenta
	return model.Overview{StatusCounts: model.StatusCounts{Arrived: 7}}, nil
}

func TestCached_SingleFlightAndTTL(t *testing.T) {
	base := &countingStore{Store: NewMemory()}
	c := NewCached(base, 200*time.Millisecond)

	// 20 chamadas concorrentes devem colapsar numa só (single-flight).
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ov, err := c.Overview(context.Background())
			if err != nil || ov.Arrived != 7 {
				t.Errorf("overview inesperado: %+v err=%v", ov, err)
			}
		}()
	}
	wg.Wait()
	if n := atomic.LoadInt32(&base.calls); n != 1 {
		t.Fatalf("single-flight: esperava 1 chamada ao store, veio %d", n)
	}

	// Dentro do TTL: serve do cache, sem nova chamada.
	if _, err := c.Overview(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := atomic.LoadInt32(&base.calls); n != 1 {
		t.Fatalf("dentro do TTL: esperava 1, veio %d", n)
	}

	// Após o TTL: o get devolve o valor velho NA HORA (não bloqueia) e dispara um
	// refresh em background, que recomputa de forma assíncrona.
	time.Sleep(250 * time.Millisecond)
	if _, err := c.Overview(context.Background()); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for atomic.LoadInt32(&base.calls) < 2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if n := atomic.LoadInt32(&base.calls); n != 2 {
		t.Fatalf("após o TTL (refresh async): esperava 2, veio %d", n)
	}
}

// empresaStore devolve um total atrelado ao DateField, para detectar colisão de
// chave de cache entre filtros de data diferentes.
type empresaStore struct {
	Store
	calls int32
}

func (e *empresaStore) Empresas(_ context.Context, f EmpresaFilter) ([]model.EmpresaAgg, int, error) {
	atomic.AddInt32(&e.calls, 1)
	return nil, map[string]int{"emissao": 288, "imported": 488}[f.DateField], nil
}

// TestCached_EmpresasKeyIncludesDateField protege contra a regressão em que a chave
// de cache do /empresas ignorava DateField/From/To: dois filtros de data com os mesmos
// limit/offset/sort colidiam e o segundo recebia o resultado cacheado do primeiro
// (em produção: "emissão" servia o número de "importação").
func TestCached_EmpresasKeyIncludesDateField(t *testing.T) {
	base := &empresaStore{Store: NewMemory()}
	c := NewCached(base, time.Minute)
	ctx := context.Background()
	win := func(field string) EmpresaFilter {
		return EmpresaFilter{DateField: field, From: "2026-06-01", To: "2026-06-29"}
	}

	// imported popula o cache primeiro (como aconteceu no bug em produção).
	if _, total, _ := c.Empresas(ctx, win("imported")); total != 488 {
		t.Fatalf("imported: esperava 488, veio %d", total)
	}
	// emissao com os MESMOS from/to NÃO pode herdar o resultado de imported.
	if _, total, _ := c.Empresas(ctx, win("emissao")); total != 288 {
		t.Fatalf("emissao colidiu com a chave de imported: esperava 288, veio %d", total)
	}
	if n := atomic.LoadInt32(&base.calls); n != 2 {
		t.Fatalf("esperava 2 computes (1 por date_field), veio %d", n)
	}
}

// TestCached_Warm garante que o aquecimento popula o cache (acesso seguinte não
// recomputa).
func TestCached_Warm(t *testing.T) {
	base := &countingStore{Store: NewMemory()}
	c := NewCached(base, time.Minute)
	c.Warm(context.Background())
	before := atomic.LoadInt32(&base.calls)
	if _, err := c.Overview(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := atomic.LoadInt32(&base.calls); got != before {
		t.Fatalf("Warm não deixou o overview em cache: chamadas foram %d->%d", before, got)
	}
}
