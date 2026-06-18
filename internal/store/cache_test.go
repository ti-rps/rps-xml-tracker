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
