package store

import (
	"context"
	"testing"
	"time"

	"github.com/EnzzoHosaki/rps-xml-tracker/internal/model"
)

func TestSummaryNotas(t *testing.T) {
	ctx := context.Background()
	m := NewMemory()
	at := time.Date(2026, 6, 10, 9, 0, 0, 0, time.UTC)
	mk := func(chave string, dt model.DocType, valor float64) model.Observation {
		v := valor
		return model.Observation{
			ChaveAcesso: chave, Stage: model.StageArrival, EventType: model.EventFileSeen,
			ObservedAt: at, DocType: dt, Source: "t", ValorTotal: &v,
		}
	}
	_, _, _ = m.AppendObservations(ctx, []model.Observation{
		mk("A", model.DocNFCe, 100.50),
		mk("B", model.DocNFCe, 200.00),
		mk("C", model.DocNFe, 999.99), // outro tipo, não deve entrar no filtro NFCE
	})

	// filtro NFC-e -> 2 notas, soma 300.50
	s, err := m.SummaryNotas(ctx, NotaFilter{DocType: model.DocNFCe})
	if err != nil {
		t.Fatal(err)
	}
	if s.Count != 2 {
		t.Errorf("count=%d want 2", s.Count)
	}
	if s.ValorTotal < 300.49 || s.ValorTotal > 300.51 {
		t.Errorf("valor_total=%.2f want 300.50", s.ValorTotal)
	}

	// sem filtro -> 3 notas, soma 1300.49
	s, _ = m.SummaryNotas(ctx, NotaFilter{})
	if s.Count != 3 || s.ValorTotal < 1300.48 || s.ValorTotal > 1300.50 {
		t.Errorf("sem filtro: count=%d valor=%.2f want 3 e 1300.49", s.Count, s.ValorTotal)
	}
}
