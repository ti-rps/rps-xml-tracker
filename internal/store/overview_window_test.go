package store

import (
	"context"
	"testing"
	"time"

	"github.com/EnzzoHosaki/rps-xml-tracker/internal/model"
)

func TestOverview_Window(t *testing.T) {
	ctx := context.Background()
	m := NewMemory()
	mk := func(chave string, at time.Time) model.Observation {
		return model.Observation{
			ChaveAcesso: chave, Stage: model.StageArrival, EventType: model.EventFileSeen,
			ObservedAt: at, DocType: model.DocNFe, Source: "t",
		}
	}
	jun := time.Date(2026, 6, 10, 9, 0, 0, 0, time.UTC)
	mai := time.Date(2026, 5, 10, 9, 0, 0, 0, time.UTC)
	_, _, _ = m.AppendObservations(ctx, []model.Observation{mk("A", jun), mk("B", mai)})

	// Sem filtro: estoque global (2 arrived), mode vazio.
	ov, err := m.Overview(ctx, OverviewFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if ov.Arrived != 2 || ov.Mode != "" {
		t.Fatalf("global: arrived=%d mode=%q, want 2 e vazio", ov.Arrived, ov.Mode)
	}

	// Janela de junho por arrived: só A; mode=flow.
	ov, err = m.Overview(ctx, OverviewFilter{DateField: "arrived", From: "2026-06-01", To: "2026-06-30"})
	if err != nil {
		t.Fatal(err)
	}
	if ov.Arrived != 1 {
		t.Errorf("janela junho: arrived=%d, want 1 (só A)", ov.Arrived)
	}
	if ov.Mode != "flow" {
		t.Errorf("janela: mode=%q, want flow", ov.Mode)
	}
	if ov.InTransit != ov.Arrived+ov.Synced {
		t.Errorf("in_transit inconsistente: %d", ov.InTransit)
	}
}
