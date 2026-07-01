package store

import (
	"context"
	"testing"
	"time"

	"github.com/EnzzoHosaki/rps-xml-tracker/internal/model"
)

func TestOverview_LatenciaCensurada(t *testing.T) {
	ctx := context.Background()
	m := NewMemory()
	// nota sincronizada há ~10 dias e AINDA não importada (status synced). Antes ela
	// era ignorada na latência sync->import (lat NULL); agora deve entrar como ~10 dias.
	arr := time.Now().Add(-10*24*time.Hour - time.Hour)
	syn := time.Now().Add(-10 * 24 * time.Hour)
	_, _, _ = m.AppendObservations(ctx, []model.Observation{
		{ChaveAcesso: "STUCK", Stage: model.StageArrival, EventType: model.EventFileSeen, ObservedAt: arr, DocType: model.DocNFe, Source: "t"},
		{ChaveAcesso: "STUCK", Stage: model.StageSync, EventType: model.EventFileMoved, ObservedAt: syn, DocType: model.DocNFe, Source: "t"},
	})

	ov, err := m.Overview(ctx, OverviewFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if ov.LatSyncImportP50S == nil {
		t.Fatal("nota travada (synced, não importada) deveria contar na latência sync->import censurada, veio nil")
	}
	if *ov.LatSyncImportP50S < 9*24*3600 {
		t.Errorf("latência sync->import censurada = %ds, esperava ~10 dias (>=9d)", *ov.LatSyncImportP50S)
	}
}

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
