package store

import (
	"context"
	"testing"
	"time"

	"github.com/EnzzoHosaki/rps-xml-tracker/internal/model"
)

func ptr(i int) *int { return &i }

func obsFor(chave string, stage model.Stage, event string, at time.Time, emp int) model.Observation {
	e := emp
	return model.Observation{
		ChaveAcesso: chave, Stage: stage, EventType: event, ObservedAt: at,
		DocType: model.DocNFe, Source: "t", CodigoEmpresa: &e, CodigoFilial: ptr(1),
	}
}

func TestOverviewAndEmpresas(t *testing.T) {
	ctx := context.Background()
	m := NewMemory()
	t0 := time.Date(2026, 6, 8, 9, 0, 0, 0, time.UTC)
	now := time.Now()

	// A (emp 1203): arrival+sync+import -> imported (today)
	// B (emp 1203): arrival+sync -> synced
	// C (emp 1100): arrival -> arrived
	_, _, _ = m.AppendObservations(ctx, []model.Observation{
		obsFor("A", model.StageArrival, model.EventFileSeen, t0, 1203),
		obsFor("A", model.StageSync, model.EventFileMoved, t0.Add(30*time.Minute), 1203),
		obsFor("A", model.StageImport, model.EventImported, now, 1203),
		obsFor("B", model.StageArrival, model.EventFileSeen, t0, 1203),
		obsFor("B", model.StageSync, model.EventFileMoved, t0.Add(10*time.Minute), 1203),
		obsFor("C", model.StageArrival, model.EventFileSeen, t0, 1100),
	})

	ov, err := m.Overview(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if ov.Imported != 1 || ov.Synced != 1 || ov.Arrived != 1 {
		t.Fatalf("counts: imported=%d synced=%d arrived=%d", ov.Imported, ov.Synced, ov.Arrived)
	}
	if ov.InTransit != 2 {
		t.Errorf("in_transit=%d want 2", ov.InTransit)
	}
	if ov.ImportedToday != 1 {
		t.Errorf("imported_today=%d want 1", ov.ImportedToday)
	}
	if ov.LatArrivalSyncP50S == nil || *ov.LatArrivalSyncP50S != 1800 {
		t.Errorf("lat arrival->sync p50 = %v want 1800", ov.LatArrivalSyncP50S)
	}

	emps, err := m.Empresas(ctx, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(emps) != 2 {
		t.Fatalf("empresas pendentes = %d want 2", len(emps))
	}
	// sorted by codigo_empresa: 1100 then 1203
	if *emps[0].CodigoEmpresa != 1100 || emps[0].Arrived != 1 {
		t.Errorf("emp[0]=%+v", emps[0])
	}
	if *emps[1].CodigoEmpresa != 1203 || emps[1].Synced != 1 || emps[1].Imported != 1 {
		t.Errorf("emp[1]=%+v", emps[1])
	}
}
