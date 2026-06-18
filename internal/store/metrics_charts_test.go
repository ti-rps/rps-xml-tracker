package store

import (
	"context"
	"testing"
	"time"

	"github.com/EnzzoHosaki/rps-xml-tracker/internal/model"
)

func TestDocTypesAndBacklogAge(t *testing.T) {
	m := NewMemory()
	ctx := context.Background()
	twoHoursAgo := time.Now().Add(-2 * time.Hour)
	// 2 notas só com chegada (status arrived = pendente), 2h atrás: 1 NFCe, 1 NFe.
	if _, _, err := m.AppendObservations(ctx, []model.Observation{
		{ChaveAcesso: "A", Stage: model.StageArrival, EventType: model.EventFileSeen, DocType: model.DocNFCe, ObservedAt: twoHoursAgo, Source: "t", FileHash: "a"},
		{ChaveAcesso: "B", Stage: model.StageArrival, EventType: model.EventFileSeen, DocType: model.DocNFe, ObservedAt: twoHoursAgo, Source: "t", FileHash: "b"},
	}); err != nil {
		t.Fatal(err)
	}

	dt, err := m.DocTypes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	got := map[model.DocType]int{}
	for _, d := range dt {
		got[d.DocType] = d.Count
	}
	if got[model.DocNFCe] != 1 || got[model.DocNFe] != 1 {
		t.Fatalf("doctypes: esperava NFCe=1 NFe=1, veio %+v", got)
	}

	ba, err := m.BacklogAge(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(ba) != len(model.BacklogBuckets) {
		t.Fatalf("backlog: esperava %d faixas, veio %d", len(model.BacklogBuckets), len(ba))
	}
	for _, b := range ba {
		want := 0
		if b.Label == "1-6h" {
			want = 2 // ambas chegaram há 2h e estão pendentes
		}
		if b.Count != want {
			t.Errorf("backlog faixa %s: esperava %d, veio %d", b.Label, want, b.Count)
		}
	}
}
