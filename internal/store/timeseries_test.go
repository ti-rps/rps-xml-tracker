package store

import (
	"context"
	"testing"
	"time"

	"github.com/EnzzoHosaki/rps-xml-tracker/internal/model"
)

// TestTimeseriesMemory exercita o shape e a bucketização do Memory store: série
// contínua (zero-fill), contagem de fluxo por evento e import_ignored datado pelo
// evento de ignore.
func TestTimeseriesMemory(t *testing.T) {
	m := NewMemory()
	ctx := context.Background()
	loc, _ := time.LoadLocation("America/Sao_Paulo")
	if loc == nil {
		loc = time.UTC
	}
	now := time.Now().In(loc)
	twoDaysAgo := now.AddDate(0, 0, -2)

	// Nota A: chegou e sincronizou 2 dias atrás (em trânsito).
	if _, _, err := m.AppendObservations(ctx, []model.Observation{
		{ChaveAcesso: "A", Stage: model.StageArrival, EventType: model.EventFileSeen, ObservedAt: twoDaysAgo, Source: "agent", FileHash: "a1"},
		{ChaveAcesso: "A", Stage: model.StageSync, EventType: model.EventFileMoved, ObservedAt: twoDaysAgo, Source: "agent", FileHash: "a2"},
	}); err != nil {
		t.Fatal(err)
	}
	// Nota B: ignorada 2 dias atrás -> status import_ignored, datada pelo evento.
	if _, _, err := m.AppendObservations(ctx, []model.Observation{
		{ChaveAcesso: "B", Stage: model.StageArrival, EventType: model.EventFileSeen, ObservedAt: twoDaysAgo, Source: "agent", FileHash: "b1"},
		{ChaveAcesso: "B", Stage: model.StageImport, EventType: model.EventImportIgnored, ObservedAt: twoDaysAgo, Source: "poller", FileHash: "b2"},
	}); err != nil {
		t.Fatal(err)
	}

	ts, err := m.Timeseries(ctx, TimeseriesFilter{RangeDays: 7, Bucket: "day"})
	if err != nil {
		t.Fatal(err)
	}
	if ts.Range != "7d" || ts.Bucket != "day" || ts.TZ != "America/Sao_Paulo" {
		t.Fatalf("meta inesperada: %+v", ts)
	}
	if len(ts.Buckets) != 7 {
		t.Fatalf("série não-contínua: esperava 7 buckets, veio %d", len(ts.Buckets))
	}
	// ordenado do mais antigo pro mais recente
	for i := 1; i < len(ts.Buckets); i++ {
		if ts.Buckets[i-1].Date >= ts.Buckets[i].Date {
			t.Fatalf("buckets fora de ordem: %s antes de %s", ts.Buckets[i-1].Date, ts.Buckets[i].Date)
		}
	}
	wantDay := bucketStart(twoDaysAgo, false, loc).Format("2006-01-02")
	var found bool
	for _, b := range ts.Buckets {
		if b.Date == wantDay {
			found = true
			if b.Arrived != 2 {
				t.Errorf("arrived: esperava 2 (A+B chegaram), veio %d", b.Arrived)
			}
			if b.Synced != 1 {
				t.Errorf("synced: esperava 1 (só A), veio %d", b.Synced)
			}
			if b.ImportIgnored != 1 {
				t.Errorf("import_ignored: esperava 1 (B), veio %d", b.ImportIgnored)
			}
			if b.Imported != 0 {
				t.Errorf("imported: esperava 0, veio %d", b.Imported)
			}
		}
	}
	if !found {
		t.Fatalf("bucket do dia %s não encontrado na série", wantDay)
	}
}

func TestTimeseriesBucketStartWeek(t *testing.T) {
	loc := time.UTC
	// Quarta-feira 2026-06-17 -> semana ISO começa na segunda 2026-06-15.
	wed := time.Date(2026, 6, 17, 15, 0, 0, 0, loc)
	if got := bucketStart(wed, true, loc).Format("2006-01-02"); got != "2026-06-15" {
		t.Errorf("início da semana: esperava 2026-06-15 (segunda), veio %s", got)
	}
	// Domingo 2026-06-21 ainda pertence à semana que começou na segunda 2026-06-15.
	sun := time.Date(2026, 6, 21, 1, 0, 0, 0, loc)
	if got := bucketStart(sun, true, loc).Format("2006-01-02"); got != "2026-06-15" {
		t.Errorf("domingo->segunda: esperava 2026-06-15, veio %s", got)
	}
}
