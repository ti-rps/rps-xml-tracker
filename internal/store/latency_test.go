package store

import (
	"context"
	"testing"
	"time"

	"github.com/EnzzoHosaki/rps-xml-tracker/internal/model"
)

// TestLatency cobre o GET /metrics/latency no store em memória (espelho do Postgres):
// chegada→sync em percentis (timestamps reais do agente) e sync→import em DIAS —
// nunca em segundos, porque o imported_at é date-only (meia-noite BRT): uma nota
// importada no MESMO dia tem imported_at ANTERIOR ao synced_at e daria negativo.
func TestLatency(t *testing.T) {
	ctx := context.Background()
	st := NewMemory()
	loc, err := time.LoadLocation("America/Sao_Paulo")
	if err != nil {
		loc = time.FixedZone("-03", -3*3600)
	}

	// dia-base: anteontem em BRT (dentro da janela de 7 dias).
	n := time.Now().In(loc).AddDate(0, 0, -2)
	midnight := time.Date(n.Year(), n.Month(), n.Day(), 0, 0, 0, 0, loc)
	sync := midnight.Add(10 * time.Hour) // 10:00 BRT

	seed := func(chave string, latS time.Duration, importedAt time.Time) {
		t.Helper()
		obs := []model.Observation{
			{ChaveAcesso: chave, Stage: model.StageArrival, EventType: model.EventFileSeen,
				ObservedAt: sync.Add(-latS), DocType: model.DocNFe, Source: "agent:test"},
			{ChaveAcesso: chave, Stage: model.StageSync, EventType: model.EventFileMoved,
				ObservedAt: sync, Source: "agent:test"},
			{ChaveAcesso: chave, Stage: model.StageImport, EventType: model.EventImported,
				ObservedAt: importedAt, Source: "poller:firebird"},
		}
		if _, _, err := st.AppendObservations(ctx, obs); err != nil {
			t.Fatal(err)
		}
	}
	// A: lat 1h, importada no MESMO dia (imported_at = meia-noite do dia do sync,
	//    ou seja, ANTES do synced_at — o caso que tornaria percentil em segundos lixo).
	// B: lat 2h, importada em D+1.
	// C: lat 3h, importada em D+2.
	seed("A", 1*time.Hour, midnight)
	seed("B", 2*time.Hour, midnight.AddDate(0, 0, 1))
	seed("C", 3*time.Hour, midnight.AddDate(0, 0, 2))

	lat, err := st.Latency(ctx, 7)
	if err != nil {
		t.Fatal(err)
	}

	as := lat.ArrivalToSync
	if as.Count != 3 || len(as.Daily) != 1 || as.Daily[0].Count != 3 {
		t.Fatalf("arrival_to_sync=%+v, want count=3 num único dia", as)
	}
	if as.P50S == nil || *as.P50S != 7200 {
		t.Errorf("p50=%v, want 7200 (mediana de 1h/2h/3h)", as.P50S)
	}
	if as.P95S == nil || *as.P95S <= 7200 || *as.P95S > 10800 {
		t.Errorf("p95=%v, want entre 7200 e 10800", as.P95S)
	}

	si := lat.SyncToImport
	if si.Count != 3 || si.SameDay != 1 || si.D1 != 1 || si.D2Plus != 1 {
		t.Fatalf("sync_to_import=%+v, want 1/1/1", si)
	}
	if si.SameDayPct != 33.33 {
		t.Errorf("same_day_pct=%v, want 33.33", si.SameDayPct)
	}
}
