package poller

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/EnzzoHosaki/rps-xml-tracker/internal/firebird"
	"github.com/EnzzoHosaki/rps-xml-tracker/internal/model"
	"github.com/EnzzoHosaki/rps-xml-tracker/internal/store"
)

// fakeReader returns canned Firebird states (no DB) for the offline unit test.
type fakeReader struct{ states map[string]firebird.ImportState }

func (f fakeReader) Lookup(_ context.Context, chaves []string) (map[string]firebird.ImportState, error) {
	out := map[string]firebird.ImportState{}
	for _, c := range chaves {
		if s, ok := f.states[c]; ok {
			out[c] = s
		}
	}
	return out, nil
}

func seedArrival(t *testing.T, st store.Store, chave string) {
	t.Helper()
	_, _, err := st.AppendObservations(context.Background(), []model.Observation{{
		ChaveAcesso: chave, Stage: model.StageArrival, EventType: model.EventFileSeen,
		ObservedAt: time.Date(2026, 6, 8, 9, 0, 0, 0, time.UTC), DocType: model.DocNFe, Source: "agent:test",
	}})
	if err != nil {
		t.Fatal(err)
	}
}

func TestPollOnce_MapsStatesAndIsIdempotent(t *testing.T) {
	ctx := context.Background()
	st := store.NewMemory()
	seedArrival(t, st, "IMPORTED")
	seedArrival(t, st, "IGNORED")
	seedArrival(t, st, "STILL_PENDING")

	fr := fakeReader{states: map[string]firebird.ImportState{
		"IMPORTED": {Found: true, Importado: true},
		"IGNORED":  {Found: true, ImportIgnorada: true, Motivo: "Empresa usa tela de Pre-Importacao"},
		// STILL_PENDING absent from Firebird -> remains in flight
	}}
	p := New(st, fr)

	res, err := p.PollOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if res.Checked != 3 || res.Imported != 1 || res.Ignored != 1 {
		t.Fatalf("res = %+v, want checked=3 imported=1 ignored=1", res)
	}

	// the imported nota is now terminal
	d, _, _ := st.GetNota(ctx, "IMPORTED")
	if d.Status != model.StatusImported || d.ImportedAt == nil {
		t.Errorf("IMPORTED status=%s importedAt=%v", d.Status, d.ImportedAt)
	}
	d, _, _ = st.GetNota(ctx, "IGNORED")
	if d.Status != model.StatusImportIgnored || d.MotivoIgnorado == "" {
		t.Errorf("IGNORED status=%s motivo=%q", d.Status, d.MotivoIgnorado)
	}

	// second cycle: terminal notas dropped out -> only STILL_PENDING remains in flight
	res2, err := p.PollOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if res2.Checked != 1 || res2.Imported != 0 || res2.Ignored != 0 {
		t.Fatalf("res2 = %+v, want checked=1 imported=0 ignored=0 (idempotent)", res2)
	}
}

func TestPollOnce_FoundButPendingEmitsSeenPending(t *testing.T) {
	ctx := context.Background()
	st := store.NewMemory()
	seedArrival(t, st, "PENDING")

	// achada no Athenas mas IMPORTADO=0 e não ignorada -> aguardando importação.
	fr := fakeReader{states: map[string]firebird.ImportState{
		"PENDING": {Found: true},
	}}
	p := New(st, fr)

	res, err := p.PollOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if res.Pending != 1 || res.Imported != 0 || res.Ignored != 0 {
		t.Fatalf("res = %+v, want pending=1 imported=0 ignored=0", res)
	}
	d, _, _ := st.GetNota(ctx, "PENDING")
	if d.Status != model.StatusPendingImport || d.PendingAt == nil {
		t.Errorf("PENDING status=%s pendingAt=%v, want pending_import", d.Status, d.PendingAt)
	}

	// a nota pendente CONTINUA in-flight (não-terminal) e a reemissão é idempotente.
	res2, err := p.PollOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if res2.Checked != 1 {
		t.Fatalf("res2 = %+v, want checked=1 (pending segue sendo pollada)", res2)
	}
}

// TestPollOnce_LiveFirebird seeds a known imported chave's arrival and verifies a
// real poll cycle marks it imported. Runs only with TRACKER_TEST_FB_DSN +
// TRACKER_TEST_FB_CHAVE (a chave known to be IMPORTADO=1 in Athenas).
func TestPollOnce_LiveFirebird(t *testing.T) {
	dsn := os.Getenv("TRACKER_TEST_FB_DSN")
	chave := os.Getenv("TRACKER_TEST_FB_CHAVE")
	if dsn == "" || chave == "" {
		t.Skip("set TRACKER_TEST_FB_DSN and TRACKER_TEST_FB_CHAVE to run the live poller test")
	}
	ctx := context.Background()
	rd, err := firebird.NewReader(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer rd.Close()

	st := store.NewMemory()
	seedArrival(t, st, chave)
	res, err := New(st, rd).PollOnce(ctx)
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	t.Logf("live poll result: %+v", res)
	d, _, _ := st.GetNota(ctx, chave)
	if d.Status != model.StatusImported {
		t.Fatalf("status = %s, want imported (chave deve estar IMPORTADO=1)", d.Status)
	}
}
