package poller

import (
	"context"
	"os"
	"testing"
	"time"
	"unicode/utf8"

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

// SweepImported retorna todas as entradas importadas (ignora `since` — é um fake).
func (f fakeReader) SweepImported(_ context.Context, _ time.Time) (map[string]firebird.ImportState, error) {
	out := map[string]firebird.ImportState{}
	for k, v := range f.states {
		if v.Importado {
			out[k] = v
		}
	}
	return out, nil
}

func ptr(i int) *int { return &i }

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

func TestRepollImportIgnored_CorrectsMisattributed(t *testing.T) {
	ctx := context.Background()
	st := store.NewMemory()
	// nota que ficou import_ignored (terceiro ROSEMBERG ignorou antes da dona).
	seedArrival(t, st, "CLW")
	_, _, err := st.AppendObservations(ctx, []model.Observation{{
		ChaveAcesso: "CLW", Stage: model.StageImport, EventType: model.EventImportIgnored,
		ObservedAt: time.Date(2026, 6, 10, 5, 0, 0, 0, time.UTC), Source: "poller:firebird",
		CodigoEmpresa: ptr(120), NomeEmpresa: "ROSEMBERG PEREIRA DE SOUZA",
		Payload: map[string]any{"motivo": "nota de terceiros"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if d, _, _ := st.GetNota(ctx, "CLW"); d.Status != model.StatusImportIgnored {
		t.Fatalf("pré-condição: status=%s want import_ignored", d.Status)
	}

	// agora o Firebird resolve para a dona (CLW, IMPORTADO=1).
	fr := fakeReader{states: map[string]firebird.ImportState{
		"CLW": {Found: true, Importado: true, CodigoEmpresa: ptr(165), NomeEmpresa: "CLW CHURRASCARIA LTDA"},
	}}
	res, err := New(st, fr).RepollImportIgnored(ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	if res.Checked != 1 || res.Corrected != 1 {
		t.Fatalf("res = %+v, want checked=1 corrigidas=1", res)
	}
	// imported vence import_ignored, e empresa (código E nome) acompanha a correção.
	d, _, _ := st.GetNota(ctx, "CLW")
	if d.Status != model.StatusImported {
		t.Errorf("status=%s want imported", d.Status)
	}
	if d.CodigoEmpresa == nil || *d.CodigoEmpresa != 165 || d.NomeEmpresa != "CLW CHURRASCARIA LTDA" {
		t.Errorf("empresa=%v/%q want 165/CLW (não ROSEMBERG)", d.CodigoEmpresa, d.NomeEmpresa)
	}
}

func TestRepollImportIgnored_FixPendingRevertsToPending(t *testing.T) {
	ctx := context.Background()
	st := store.NewMemory()
	// nota presa em import_ignored por um TERCEIRO, mas a dona ainda não importou.
	seedArrival(t, st, "PEND")
	_, _, err := st.AppendObservations(ctx, []model.Observation{
		{ChaveAcesso: "PEND", Stage: model.StageSync, EventType: model.EventFileMoved,
			ObservedAt: time.Date(2026, 6, 9, 9, 0, 0, 0, time.UTC), Source: "agent:test"},
		{ChaveAcesso: "PEND", Stage: model.StageImport, EventType: model.EventImportIgnored,
			ObservedAt: time.Date(2026, 6, 10, 5, 0, 0, 0, time.UTC), Source: "poller:firebird",
			CodigoEmpresa: ptr(120), NomeEmpresa: "TERCEIRO", Payload: map[string]any{"motivo": "de terceiros"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if d, _, _ := st.GetNota(ctx, "PEND"); d.Status != model.StatusImportIgnored {
		t.Fatalf("pré: status=%s want import_ignored", d.Status)
	}

	// Firebird resolve p/ pendente (dona 0/0, ninguém importou).
	fr := fakeReader{states: map[string]firebird.ImportState{
		"PEND": {Found: true, CodigoEmpresa: ptr(165), NomeEmpresa: "DONA LTDA"},
	}}

	// sem fix: não toca, conta StillPending.
	res, _ := New(st, fr).RepollImportIgnored(ctx, false)
	if res.StillPending != 1 || res.FixedPending != 0 {
		t.Fatalf("sem fix: res=%+v want StillPending=1", res)
	}
	if d, _, _ := st.GetNota(ctx, "PEND"); d.Status != model.StatusImportIgnored {
		t.Errorf("sem fix não deveria mudar; status=%s", d.Status)
	}

	// com fix: remove a import_ignored errada + emite seen_pending -> pending_import.
	res, err = New(st, fr).RepollImportIgnored(ctx, true)
	if err != nil {
		t.Fatal(err)
	}
	if res.FixedPending != 1 {
		t.Fatalf("com fix: res=%+v want FixedPending=1", res)
	}
	d, _, _ := st.GetNota(ctx, "PEND")
	if d.Status != model.StatusPendingImport {
		t.Errorf("status=%s want pending_import", d.Status)
	}
	if d.ImportIgnored {
		t.Error("a observação import_ignored deveria ter sido removida")
	}
	if d.CodigoEmpresa == nil || *d.CodigoEmpresa != 165 {
		t.Errorf("empresa=%v want 165 (dona, não terceiro 120)", d.CodigoEmpresa)
	}
}

func TestToUTF8(t *testing.T) {
	// Latin-1 cru do Firebird (charset=NONE): 0xC1='Á' + 'R' -> "ÁR" UTF-8 válido.
	got := toUTF8(string([]byte{0xc1, 0x52}))
	if got != "ÁR" || !utf8.ValidString(got) {
		t.Errorf("toUTF8(latin1) = %q (valid=%v), want \"ÁR\"", got, utf8.ValidString(got))
	}
	// já UTF-8 válido (acento multibyte) passa intacto.
	if got := toUTF8("AÇÃO"); got != "AÇÃO" {
		t.Errorf("toUTF8(utf8) = %q, want intacto", got)
	}
	// ASCII puro intacto.
	if got := toUTF8("CLW LTDA"); got != "CLW LTDA" {
		t.Errorf("toUTF8(ascii) = %q, want intacto", got)
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

func TestSweepOnce_EmitsImportedAndIsIdempotent(t *testing.T) {
	ctx := context.Background()
	st := store.NewMemory()
	seedArrival(t, st, "SW_IMPORTED")
	seedArrival(t, st, "SW_PENDING")

	fr := fakeReader{states: map[string]firebird.ImportState{
		"SW_IMPORTED": {Found: true, Importado: true, Chave: "SW_IMPORTED"},
		"SW_PENDING":  {Found: true, Importado: false, Chave: "SW_PENDING"},
	}}
	p := New(st, fr)
	since := time.Now().Add(-1 * time.Hour)

	res, err := p.SweepOnce(ctx, since)
	if err != nil {
		t.Fatal(err)
	}
	if res.Found != 1 || res.Emitted != 1 || res.Skipped != 0 {
		t.Fatalf("sweep1: res=%+v, want Found=1 Emitted=1 Skipped=0", res)
	}
	d, _, _ := st.GetNota(ctx, "SW_IMPORTED")
	if d.Status != model.StatusImported {
		t.Errorf("status=%s, want imported", d.Status)
	}

	// segunda passada: dedup rejeita a observação duplicada
	res2, err := p.SweepOnce(ctx, since)
	if err != nil {
		t.Fatal(err)
	}
	if res2.Emitted != 0 || res2.Skipped != 1 {
		t.Fatalf("sweep2 (dedup): res=%+v, want Emitted=0 Skipped=1", res2)
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
