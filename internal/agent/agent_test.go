package agent

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/EnzzoHosaki/rps-xml-tracker/internal/model"
)

type fakeSink struct{ got []model.Observation }

func (f *fakeSink) Submit(_ context.Context, b []model.Observation) error {
	f.got = append(f.got, b...)
	return nil
}
func (f *fakeSink) FlushSpool(context.Context) (int, error) { return 0, nil }

const nfeXML = `<nfeProc><NFe><infNFe Id="NFe35250712345678000190550010000001231000001234"><ide><mod>55</mod></ide></infNFe></NFe></nfeProc>`
const nfseXML = `<CompNfse><Nfse><InfNfse><Numero>123</Numero></InfNfse></Nfse></CompNfse>`

func write(t *testing.T, dir, rel, content string) {
	t.Helper()
	p := filepath.Join(dir, rel)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

// backdate sets a file's modtime so the cutoff logic is deterministic in tests.
func backdate(t *testing.T, dir, rel string, mt time.Time) {
	t.Helper()
	p := filepath.Join(dir, rel)
	if err := os.Chtimes(p, mt, mt); err != nil {
		t.Fatal(err)
	}
}

func newAgent(t *testing.T, root string, sink *fakeSink, backfill bool) *Agent {
	t.Helper()
	a, err := New(Config{
		Name:      "TEST",
		Roots:     []Root{{Path: root, Stage: model.StageArrival, Event: model.EventFileSeen}},
		StatePath: filepath.Join(t.TempDir(), "state.db"),
		StableAge: time.Nanosecond, // don't wait for files to "settle" in tests
		Backfill:  backfill,
	}, sink)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { a.Close() })
	return a
}

func TestScanOnce_EmitsNFeSkipsNFSe_WithEmpresaFromPath(t *testing.T) {
	root := t.TempDir()
	write(t, root, "1203-1 ESTRELA DALVA/NFE/nota.xml", nfeXML)
	write(t, root, "1203-1 ESTRELA DALVA/XML NFS/servico.xml", nfseXML)

	sink := &fakeSink{}
	a := newAgent(t, root, sink, true) // backfill=true -> emit immediately

	res, err := a.ScanOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Scanned != 2 || res.Emitted != 1 || res.SkippedNoChave != 1 {
		t.Fatalf("res=%+v want scanned=2 emitted=1 skipped=1", res)
	}
	if len(sink.got) != 1 {
		t.Fatalf("submitted %d obs, want 1", len(sink.got))
	}
	o := sink.got[0]
	if o.ChaveAcesso != "35250712345678000190550010000001231000001234" {
		t.Errorf("chave = %s", o.ChaveAcesso)
	}
	if o.DocType != model.DocNFe || o.Stage != model.StageArrival {
		t.Errorf("docType=%s stage=%s", o.DocType, o.Stage)
	}
	if o.CodigoEmpresa == nil || *o.CodigoEmpresa != 1203 || o.CodigoFilial == nil || *o.CodigoFilial != 1 {
		t.Errorf("empresa from path wrong: emp=%v fil=%v", o.CodigoEmpresa, o.CodigoFilial)
	}
	if o.FileHash == "" {
		t.Error("file hash empty")
	}

	// second scan: nothing new (idempotent)
	sink.got = nil
	res2, _ := a.ScanOnce(context.Background())
	if res2.New != 0 || len(sink.got) != 0 {
		t.Fatalf("second scan emitted again: new=%d got=%d", res2.New, len(sink.got))
	}
}

func TestScanOnce_BackfillFalse_SeedsThenEmitsOnlyNew(t *testing.T) {
	root := t.TempDir()
	write(t, root, "1100-1 ACME/old.xml", nfeXML) // pre-existing backlog

	t0 := time.Date(2026, 6, 9, 12, 0, 0, 0, time.UTC)
	backdate(t, root, "1100-1 ACME/old.xml", t0.Add(-time.Hour)) // before the cutoff

	sink := &fakeSink{}
	a := newAgent(t, root, sink, false) // backfill=false -> first run sets cutoff
	a.now = func() time.Time { return t0 }

	res, err := a.ScanOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !res.Seeded || res.Emitted != 0 || len(sink.got) != 0 {
		t.Fatalf("first run should set cutoff without emitting backlog: res=%+v got=%d", res, len(sink.got))
	}

	// a NEW file arrives after the cutoff -> emitted on the next scan
	write(t, root, "1100-1 ACME/new.xml",
		`<NFe><infNFe Id="NFe35250799999999000191650010000005551000005550"><ide><mod>65</mod></ide></infNFe></NFe>`)
	a.now = func() time.Time { return t0.Add(time.Hour) }      // advance the clock
	backdate(t, root, "1100-1 ACME/new.xml", t0.Add(time.Minute)) // after cutoff, stable

	res2, _ := a.ScanOnce(context.Background())
	if res2.Seeded || res2.Emitted != 1 || len(sink.got) != 1 {
		t.Fatalf("post-cutoff scan: res=%+v got=%d (want emitted=1)", res2, len(sink.got))
	}
	if sink.got[0].DocType != model.DocNFCe {
		t.Errorf("docType=%s want NFCE", sink.got[0].DocType)
	}
}

func TestScanOnce_SyncStageUsesDetectionTime(t *testing.T) {
	// O mover preserva o mtime ao mover p/ SINCRONIZADO; a etapa sync deve
	// carimbar a hora de DETECÇÃO (now), não o mtime, senão a latência dá 0.
	root := t.TempDir()
	write(t, root, "1100-1 ACME/nota.xml", nfeXML)
	mtime := time.Date(2026, 6, 9, 1, 0, 0, 0, time.UTC)
	backdate(t, root, "1100-1 ACME/nota.xml", mtime)

	sink := &fakeSink{}
	a, err := New(Config{
		Name:      "TEST",
		Roots:     []Root{{Path: root, Stage: model.StageSync, Event: model.EventFileMoved}},
		StatePath: filepath.Join(t.TempDir(), "state.db"),
		StableAge: time.Nanosecond,
		Backfill:  true, // emite direto, sem cutoff
	}, sink)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { a.Close() })

	detect := time.Date(2026, 6, 10, 8, 0, 0, 0, time.UTC)
	a.now = func() time.Time { return detect }

	if _, err := a.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(sink.got) != 1 {
		t.Fatalf("emitidas %d, want 1", len(sink.got))
	}
	o := sink.got[0]
	if o.Stage != model.StageSync {
		t.Fatalf("stage=%s want sync", o.Stage)
	}
	if !o.ObservedAt.Equal(detect) {
		t.Errorf("sync ObservedAt=%v, want hora de detecção %v (não o mtime %v)", o.ObservedAt, detect, mtime)
	}
}

func TestScanOnce_ArrivalStageUsesFileMtime(t *testing.T) {
	root := t.TempDir()
	write(t, root, "1100-1 ACME/nota.xml", nfeXML)
	mtime := time.Date(2026, 6, 9, 1, 0, 0, 0, time.UTC)
	backdate(t, root, "1100-1 ACME/nota.xml", mtime)

	sink := &fakeSink{}
	a := newAgent(t, root, sink, true) // arrival root, backfill=true
	a.now = func() time.Time { return time.Date(2026, 6, 10, 8, 0, 0, 0, time.UTC) }

	if _, err := a.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(sink.got) != 1 {
		t.Fatalf("emitidas %d, want 1", len(sink.got))
	}
	if !sink.got[0].ObservedAt.Equal(mtime) {
		t.Errorf("arrival ObservedAt=%v, want mtime do arquivo %v", sink.got[0].ObservedAt, mtime)
	}
}

func TestSeedCutoff_PersistsAndPreventsReseed(t *testing.T) {
	root := t.TempDir()
	write(t, root, "1100-1 ACME/old.xml", nfeXML)
	t0 := time.Date(2026, 6, 9, 12, 0, 0, 0, time.UTC)
	backdate(t, root, "1100-1 ACME/old.xml", t0.Add(-time.Hour))

	statePath := filepath.Join(t.TempDir(), "state.db")
	mk := func() *Agent {
		a, err := New(Config{
			Name:      "TEST",
			Roots:     []Root{{Path: root, Stage: model.StageArrival, Event: model.EventFileSeen}},
			StatePath: statePath,
			StableAge: time.Nanosecond,
		}, &fakeSink{})
		if err != nil {
			t.Fatal(err)
		}
		a.now = func() time.Time { return t0 }
		return a
	}

	a1 := mk()
	r1, err := a1.ScanOnce(context.Background())
	if err != nil || !r1.Seeded {
		t.Fatalf("first run should seed: r=%+v err=%v", r1, err)
	}
	if cut, ok := a1.seedCutoff(); !ok || !cut.Equal(t0) {
		t.Fatalf("cutoff not persisted: ok=%v cut=%v want %v", ok, cut, t0)
	}
	a1.Close()

	// reopen on the SAME state file: must NOT re-seed (simulates a restart after
	// an interrupted first scan).
	a2 := mk()
	defer a2.Close()
	r2, err := a2.ScanOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if r2.Seeded {
		t.Fatalf("reopened agent re-seeded; persisted cutoff should make Seeded=false")
	}
}

// TestScanRoots_SubsetByStage garante que scanRoots varre APENAS os roots passados
// e marca o estágio certo — base do RunSplit (chegada e sync em loops separados).
func TestScanRoots_SubsetByStage(t *testing.T) {
	arrivalDir := t.TempDir()
	syncDir := t.TempDir()
	write(t, arrivalDir, "1203-1 EMP/NFE/a.xml", nfeXML)
	write(t, syncDir, "1203-1 EMP/NFE/s.xml", nfeXML)

	sink := &fakeSink{}
	a, err := New(Config{
		Name: "TEST",
		Roots: []Root{
			{Path: arrivalDir, Stage: model.StageArrival, Event: model.EventFileSeen},
			{Path: syncDir, Stage: model.StageSync, Event: model.EventFileMoved},
		},
		StatePath: filepath.Join(t.TempDir(), "state.db"),
		StableAge: time.Nanosecond,
		Backfill:  true,
	}, sink)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { a.Close() })

	// só o root de chegada -> 1 obs de chegada
	if _, err := a.scanRoots(context.Background(), "chegada", []Root{a.cfg.Roots[0]}); err != nil {
		t.Fatal(err)
	}
	if len(sink.got) != 1 || sink.got[0].Stage != model.StageArrival {
		t.Fatalf("esperava 1 obs de chegada, veio %+v", sink.got)
	}

	// só o root de sync -> +1 obs de sync (a de chegada não é re-varrida aqui)
	if _, err := a.scanRoots(context.Background(), "sync", []Root{a.cfg.Roots[1]}); err != nil {
		t.Fatal(err)
	}
	if len(sink.got) != 2 || sink.got[1].Stage != model.StageSync {
		t.Fatalf("esperava +1 obs de sync, veio %+v", sink.got)
	}
}
