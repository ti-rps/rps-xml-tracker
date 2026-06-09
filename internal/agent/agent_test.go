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

	sink := &fakeSink{}
	a := newAgent(t, root, sink, false) // backfill=false -> first scan seeds

	res, err := a.ScanOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !res.Seeded || res.Emitted != 0 || len(sink.got) != 0 {
		t.Fatalf("first scan should seed without emitting: res=%+v got=%d", res, len(sink.got))
	}

	// a NEW file arrives after seeding -> emitted
	write(t, root, "1100-1 ACME/new.xml",
		`<NFe><infNFe Id="NFe35250799999999000191650010000005551000005550"><ide><mod>65</mod></ide></infNFe></NFe>`)
	res2, _ := a.ScanOnce(context.Background())
	if res2.Seeded || res2.Emitted != 1 || len(sink.got) != 1 {
		t.Fatalf("post-seed scan: res=%+v got=%d (want emitted=1)", res2, len(sink.got))
	}
	if sink.got[0].DocType != model.DocNFCe {
		t.Errorf("docType=%s want NFCE", sink.got[0].DocType)
	}
}
