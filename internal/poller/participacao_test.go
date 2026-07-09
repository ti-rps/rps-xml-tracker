package poller

import (
	"context"
	"testing"
	"time"

	"github.com/EnzzoHosaki/rps-xml-tracker/internal/firebird"
	"github.com/EnzzoHosaki/rps-xml-tracker/internal/model"
	"github.com/EnzzoHosaki/rps-xml-tracker/internal/store"
)

func row(emp, fil int, imported, ignored bool) firebird.EmpresaImport {
	e, f := emp, fil
	return firebird.EmpresaImport{
		CodigoEmpresa: &e, CodigoFilial: &f,
		Importado: imported, ImportIgnorada: ignored,
		NomeEmpresa: "EMP", CnpjFilial: "11222333000181",
	}
}

// O fluxo inteiro do M0: A importa, B pende -> a nota registra a importação de A
// mas NÃO termina (segue in-flight); B importa no ciclo seguinte -> termina.
func TestPollOnce_MultiParticipacao(t *testing.T) {
	ctx := context.Background()
	st := store.NewMemory()
	seedArrival(t, st, "K1")

	stateA := firebird.ImportState{
		Chave: "K1", Found: true, Importado: true, // representante: A importou
		Rows: []firebird.EmpresaImport{row(100, 1, true, false), row(200, 1, false, false)},
	}
	fr := &fakeReader{states: map[string]firebird.ImportState{"K1": stateA}}
	p := New(st, fr)

	res, err := p.PollOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if res.Imported != 1 {
		t.Errorf("res.Imported = %d; want 1 (contagem por nota)", res.Imported)
	}

	d, ok, err := st.GetNota(ctx, "K1")
	if err != nil || !ok {
		t.Fatalf("GetNota: ok=%v err=%v", ok, err)
	}
	if d.Status != model.StatusPendingImport {
		t.Errorf("status = %s; want pending_import (B pende) — o ponto cego do modelo antigo", d.Status)
	}
	if d.ImportedAt == nil {
		t.Error("imported_at deveria registrar a importação de A")
	}
	if len(d.Participacoes) != 2 {
		t.Fatalf("participações = %+v; want 2", d.Participacoes)
	}

	// segue no radar: B ainda não terminou.
	inflight, _ := st.ListInflightChaves(ctx, 10)
	if len(inflight) != 1 || inflight[0] != "K1" {
		t.Fatalf("in-flight = %v; want [K1]", inflight)
	}

	// mas o reconcile NÃO a conta como faltante (a importação está registrada).
	known, err := st.KnownImported(ctx, []string{"K1"})
	if err != nil || !known["K1"] {
		t.Errorf("KnownImported[K1] = %v err=%v; want true", known["K1"], err)
	}

	// ciclo seguinte: B também importou -> todas terminais -> sai do radar.
	fr.states["K1"] = firebird.ImportState{
		Chave: "K1", Found: true, Importado: true,
		Rows: []firebird.EmpresaImport{row(100, 1, true, false), row(200, 1, true, false)},
	}
	if _, err := p.PollOnce(ctx); err != nil {
		t.Fatal(err)
	}
	d, _, _ = st.GetNota(ctx, "K1")
	if d.Status != model.StatusImported {
		t.Errorf("status final = %s; want imported", d.Status)
	}
	inflight, _ = st.ListInflightChaves(ctx, 10)
	if len(inflight) != 0 {
		t.Errorf("in-flight final = %v; want vazio", inflight)
	}
}

// ReconcileOnce usa KnownImported: "importada 1/2" não é faltante eterna.
func TestReconcileOnce_MeiaImportadaNaoEhFaltante(t *testing.T) {
	ctx := context.Background()
	st := store.NewMemory()
	seedArrival(t, st, "K1")

	half := firebird.ImportState{
		Chave: "K1", Found: true, Importado: true,
		Rows: []firebird.EmpresaImport{row(100, 1, true, false), row(200, 1, false, false)},
	}
	fr := &fakeReader{states: map[string]firebird.ImportState{"K1": half}}
	p := New(st, fr)
	if _, err := p.PollOnce(ctx); err != nil {
		t.Fatal(err)
	}

	res, err := p.ReconcileOnce(ctx, time.Hour, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	if res.Missing != 0 {
		t.Errorf("missing = %d (%v); want 0 — imported_at registrado, mesmo com participação pendente",
			res.Missing, res.MissingSample)
	}
}

func TestParticipacoes_ResolucaoDeDuplicatas(t *testing.T) {
	st := firebird.ImportState{Rows: []firebird.EmpresaImport{
		row(200, 1, false, true),  // dup da mesma participação: ignorada...
		row(200, 1, false, false), // ...e pendente -> pendente vence (ainda pode importar)
		row(100, 1, false, false), // dup: pendente...
		row(100, 1, true, false),  // ...e importada -> importada vence (fato consumado)
		{CodigoEmpresa: nil},      // linha órfã: fora
	}}
	parts := st.Participacoes()
	if len(parts) != 2 {
		t.Fatalf("participações = %d; want 2 (%+v)", len(parts), parts)
	}
	if *parts[0].CodigoEmpresa != 100 || !parts[0].Importado {
		t.Errorf("part[0] = %+v; want emp 100 importada", parts[0])
	}
	if *parts[1].CodigoEmpresa != 200 || parts[1].Importado || parts[1].ImportIgnorada {
		t.Errorf("part[1] = %+v; want emp 200 pendente", parts[1])
	}
}

// Linha sem CODIGOEMPRESA não ancora participação -> cai no comportamento
// clássico (uma observação com o estado representante).
func TestPollOnce_LinhaOrfaSemEmpresa(t *testing.T) {
	ctx := context.Background()
	st := store.NewMemory()
	seedArrival(t, st, "K2")

	fr := &fakeReader{states: map[string]firebird.ImportState{
		"K2": {Chave: "K2", Found: true, Importado: true,
			Rows: []firebird.EmpresaImport{{Importado: true}}}, // sem empresa
	}}
	if _, err := New(st, fr).PollOnce(ctx); err != nil {
		t.Fatal(err)
	}
	d, _, _ := st.GetNota(ctx, "K2")
	if d.Status != model.StatusImported {
		t.Errorf("status = %s; want imported (fallback representante)", d.Status)
	}
	if len(d.Participacoes) != 0 {
		t.Errorf("participações = %+v; want vazio", d.Participacoes)
	}
}
