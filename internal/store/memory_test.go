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

func TestListNotas_Filters(t *testing.T) {
	ctx := context.Background()
	m := NewMemory()
	at := time.Date(2026, 6, 8, 9, 0, 0, 0, time.UTC)
	mk := func(chave, emp, cnpjE, emissao string) model.Observation {
		return model.Observation{
			ChaveAcesso: chave, Stage: model.StageArrival, EventType: model.EventFileSeen,
			ObservedAt: at, DocType: model.DocNFe, Source: "t",
			NomeEmpresa: emp, CnpjEmitente: cnpjE, DataEmissao: emissao,
		}
	}
	_, _, _ = m.AppendObservations(ctx, []model.Observation{
		mk("A", "PJA INDUSTRIA", "15484297000185", "2026-06-05"),
		mk("B", "OUTRA EMPRESA", "99999999000191", "2026-05-20"),
	})

	check := func(name string, f NotaFilter, wantTotal int) {
		_, total, err := m.ListNotas(ctx, f)
		if err != nil || total != wantTotal {
			t.Errorf("%s: total=%d err=%v, want %d", name, total, err, wantTotal)
		}
	}
	check("empresa", NotaFilter{EmpresaQuery: "pja"}, 1)          // case-insensitive
	check("cnpj", NotaFilter{Cnpj: "15484297000185"}, 1)
	check("emissao range", NotaFilter{DateField: "emissao", From: "2026-06-01", To: "2026-06-30"}, 1)
	check("emissao range ambas", NotaFilter{DateField: "emissao", From: "2026-05-01"}, 2)
	check("sem filtro", NotaFilter{}, 2)
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

	emps, total, err := m.Empresas(ctx, EmpresaFilter{PendentesOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(emps) != 2 || total != 2 {
		t.Fatalf("empresas pendentes = %d total=%d want 2/2", len(emps), total)
	}
	// sorted by codigo_empresa: 1100 then 1203
	if *emps[0].CodigoEmpresa != 1100 || emps[0].Arrived != 1 || emps[0].InTransit != 1 {
		t.Errorf("emp[0]=%+v", emps[0])
	}
	if *emps[1].CodigoEmpresa != 1203 || emps[1].Synced != 1 || emps[1].Imported != 1 || emps[1].InTransit != 1 {
		t.Errorf("emp[1]=%+v", emps[1])
	}

	// paginação: limit=1 + offset=1 retorna só a 2ª empresa, mas total=2
	page, ptotal, err := m.Empresas(ctx, EmpresaFilter{PendentesOnly: true, Limit: 1, Offset: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(page) != 1 || ptotal != 2 || *page[0].CodigoEmpresa != 1203 {
		t.Errorf("paginação inesperada: items=%d total=%d %+v", len(page), ptotal, page)
	}
}

func TestEmpresas_SemEmpresaBucket(t *testing.T) {
	ctx := context.Background()
	m := NewMemory()
	at := time.Date(2026, 6, 8, 9, 0, 0, 0, time.UTC)
	// Duas notas sem empresa (codigo_empresa nil) + uma com empresa.
	semEmp := model.Observation{
		ChaveAcesso: "X", Stage: model.StageArrival, EventType: model.EventFileSeen,
		ObservedAt: at, DocType: model.DocNFe, Source: "t",
	}
	semEmp2 := semEmp
	semEmp2.ChaveAcesso = "Y"
	_, _, _ = m.AppendObservations(ctx, []model.Observation{
		semEmp, semEmp2,
		obsFor("Z", model.StageArrival, model.EventFileSeen, at, 1203),
	})

	emps, total, err := m.Empresas(ctx, EmpresaFilter{})
	if err != nil {
		t.Fatal(err)
	}
	// 1203 + 1 bucket "sem empresa" = 2 linhas; bucket colapsa X e Y numa só.
	if len(emps) != 2 || total != 2 {
		t.Fatalf("empresas=%d total=%d want 2/2: %+v", len(emps), total, emps)
	}
	// bucket "sem empresa" ordena por último (codigo nil).
	bucket := emps[len(emps)-1]
	if bucket.CodigoEmpresa != nil || bucket.CodigoFilial != nil {
		t.Errorf("bucket sem empresa deveria ter codigo nil: %+v", bucket)
	}
	if bucket.Arrived != 2 {
		t.Errorf("bucket sem empresa arrived=%d want 2 (X+Y colapsados)", bucket.Arrived)
	}

	// drill-down: sem_empresa=true retorna só X e Y.
	_, semTotal, err := m.ListNotas(ctx, NotaFilter{SemEmpresa: true})
	if err != nil || semTotal != 2 {
		t.Errorf("sem_empresa drill-down total=%d err=%v want 2", semTotal, err)
	}
}

func TestListNotas_CodigoFilial(t *testing.T) {
	ctx := context.Background()
	m := NewMemory()
	at := time.Date(2026, 6, 8, 9, 0, 0, 0, time.UTC)
	mk := func(chave string, fil int) model.Observation {
		return model.Observation{
			ChaveAcesso: chave, Stage: model.StageArrival, EventType: model.EventFileSeen,
			ObservedAt: at, DocType: model.DocNFe, Source: "t",
			CodigoEmpresa: ptr(1203), CodigoFilial: ptr(fil),
		}
	}
	_, _, _ = m.AppendObservations(ctx, []model.Observation{mk("A", 1), mk("B", 2), mk("C", 2)})

	// codigo_empresa + codigo_filial combinam via AND.
	_, total, err := m.ListNotas(ctx, NotaFilter{CodigoEmpresa: ptr(1203), CodigoFilial: ptr(2)})
	if err != nil || total != 2 {
		t.Errorf("filial 2 total=%d err=%v want 2", total, err)
	}
	_, total, _ = m.ListNotas(ctx, NotaFilter{CodigoEmpresa: ptr(1203), CodigoFilial: ptr(1)})
	if total != 1 {
		t.Errorf("filial 1 total=%d want 1", total)
	}
}
