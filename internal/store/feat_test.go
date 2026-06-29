package store

import (
	"context"
	"testing"
	"time"

	"github.com/EnzzoHosaki/rps-xml-tracker/internal/model"
)

// chaveComNumero monta uma chave de 44 dígitos cujo nNF (posições 26–34, sem zeros
// à esquerda) é `num`. Espelha model.NumeroNota p/ exercitar o filtro `numero`.
func chaveComNumero(num string) string {
	nnf := num
	for len(nnf) < 9 {
		nnf = "0" + nnf // zero-pad à esquerda até 9 (posições 26–34)
	}
	const prefix = "0000000000000000000000000" // 25 dígitos (posições 1–25)
	const suffix = "0000000000"                // 10 dígitos (posições 35–44)
	return prefix + nnf + suffix               // 25 + 9 + 10 = 44
}

func TestListNotas_NumeroPrefixo(t *testing.T) {
	ctx := context.Background()
	m := NewMemory()
	at := time.Date(2026, 6, 8, 9, 0, 0, 0, time.UTC)
	mk := func(num string) model.Observation {
		return model.Observation{
			ChaveAcesso: chaveComNumero(num), Stage: model.StageArrival,
			EventType: model.EventFileSeen, ObservedAt: at, DocType: model.DocNFe, Source: "t",
		}
	}
	_, _, _ = m.AppendObservations(ctx, []model.Observation{mk("12345"), mk("67890")})

	check := func(name, numero string, want int) {
		_, total, err := m.ListNotas(ctx, NotaFilter{Numero: numero})
		if err != nil || total != want {
			t.Errorf("%s: numero=%q total=%d err=%v, want %d", name, numero, total, err, want)
		}
	}
	check("prefixo casa um", "123", 1)
	check("prefixo casa o outro", "678", 1)
	check("número completo", "12345", 1)
	check("não é meio de string (prefixo, não contém)", "234", 0)
	check("inexistente", "555", 0)
	check("vazio = sem filtro", "", 2)
}

func TestEmpresas_DocTypeFilter(t *testing.T) {
	ctx := context.Background()
	m := NewMemory()
	at := time.Date(2026, 6, 8, 9, 0, 0, 0, time.UTC)
	mk := func(chave string, emp int, dt model.DocType) model.Observation {
		e := emp
		return model.Observation{
			ChaveAcesso: chave, Stage: model.StageArrival, EventType: model.EventFileSeen,
			ObservedAt: at, DocType: dt, Source: "t", CodigoEmpresa: &e, CodigoFilial: ptr(1),
		}
	}
	_, _, _ = m.AppendObservations(ctx, []model.Observation{
		mk("A", 10, model.DocNFe),
		mk("B", 20, model.DocNFCe),
		mk("C", 30, model.DocNFe),
	})

	_, total, err := m.Empresas(ctx, EmpresaFilter{})
	if err != nil || total != 3 {
		t.Fatalf("sem filtro: total=%d err=%v, want 3", total, err)
	}
	_, total, err = m.Empresas(ctx, EmpresaFilter{DocType: model.DocNFe})
	if err != nil || total != 2 {
		t.Fatalf("doc_type=NFE: total=%d err=%v, want 2 (empresas 10 e 30)", total, err)
	}
	_, total, _ = m.Empresas(ctx, EmpresaFilter{DocType: model.DocNFCe})
	if total != 1 {
		t.Fatalf("doc_type=NFCE: total=%d, want 1 (empresa 20)", total)
	}
}

func TestAging_BucketsAndFilter(t *testing.T) {
	ctx := context.Background()
	m := NewMemory()
	now := time.Now()
	arrival := func(chave string, emp int, ago time.Duration) model.Observation {
		e := emp
		return model.Observation{
			ChaveAcesso: chave, Stage: model.StageArrival, EventType: model.EventFileSeen,
			ObservedAt: now.Add(-ago), DocType: model.DocNFe, Source: "t",
			CodigoEmpresa: &e, CodigoFilial: ptr(1),
		}
	}
	sync := func(chave string, ago time.Duration) model.Observation {
		return model.Observation{
			ChaveAcesso: chave, Stage: model.StageSync, EventType: model.EventFileMoved,
			ObservedAt: now.Add(-ago), DocType: model.DocNFe, Source: "t",
		}
	}
	// AR1: arrived há 2 dias -> to_sync 1-3d. AR2: arrived há 10 dias -> to_sync 7-30d.
	// SY1: synced há 12h -> to_import <1d.
	_, _, _ = m.AppendObservations(ctx, []model.Observation{
		arrival("AR1", 10, 48*time.Hour),
		arrival("AR2", 10, 240*time.Hour),
		arrival("SY1", 20, 13*time.Hour), sync("SY1", 12*time.Hour),
	})

	ag, err := m.Aging(ctx, AgingFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if ag.AnchorToSync != "arrived_at" || ag.AnchorToImport != "synced_at" {
		t.Fatalf("anchors errados: %+v", ag)
	}
	bucket := func(bs []model.AgingBucket, label string) int {
		for _, b := range bs {
			if b.Label == label {
				return b.Count
			}
		}
		t.Fatalf("faixa %q ausente", label)
		return -1
	}
	if got := bucket(ag.ToSync, "1-3d"); got != 1 {
		t.Errorf("to_sync 1-3d = %d, want 1", got)
	}
	if got := bucket(ag.ToSync, "7-30d"); got != 1 {
		t.Errorf("to_sync 7-30d = %d, want 1", got)
	}
	if got := bucket(ag.ToImport, "<1d"); got != 1 {
		t.Errorf("to_import <1d = %d, want 1", got)
	}
	// faixas canônicas completas (5 cada), com MaxDays só nas fechadas.
	if len(ag.ToSync) != 5 || len(ag.ToImport) != 5 {
		t.Fatalf("esperava 5 faixas em cada, veio to_sync=%d to_import=%d", len(ag.ToSync), len(ag.ToImport))
	}
	for _, b := range ag.ToSync {
		if (b.Label == ">30d") != (b.MaxDays == nil) {
			t.Errorf("MaxDays inconsistente na faixa %q: %v", b.Label, b.MaxDays)
		}
	}

	// filtro por empresa: só a 20 (SY1) -> to_sync zerado, to_import <1d=1.
	ag, _ = m.Aging(ctx, AgingFilter{CodigoEmpresa: ptr(20)})
	if got := bucket(ag.ToImport, "<1d"); got != 1 {
		t.Errorf("empresa 20 to_import <1d = %d, want 1", got)
	}
	if got := bucket(ag.ToSync, "1-3d"); got != 0 {
		t.Errorf("empresa 20 to_sync 1-3d = %d, want 0", got)
	}
}
