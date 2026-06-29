package store

import (
	"context"
	"testing"
	"time"

	"github.com/EnzzoHosaki/rps-xml-tracker/internal/model"
)

// obs com Direction setada (simula a saída do poller, que computa a direção a partir
// do CNPJ da filial). Stage import/seen_pending -> nota pending_import.
func dirObs(chave string, emp int, dir string, at time.Time) model.Observation {
	e := emp
	return model.Observation{
		ChaveAcesso: chave, Stage: model.StageImport, EventType: model.EventSeenPending,
		ObservedAt: at, DocType: model.DocNFe, Source: "t",
		CodigoEmpresa: &e, CodigoFilial: ptr(1), Direction: dir,
	}
}

func TestDirection_DeriveAndFilters(t *testing.T) {
	ctx := context.Background()
	m := NewMemory()
	at := time.Date(2026, 6, 10, 9, 0, 0, 0, time.UTC)
	_, _, _ = m.AppendObservations(ctx, []model.Observation{
		dirObs("S", 10, model.DirSaida, at),
		dirObs("E", 20, model.DirEntrada, at),
		dirObs("N", 30, "", at), // indeterminada
	})

	// derive propaga a direção para a nota.
	det, ok, _ := m.GetNota(ctx, "S")
	if !ok || det.Direction != model.DirSaida {
		t.Fatalf("derive: nota S direction=%q ok=%v, want saida", det.Direction, ok)
	}

	// filtro em /notas.
	count := func(dir string) int {
		_, total, _ := m.ListNotas(ctx, NotaFilter{Direction: dir})
		return total
	}
	if count(model.DirSaida) != 1 || count(model.DirEntrada) != 1 {
		t.Errorf("ListNotas por direção: saida=%d entrada=%d, want 1 e 1", count(model.DirSaida), count(model.DirEntrada))
	}
	if count("") != 3 {
		t.Errorf("ListNotas sem filtro de direção: %d, want 3", count(""))
	}

	// filtro em /empresas.
	_, total, _ := m.Empresas(ctx, EmpresaFilter{Direction: model.DirSaida})
	if total != 1 {
		t.Errorf("Empresas direction=saida: total=%d, want 1 (só empresa 10)", total)
	}

	// filtro em /metrics/aging (todas as 3 são pending_import -> to_import).
	ag, _ := m.Aging(ctx, AgingFilter{Direction: model.DirEntrada})
	var toImport int
	for _, b := range ag.ToImport {
		toImport += b.Count
	}
	if toImport != 1 {
		t.Errorf("Aging direction=entrada: to_import total=%d, want 1", toImport)
	}
}
