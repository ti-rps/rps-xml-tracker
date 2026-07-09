package derive

import (
	"testing"
	"time"

	"github.com/EnzzoHosaki/rps-xml-tracker/internal/model"
)

func impObs(chave, event string, at time.Time, emp, fil int, extra func(*model.Observation)) model.Observation {
	o := model.Observation{
		ChaveAcesso: chave, Stage: model.StageImport, EventType: event,
		ObservedAt: at, Source: "poller:firebird",
		CodigoEmpresa: &emp, CodigoFilial: &fil,
	}
	if extra != nil {
		extra(&o)
	}
	return o
}

// O ponto cego que motivou o M0: A importou, B ainda pende -> a nota NÃO é
// terminal ("importada 1/2") e segue no radar; quando B importa, aí sim termina.
func TestNota_MultiParticipacaoSeguraTerminal(t *testing.T) {
	t0 := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)
	obs := []model.Observation{
		impObs("K", model.EventSeenPending, t0, 200, 1, nil),
		impObs("K", model.EventImported, t0.Add(time.Hour), 100, 1, nil),
	}
	n, parts := NotaParticipacoes("K", obs)

	if n.Status != model.StatusPendingImport {
		t.Errorf("status agregado = %s; want pending_import (B ainda pende)", n.Status)
	}
	if n.ImportedAt == nil {
		t.Error("imported_at da nota deveria registrar a importação de A")
	}
	if len(parts) != 2 {
		t.Fatalf("participações = %d; want 2 (%+v)", len(parts), parts)
	}
	// ordenadas por empresa: 100 (imported), 200 (pending)
	if parts[0].CodigoEmpresa != 100 || parts[0].Status != model.StatusImported {
		t.Errorf("part[0] = %+v; want emp 100 imported", parts[0])
	}
	if parts[1].CodigoEmpresa != 200 || parts[1].Status != model.StatusPendingImport {
		t.Errorf("part[1] = %+v; want emp 200 pending_import", parts[1])
	}

	// B importa -> todas terminais -> nota imported.
	obs = append(obs, impObs("K", model.EventImported, t0.Add(2*time.Hour), 200, 1, nil))
	n, parts = NotaParticipacoes("K", obs)
	if n.Status != model.StatusImported {
		t.Errorf("status após B importar = %s; want imported", n.Status)
	}
	if len(parts) != 2 || parts[1].Status != model.StatusImported {
		t.Errorf("part[1] após importar = %+v", parts)
	}
}

// Ignorada + importada = todas terminais; imported vence na precedência da nota.
func TestNota_IgnoradaMaisImportadaTermina(t *testing.T) {
	t0 := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)
	obs := []model.Observation{
		impObs("K", model.EventImportIgnored, t0, 200, 1, func(o *model.Observation) {
			o.Payload = map[string]any{"motivo": "Simples Nacional"}
		}),
		impObs("K", model.EventImported, t0.Add(time.Hour), 100, 1, nil),
	}
	n, parts := NotaParticipacoes("K", obs)
	if n.Status != model.StatusImported {
		t.Errorf("status = %s; want imported (todas terminais)", n.Status)
	}
	if len(parts) != 2 || parts[1].Status != model.StatusImportIgnored || parts[1].MotivoIgnorado != "Simples Nacional" {
		t.Errorf("participações = %+v", parts)
	}
}

// Observações antigas sem empresa não formam participação e mantêm a
// precedência clássica (compatibilidade com o histórico).
func TestNota_SemEmpresaMantemClassico(t *testing.T) {
	t0 := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)
	obs := []model.Observation{
		{ChaveAcesso: "K", Stage: model.StageImport, EventType: model.EventImported, ObservedAt: t0, Source: "poller:firebird"},
	}
	n, parts := NotaParticipacoes("K", obs)
	if n.Status != model.StatusImported {
		t.Errorf("status = %s; want imported", n.Status)
	}
	if len(parts) != 0 {
		t.Errorf("participações = %+v; want vazio", parts)
	}
}

// Participação fundada só pelo sync (F1: syncer posicionou a cópia; o Athenas
// ainda não mostrou a linha) fica synced e NÃO rebaixa a nota p/ pending.
func TestNota_ParticipacaoSoComSync(t *testing.T) {
	t0 := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)
	emp, fil := 100, 1
	obs := []model.Observation{
		{ChaveAcesso: "K", Stage: model.StageArrival, EventType: model.EventFileSeen, ObservedAt: t0, Source: "agent:x"},
		{ChaveAcesso: "K", Stage: model.StageSync, EventType: model.EventFileMoved, ObservedAt: t0.Add(time.Minute),
			Source: "syncer:x", CodigoEmpresa: &emp, CodigoFilial: &fil,
			Payload: map[string]any{"url": `\EMP\123\NFe\SAIDA\202607\K.xml`}},
	}
	n, parts := NotaParticipacoes("K", obs)
	if n.Status != model.StatusSynced {
		t.Errorf("status = %s; want synced", n.Status)
	}
	if len(parts) != 1 || parts[0].Status != model.StatusSynced || parts[0].SyncedAt == nil {
		t.Fatalf("participações = %+v; want 1 synced", parts)
	}
	if parts[0].SyncURL == "" {
		t.Error("sync_url deveria vir do payload")
	}
}

// Direção define o papel da participação (saida=emitente, entrada=destinatario).
func TestParticipacoes_PapelDaDirecao(t *testing.T) {
	t0 := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)
	obs := []model.Observation{
		impObs("K", model.EventSeenPending, t0, 100, 1, func(o *model.Observation) { o.Direction = model.DirSaida }),
		impObs("K", model.EventSeenPending, t0, 200, 1, func(o *model.Observation) { o.Direction = model.DirEntrada }),
	}
	parts := Participacoes(obs)
	if len(parts) != 2 || parts[0].Papel != "emitente" || parts[1].Papel != "destinatario" {
		t.Errorf("papéis = %+v", parts)
	}
}
