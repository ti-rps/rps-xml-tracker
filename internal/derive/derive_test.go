package derive

import (
	"testing"
	"time"

	"github.com/EnzzoHosaki/rps-xml-tracker/internal/model"
)

func ts(s string) time.Time {
	t, _ := time.Parse(time.RFC3339, s)
	return t
}

func obs(stage model.Stage, event string, at string) model.Observation {
	return model.Observation{
		ChaveAcesso: "C", Stage: stage, EventType: event, ObservedAt: ts(at),
		DocType: model.DocNFe,
	}
}

func TestDerive_FullHappyPath(t *testing.T) {
	n := Nota("C", []model.Observation{
		obs(model.StageImport, model.EventImported, "2026-06-08T10:00:00Z"),
		obs(model.StageArrival, model.EventFileSeen, "2026-06-08T09:00:00Z"),
		obs(model.StageSync, model.EventFileMoved, "2026-06-08T09:30:00Z"),
	})
	if n.Status != model.StatusImported {
		t.Fatalf("status = %s, want imported", n.Status)
	}
	if n.ArrivedAt == nil || n.SyncedAt == nil || n.ImportedAt == nil {
		t.Fatal("expected all three spans set")
	}
	if n.LatArrivalSyncS == nil || *n.LatArrivalSyncS != 1800 {
		t.Errorf("lat arrival->sync = %v, want 1800", n.LatArrivalSyncS)
	}
	if n.LatSyncImportS == nil || *n.LatSyncImportS != 1800 {
		t.Errorf("lat sync->import = %v, want 1800", n.LatSyncImportS)
	}
	if !n.FirstSeenAt.Equal(ts("2026-06-08T09:00:00Z")) {
		t.Errorf("first_seen = %v, want arrival time", n.FirstSeenAt)
	}
}

func TestDerive_SeenPendingRanksAboveSynced(t *testing.T) {
	// arquivo sincronizado (agent) + visto no Athenas aguardando (poller):
	// pending_import vence synced, e arrived continua registrado.
	n := Nota("C", []model.Observation{
		obs(model.StageArrival, model.EventFileSeen, "2026-06-08T09:00:00Z"),
		obs(model.StageSync, model.EventFileMoved, "2026-06-08T09:30:00Z"),
		obs(model.StageImport, model.EventSeenPending, "2026-06-08T09:45:00Z"),
	})
	if n.Status != model.StatusPendingImport {
		t.Fatalf("status = %s, want pending_import", n.Status)
	}
	if n.PendingAt == nil || n.SyncedAt == nil || n.ArrivedAt == nil {
		t.Fatal("expected arrived/synced/pending all set")
	}
	if n.ImportedAt != nil || n.ImportIgnored {
		t.Error("seen_pending não é terminal: não deve setar imported/ignored")
	}
}

func TestDerive_ImportedBeatsPending(t *testing.T) {
	// uma vez importada, pending_import não pode mascarar o estado terminal.
	n := Nota("C", []model.Observation{
		obs(model.StageImport, model.EventSeenPending, "2026-06-08T09:45:00Z"),
		obs(model.StageImport, model.EventImported, "2026-06-08T10:00:00Z"),
	})
	if n.Status != model.StatusImported {
		t.Fatalf("status = %s, want imported", n.Status)
	}
}

func TestDerive_ImportIgnoredIsTerminalNotStuck(t *testing.T) {
	o := obs(model.StageImport, model.EventImportIgnored, "2026-06-08T10:00:00Z")
	o.Payload = map[string]any{"motivo": "Empresa usa tela de Pre-Importacao"}
	n := Nota("C", []model.Observation{
		obs(model.StageArrival, model.EventFileSeen, "2026-06-08T09:00:00Z"),
		o,
	})
	if n.Status != model.StatusImportIgnored {
		t.Fatalf("status = %s, want import_ignored", n.Status)
	}
	if !n.ImportIgnored || n.MotivoIgnorado == "" {
		t.Errorf("expected ImportIgnored + motivo, got %v / %q", n.ImportIgnored, n.MotivoIgnorado)
	}
	if n.ImportedAt != nil {
		t.Error("import_ignored must not set imported_at")
	}
}

func TestDerive_PartialAndOrdering(t *testing.T) {
	// only arrival -> arrived; idempotent regardless of order/duplication
	a := obs(model.StageArrival, model.EventFileSeen, "2026-06-08T09:00:00Z")
	n1 := Nota("C", []model.Observation{a})
	n2 := Nota("C", []model.Observation{a, a}) // duplicate observed -> same derived state
	if n1.Status != model.StatusArrived || n2.Status != model.StatusArrived {
		t.Fatalf("status n1=%s n2=%s, want arrived", n1.Status, n2.Status)
	}
	if n1.SyncedAt != nil || n1.LatArrivalSyncS != nil {
		t.Error("no sync span -> synced_at and latency must be nil")
	}
}

func TestDerive_Empty(t *testing.T) {
	n := Nota("C", nil)
	if n.Status != "" && n.Status != model.NotaStatus("") {
		// empty observations -> zero-value status (no signal yet)
		t.Logf("empty status = %q", n.Status)
	}
}
