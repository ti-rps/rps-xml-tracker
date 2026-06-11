// Package derive computes a nota's derived state from its observations.
//
// This is a PURE function (no I/O): given all observations for one chave, it
// produces the Nota. That keeps the core logic trivially testable and makes the
// state recomputable/idempotent — re-applying the same observations yields the
// same Nota (a Fase 0 requirement for "nota sumida" investigation and re-scan).
package derive

import (
	"sort"
	"time"

	"github.com/EnzzoHosaki/rps-xml-tracker/internal/model"
)

// Nota derives the state for chave from its observations (any order).
// Precedence of terminal states: import_ignored and imported are terminal; an
// import_ignored signal is NOT treated as stuck/lost (Fase 0: it's a legitimate
// business-config outcome).
func Nota(chave string, obs []model.Observation) model.Nota {
	n := model.Nota{ChaveAcesso: chave, DocType: model.DocUnknown}
	if len(obs) == 0 {
		return n
	}

	// Process in chronological order so "first seen" / span timestamps are stable.
	sorted := append([]model.Observation(nil), obs...)
	sort.SliceStable(sorted, func(i, j int) bool {
		return sorted[i].ObservedAt.Before(sorted[j].ObservedAt)
	})

	n.FirstSeenAt = sorted[0].ObservedAt
	n.LastUpdateAt = sorted[len(sorted)-1].ObservedAt

	for _, o := range sorted {
		if o.DocType != "" && o.DocType != model.DocUnknown {
			n.DocType = o.DocType
		}
		if o.CodigoEmpresa != nil {
			n.CodigoEmpresa = o.CodigoEmpresa
		}
		if o.CodigoFilial != nil {
			n.CodigoFilial = o.CodigoFilial
		}
		// metadados: primeira observação não-vazia vence (são imutáveis por nota)
		setIfEmpty(&n.NomeEmpresa, o.NomeEmpresa)
		setIfEmpty(&n.CnpjEmitente, o.CnpjEmitente)
		setIfEmpty(&n.NomeEmitente, o.NomeEmitente)
		setIfEmpty(&n.CnpjDestinatario, o.CnpjDestinatario)
		setIfEmpty(&n.NomeDestinatario, o.NomeDestinatario)
		setIfEmpty(&n.DataEmissao, o.DataEmissao)
		if n.ValorTotal == nil && o.ValorTotal != nil {
			n.ValorTotal = o.ValorTotal
		}
		switch o.Stage {
		case model.StageArrival:
			setIfEarlier(&n.ArrivedAt, o.ObservedAt)
		case model.StageSync:
			setIfEarlier(&n.SyncedAt, o.ObservedAt)
		case model.StageImport:
			switch o.EventType {
			case model.EventImportIgnored:
				n.ImportIgnored = true
				if m, ok := o.Payload["motivo"].(string); ok {
					n.MotivoIgnorado = m
				}
			case model.EventSeenPending:
				setIfEarlier(&n.PendingAt, o.ObservedAt)
			default: // imported
				setIfEarlier(&n.ImportedAt, o.ObservedAt)
			}
		}
	}

	n.Status = status(n)
	n.LatArrivalSyncS = diffSeconds(n.ArrivedAt, n.SyncedAt)
	n.LatSyncImportS = diffSeconds(n.SyncedAt, n.ImportedAt)
	return n
}

// status applies the precedence:
//
//	imported > import_ignored > pending_import > synced > arrived
//
// pending_import (visto no Athenas via poller, IMPORTADO=0) rankeia ACIMA de
// synced (arquivo posicionado pelo agent): a nota progrediu — o Athenas já a
// enxergou e só falta importar. O default final continua pending_import como
// fallback para a observação degenerada (stage desconhecido).
func status(n model.Nota) model.NotaStatus {
	switch {
	case n.ImportedAt != nil:
		return model.StatusImported
	case n.ImportIgnored:
		return model.StatusImportIgnored
	case n.PendingAt != nil:
		return model.StatusPendingImport
	case n.SyncedAt != nil:
		return model.StatusSynced
	case n.ArrivedAt != nil:
		return model.StatusArrived
	default:
		return model.StatusPendingImport
	}
}

func setIfEmpty(dst *string, v string) {
	if *dst == "" && v != "" {
		*dst = v
	}
}

func setIfEarlier(dst **time.Time, t time.Time) {
	if *dst == nil || t.Before(**dst) {
		tt := t
		*dst = &tt
	}
}

func diffSeconds(from, to *time.Time) *int64 {
	if from == nil || to == nil {
		return nil
	}
	s := int64(to.Sub(*from).Seconds())
	return &s
}
