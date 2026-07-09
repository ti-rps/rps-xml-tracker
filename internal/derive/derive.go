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
	n, _ := NotaParticipacoes(chave, obs)
	return n
}

// Participacoes derives only the per-empresa participações for the observations.
func Participacoes(obs []model.Observation) []model.Participacao {
	return participacoes(sortObs(obs))
}

// NotaParticipacoes derives the nota AND its participações in one pass over the
// sorted observations (o recompute do store materializa os dois juntos).
func NotaParticipacoes(chave string, obs []model.Observation) (model.Nota, []model.Participacao) {
	n := model.Nota{ChaveAcesso: chave, DocType: model.DocUnknown}
	if len(obs) == 0 {
		return n, nil
	}

	// Process in chronological order so "first seen" / span timestamps are stable.
	sorted := sortObs(obs)

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
		// empresa (código acima e nome aqui): ÚLTIMO não-vazio vence, como o código.
		// A qual empresa a nota é atribuída depende da linha do Firebird (selectState)
		// e pode mudar numa correção (terceiro->dona); o nome tem de acompanhar, senão
		// fica divergente (ex.: codigo=CLW mas nome=ROSEMBERG).
		if o.NomeEmpresa != "" {
			n.NomeEmpresa = o.NomeEmpresa
		}
		// direção acompanha a atribuição de empresa (último não-vazio vence): vem do
		// poller (compara CNPJ da filial com emitente/destinatário). Obs do agente
		// (chegada/sync) não a trazem, então não sobrescrevem com vazio.
		if o.Direction != "" {
			n.Direction = o.Direction
		}
		// metadados imutáveis por nota: primeira observação não-vazia vence
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

	parts := participacoes(sorted)
	n.Status = status(n, parts)
	n.NumeroNota = model.NumeroNota(chave)
	n.LatArrivalSyncS = diffSeconds(n.ArrivedAt, n.SyncedAt)
	n.LatSyncImportS = diffSeconds(n.SyncedAt, n.ImportedAt)
	return n, parts
}

func sortObs(obs []model.Observation) []model.Observation {
	sorted := append([]model.Observation(nil), obs...)
	sort.SliceStable(sorted, func(i, j int) bool {
		return sorted[i].ObservedAt.Before(sorted[j].ObservedAt)
	})
	return sorted
}

// participacoes agrupa as observações de import/sync POR EMPRESA (M0): cada
// participação (empresa, filial) tem seu próprio ciclo pending→imported/ignored
// no Athenas, e o sync da cópia física daquela empresa (F1). Observações sem
// empresa (chegada do agente, linhas antigas) não formam participação. Dentro de
// uma participação, imported é fato consumado e vence ignored (correção
// terceiro→dona já usava essa precedência).
func participacoes(sorted []model.Observation) []model.Participacao {
	type key struct{ emp, fil int }
	idx := map[key]int{}
	var parts []model.Participacao
	for _, o := range sorted {
		if o.CodigoEmpresa == nil || (o.Stage != model.StageImport && o.Stage != model.StageSync) {
			continue
		}
		k := key{*o.CodigoEmpresa, 0}
		if o.CodigoFilial != nil {
			k.fil = *o.CodigoFilial
		}
		i, ok := idx[k]
		if !ok {
			i = len(parts)
			idx[k] = i
			parts = append(parts, model.Participacao{CodigoEmpresa: k.emp, CodigoFilial: k.fil})
		}
		p := &parts[i]
		if o.NomeEmpresa != "" {
			p.NomeEmpresa = o.NomeEmpresa
		}
		if o.Direction != "" {
			p.Direction = o.Direction
		}
		if o.Stage == model.StageSync {
			setIfEarlier(&p.SyncedAt, o.ObservedAt)
			if u, ok := o.Payload["url"].(string); ok && p.SyncURL == "" {
				p.SyncURL = u
			}
			continue
		}
		switch o.EventType {
		case model.EventImported:
			setIfEarlier(&p.ImportedAt, o.ObservedAt)
		case model.EventImportIgnored:
			p.MotivoIgnorado = "" // pode ter vindo de outra observação; a mais nova vence
			if m, ok := o.Payload["motivo"].(string); ok {
				p.MotivoIgnorado = m
			}
			p.Status = model.StatusImportIgnored // marcado; resolvido abaixo
		case model.EventSeenPending:
			setIfEarlier(&p.PendingAt, o.ObservedAt)
		}
	}
	for i := range parts {
		p := &parts[i]
		switch {
		case p.ImportedAt != nil:
			p.Status = model.StatusImported
		case p.Status == model.StatusImportIgnored:
			// terminal por config; mantém
		case p.PendingAt != nil:
			p.Status = model.StatusPendingImport
		case p.SyncedAt != nil:
			// fundada só pelo sync (F1: syncer posicionou a cópia desta empresa;
			// o Athenas ainda não mostrou a linha) — a participação está synced.
			p.Status = model.StatusSynced
		default:
			p.Status = model.StatusPendingImport
		}
		switch p.Direction {
		case model.DirSaida:
			p.Papel = "emitente"
		case model.DirEntrada:
			p.Papel = "destinatario"
		}
	}
	sort.SliceStable(parts, func(i, j int) bool {
		if parts[i].CodigoEmpresa != parts[j].CodigoEmpresa {
			return parts[i].CodigoEmpresa < parts[j].CodigoEmpresa
		}
		return parts[i].CodigoFilial < parts[j].CodigoFilial
	})
	return parts
}

// status applies the precedence:
//
//	imported > import_ignored > pending_import > synced > arrived
//
// M0: a nota só é TERMINAL quando TODAS as participações terminam. "Importada
// 1/2" (A importou, B pendente) fica pending_import e segue no radar do poller —
// era exatamente o ponto cego do modelo colapsado (a importação de B nunca era
// registrada). Com participação não-terminal, o status reflete o progresso
// não-terminal mais avançado (pendente > synced > arrived). Nota sem
// participações (chegada pura, observações antigas sem empresa) mantém a
// precedência clássica.
//
// pending_import (visto no Athenas via poller, IMPORTADO=0) rankeia ACIMA de
// synced (arquivo posicionado pelo agent): a nota progrediu — o Athenas já a
// enxergou e só falta importar. O default final continua pending_import como
// fallback para a observação degenerada (stage desconhecido).
func status(n model.Nota, parts []model.Participacao) model.NotaStatus {
	aberta, pendente := false, false
	for _, p := range parts {
		if p.Status != model.StatusImported && p.Status != model.StatusImportIgnored {
			aberta = true
			if p.Status == model.StatusPendingImport {
				pendente = true
			}
		}
	}
	if aberta {
		switch {
		case pendente || n.PendingAt != nil:
			return model.StatusPendingImport
		case n.SyncedAt != nil:
			return model.StatusSynced
		case n.ArrivedAt != nil:
			return model.StatusArrived
		default:
			return model.StatusPendingImport
		}
	}
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
