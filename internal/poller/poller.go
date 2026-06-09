// Package poller closes the import span. It is chave-driven (Fase 0): each cycle
// it asks the store for in-flight chaves (arrived/synced), looks them up in the
// Athenas Firebird by indexed CHAVEACESSO (read-only), and emits an observation
// when a nota became imported or import_ignored — never scanning the 23.5M-row
// table. Emitted observations are idempotent (stable dedup_key), so re-running a
// cycle is harmless and imported notas drop out of the in-flight set.
package poller

import (
	"context"
	"time"

	"github.com/EnzzoHosaki/rps-xml-tracker/internal/firebird"
	"github.com/EnzzoHosaki/rps-xml-tracker/internal/model"
	"github.com/EnzzoHosaki/rps-xml-tracker/internal/store"
)

// reader is the read-only Firebird capability the poller needs (interface for tests).
type reader interface {
	Lookup(ctx context.Context, chaves []string) (map[string]firebird.ImportState, error)
}

// clock returns "now"; injectable for deterministic tests.
type clock func() time.Time

// Poller wires the store and the Firebird reader.
type Poller struct {
	st    store.Store
	fb    reader
	now   clock
	batch int
}

func New(st store.Store, fb reader) *Poller {
	return &Poller{st: st, fb: fb, now: time.Now, batch: 1000}
}

// Result reports one cycle's outcome.
type Result struct {
	Checked  int
	Imported int
	Ignored  int
}

// PollOnce runs a single cycle: in-flight chaves -> Firebird -> emit observations.
func (p *Poller) PollOnce(ctx context.Context) (Result, error) {
	var res Result
	chaves, err := p.st.ListInflightChaves(ctx, p.batch)
	if err != nil {
		return res, err
	}
	if len(chaves) == 0 {
		return res, nil
	}
	res.Checked = len(chaves)

	states, err := p.fb.Lookup(ctx, chaves)
	if err != nil {
		return res, err
	}

	now := p.now()
	var obs []model.Observation
	for _, c := range chaves {
		st, ok := states[c]
		if !ok {
			continue // ainda não chegou no Athenas — segue em trânsito
		}
		switch {
		case st.Importado:
			obs = append(obs, importObs(c, model.EventImported, now, nil, st))
			res.Imported++
		case st.ImportIgnorada:
			payload := map[string]any{}
			if st.Motivo != "" {
				payload["motivo"] = st.Motivo
			}
			obs = append(obs, importObs(c, model.EventImportIgnored, now, payload, st))
			res.Ignored++
		}
	}
	if len(obs) > 0 {
		if _, _, err := p.st.AppendObservations(ctx, obs); err != nil {
			return res, err
		}
	}
	return res, nil
}

// Run loops PollOnce every interval until ctx is cancelled.
func (p *Poller) Run(ctx context.Context, interval time.Duration, onResult func(Result, error)) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		res, err := p.PollOnce(ctx)
		if onResult != nil {
			onResult(res, err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func importObs(chave, event string, now time.Time, payload map[string]any, st firebird.ImportState) model.Observation {
	return model.Observation{
		ChaveAcesso: chave,
		Stage:       model.StageImport,
		EventType:   event,
		ObservedAt:  now, // transição detectada agora ~= imported_at (Fase 0)
		IngestedAt:  now,
		Source:      "poller:firebird",
		Payload:     payload,
		// enriquece com os dados da linha do Athenas (código do cliente + partes)
		CodigoEmpresa:    st.CodigoEmpresa,
		CodigoFilial:     st.CodigoFilial,
		CnpjEmitente:     st.CnpjEmitente,
		NomeEmitente:     st.NomeEmitente,
		CnpjDestinatario: st.CnpjDestinatario,
		NomeDestinatario: st.NomeDestinatario,
		DataEmissao:      st.DataEmissao,
		ValorTotal:       st.ValorTotal,
	}
}
