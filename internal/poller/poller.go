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
	"unicode/utf8"

	"github.com/EnzzoHosaki/rps-xml-tracker/internal/firebird"
	"github.com/EnzzoHosaki/rps-xml-tracker/internal/model"
	"github.com/EnzzoHosaki/rps-xml-tracker/internal/store"
)

// toUTF8 garante UTF-8 válido. O Firebird do Athenas conecta com charset=NONE e
// devolve texto em Latin-1 (bytes 0x80-0xFF crus, ex.: 0xC1='Á'), que o Postgres
// (UTF-8) rejeita na inserção ("invalid byte sequence", SQLSTATE 22021) e derruba
// o lote inteiro do ciclo. Se a string já é UTF-8 válida, devolve como está; senão
// decodifica como Latin-1 (cada byte -> rune), cobrindo os acentos sem dependência.
func toUTF8(s string) string {
	if utf8.ValidString(s) {
		return s
	}
	r := make([]rune, 0, len(s))
	for i := 0; i < len(s); i++ {
		r = append(r, rune(s[i]))
	}
	return string(r)
}

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
	Pending  int
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
				payload["motivo"] = toUTF8(st.Motivo)
			}
			obs = append(obs, importObs(c, model.EventImportIgnored, now, payload, st))
			res.Ignored++
		default:
			// achada no Athenas mas IMPORTADO=0 e não ignorada -> aguardando
			// importação. Sinal não-terminal: dedup colapsa as reemissões a
			// cada ciclo, então pending_at fica no 1º avistamento.
			obs = append(obs, importObs(c, model.EventSeenPending, now, nil, st))
			res.Pending++
		}
	}
	if len(obs) > 0 {
		if _, _, err := p.st.AppendObservations(ctx, obs); err != nil {
			return res, err
		}
	}
	return res, nil
}

// RepollResult reports the one-off retroactive correction.
type RepollResult struct {
	Checked      int
	Corrected    int // resolveu p/ imported (dona) -> emitiu 'imported' corretivo
	StillIgnored int // segue ignorada de fato (nada a fazer)
	StillPending int // resolve p/ pendente e fixPending=false -> não tocada
	FixedPending int // resolve p/ pendente e fixPending=true -> import_ignored removida + seen_pending
	NotFound     int // sumiu do Athenas
}

// RepollImportIgnored re-polls notas atualmente import_ignored (terminais, fora do
// conjunto in-flight) com a lógica nova do selectState. Correção retroativa one-off:
//
//   - resolve p/ a dona (IMPORTADO=1) -> emite 'imported' (dedup_key distinto do
//     'import_ignored', então é aceito; o derive faz imported vencer).
//   - resolve p/ pendente: se fixPending=true, REMOVE a observação import_ignored
//     errada (destrutivo) e emite seen_pending -> a nota vira pending_import; se
//     fixPending=false, só conta em StillPending (append não corrige porque
//     import_ignored > pending_import na precedência).
//   - tudo ignorado de fato -> no-op.
func (p *Poller) RepollImportIgnored(ctx context.Context, fixPending bool) (RepollResult, error) {
	var res RepollResult
	chaves, err := p.st.ListChavesByStatus(ctx, model.StatusImportIgnored, 0, 0)
	if err != nil {
		return res, err
	}
	now := p.now()
	for start := 0; start < len(chaves); start += p.batch {
		end := start + p.batch
		if end > len(chaves) {
			end = len(chaves)
		}
		batch := chaves[start:end]
		states, err := p.fb.Lookup(ctx, batch)
		if err != nil {
			return res, err
		}
		var obs []model.Observation
		for _, c := range batch {
			res.Checked++
			st, ok := states[c]
			switch {
			case !ok:
				res.NotFound++
			case st.Importado:
				obs = append(obs, importObs(c, model.EventImported, now, nil, st))
				res.Corrected++
			case st.ImportIgnorada:
				res.StillIgnored++
			case fixPending:
				// pendente de fato (dona ainda não importou): a import_ignored era de
				// terceiro. Remove a observação errada e emite seen_pending.
				if _, err := p.st.DeleteImportIgnoredObs(ctx, c); err != nil {
					return res, err
				}
				obs = append(obs, importObs(c, model.EventSeenPending, now, nil, st))
				res.FixedPending++
			default:
				res.StillPending++
			}
		}
		if len(obs) > 0 {
			if _, _, err := p.st.AppendObservations(ctx, obs); err != nil {
				return res, err
			}
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
		// enriquece com os dados da linha do Athenas (código do cliente + partes).
		// strings sanitizadas: o Firebird (charset=NONE) devolve Latin-1, inválido em UTF-8.
		CodigoEmpresa:    st.CodigoEmpresa,
		CodigoFilial:     st.CodigoFilial,
		NomeEmpresa:      toUTF8(st.NomeEmpresa),
		CnpjEmitente:     toUTF8(st.CnpjEmitente),
		NomeEmitente:     toUTF8(st.NomeEmitente),
		CnpjDestinatario: toUTF8(st.CnpjDestinatario),
		NomeDestinatario: toUTF8(st.NomeDestinatario),
		DataEmissao:      st.DataEmissao,
		ValorTotal:       st.ValorTotal,
	}
}
