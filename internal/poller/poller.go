// Package poller closes the import span. It is chave-driven (Fase 0): each cycle
// it asks the store for in-flight chaves (arrived/synced), looks them up in the
// Athenas Firebird by indexed CHAVEACESSO (read-only), and emits an observation
// when a nota became imported or import_ignored — never scanning the 23.5M-row
// table. Emitted observations are idempotent (stable dedup_key), so re-running a
// cycle is harmless and imported notas drop out of the in-flight set.
package poller

import (
	"context"
	"sort"
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
	// TerminalChavesSince retorna as chaves com linha terminal (IMPORTADO=1 ou
	// IGNORADA=1) e DATAINCLUSAO >= o dia de since (piso na meia-noite — DATAINCLUSAO
	// é date-only). É O(recentes) e não depende do tamanho do backlog in-flight —
	// complementa a rotação do Lookup.
	TerminalChavesSince(ctx context.Context, since time.Time) ([]string, error)
	// ImportedSince retorna as chaves IMPORTADO=1 com DATAINCLUSAO na janela
	// [since, until), opcionalmente por empresa/filial. É o lado Athenas do reconcile.
	ImportedSince(ctx context.Context, since, until time.Time, codigoEmpresa, codigoFilial *int) (map[string]firebird.ImportState, error)
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

// SetBatch ajusta quantas chaves in-flight são checadas por ciclo. Lotes maiores
// drenam um backlog grande de in-flight bem mais rápido (o Lookup chunka em 400 p/
// o Firebird, então é seguro). Ignora valores <=0.
func (p *Poller) SetBatch(n int) {
	if n > 0 {
		p.batch = n
	}
}

// Result reports one poll cycle's outcome.
type Result struct {
	Checked  int
	Imported int
	Ignored  int
	Pending  int
}

// SweepResult reports one sweep cycle's outcome.
type SweepResult struct {
	Found    int // chaves com linha terminal (imported/ignorada) no(s) dia(s) da janela
	Imported int // observações 'imported' montadas neste ciclo (só chaves novas)
	Ignored  int // observações 'import_ignored' montadas (após resolução com Lookup)
	Pending  int // candidatas que resolveram p/ pendente (dona ainda vai importar)
	Emitted  int // observações accepted pelo store (novas)
	Skipped  int // já conhecidas: puladas pelo pré-filtro de status + rejeitadas por dedup
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
		// M0: uma observação POR PARTICIPAÇÃO (empresa/filial), não mais só a
		// representante — o derive agrega e a nota só termina quando TODAS
		// terminam. Contadores seguem por NOTA (estado representante), como antes.
		obs = append(obs, obsPorParticipacao(c, now, st)...)
		switch {
		case st.Importado:
			res.Imported++
		case st.ImportIgnorada:
			res.Ignored++
		default:
			// achada no Athenas mas IMPORTADO=0 e não ignorada -> aguardando
			// importação. Sinal não-terminal: dedup colapsa as reemissões a
			// cada ciclo, então pending_at fica no 1º avistamento.
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
				obs = append(obs, obsPorParticipacao(c, now, st)...)
				res.Corrected++
			case st.ImportIgnorada:
				res.StillIgnored++
			case fixPending:
				// pendente de fato (dona ainda não importou): a import_ignored era de
				// terceiro. Remove a observação errada e emite as participações —
				// MENOS as ignoradas, senão recriaríamos na hora o que acabamos de
				// apagar (a participação ignorada do terceiro volta go-forward, no
				// próximo poll normal, já no modelo agregado que não termina a nota).
				if _, err := p.st.DeleteImportIgnoredObs(ctx, c); err != nil {
					return res, err
				}
				for _, o := range obsPorParticipacao(c, now, st) {
					if o.EventType != model.EventImportIgnored {
						obs = append(obs, o)
					}
				}
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

// FixImportedResult reporta a correção retroativa do imported_at (fuso).
type FixImportedResult struct {
	Checked    int // chaves imported na janela
	Corrected  int // observed_at do imported reescrito (fuso/data corrigidos)
	AlreadyOK  int // já estava com o valor certo (idempotente)
	NoFirebird int // sem DATAROBO/DATAINCLUSAO no Firebird -> mantém a detecção (now)
	NotFound   int // sumiu do Firebird
}

// FixImportedAt re-lê o Firebird das notas imported desde `since` e reescreve o
// observed_at do evento 'imported' para o valor de DATAROBO/DATAINCLUSAO (já com o
// fuso certo — o reader aplica fbLocalTime). Notas sem data no Firebird são puladas
// (mantêm a hora de detecção). É a correção retroativa do bug de fuso do imported_at:
// o poller normal não revisita notas terminais e o dedup bloquearia uma reemissão.
// DESTRUTIVO: reescreve observed_at. One-off (cmd/repoll --fix-imported-at).
func (p *Poller) FixImportedAt(ctx context.Context, since time.Time) (FixImportedResult, error) {
	var res FixImportedResult
	chaves, err := p.st.ListChavesImportedSince(ctx, since)
	if err != nil {
		return res, err
	}
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
		for _, c := range batch {
			res.Checked++
			st, ok := states[c]
			switch {
			case !ok:
				res.NotFound++
			case st.DataRobo == nil && st.DataInclusao == nil:
				res.NoFirebird++
			default:
				// mesma precedência da cascata do importObs: DATAROBO antes de DATAINCLUSAO.
				at := st.DataInclusao
				if st.DataRobo != nil {
					at = st.DataRobo
				}
				changed, err := p.st.UpdateImportedObservedAt(ctx, c, *at)
				if err != nil {
					return res, err
				}
				if changed {
					res.Corrected++
				} else {
					res.AlreadyOK++
				}
			}
		}
	}
	return res, nil
}

// SweepOnce pergunta ao Firebird quais notas inseridas no(s) dia(s) da janela já têm
// linha TERMINAL (IMPORTADO=1 ou IGNORADA=1, por DATAINCLUSAO — date-only, então o
// recorte tem piso na meia-noite) e emite as observações das que o tracker ainda não
// conhece. É O(inseridas_recentes) — não enumera o backlog in-flight.
//
// Duas fases: (1) TerminalChavesSince traz SÓ as chaves do dia (leve — como o recorte
// revê o dia inteiro a cada ciclo, buscar as linhas completas seria dezenas de MB a
// cada 5min); as que o tracker já tem terminais são puladas por status (Skipped).
// (2) As NOVAS são resolvidas com Lookup completo (todas as linhas por empresa,
// selectState) — nunca pelo recorte terminal do sweep, que pode esconder a linha
// PENDENTE da dona e tornar a nota terminal cedo demais (o bug histórico
// CLW/ROSEMBERG): imported/ignorada/pendente saem da resolução, a mesma do ciclo
// rotacional. Dedup segura reemissões residuais (idempotente).
func (p *Poller) SweepOnce(ctx context.Context, since time.Time) (SweepResult, error) {
	var res SweepResult
	terminal, err := p.fb.TerminalChavesSince(ctx, since)
	if err != nil {
		return res, err
	}
	res.Found = len(terminal)
	if res.Found == 0 {
		return res, nil
	}
	statuses, err := p.st.StatusForChaves(ctx, terminal)
	if err != nil {
		return res, err
	}
	var fresh []string
	for _, c := range terminal {
		switch statuses[c] {
		case model.StatusImported, model.StatusImportIgnored:
			res.Skipped++ // o tracker já sabe o desfecho — nada a fazer
		default:
			fresh = append(fresh, c)
		}
	}
	if len(fresh) == 0 {
		return res, nil
	}
	full, err := p.fb.Lookup(ctx, fresh) // visão completa (todas as linhas por empresa)
	if err != nil {
		return res, err
	}
	now := p.now()
	var obs []model.Observation
	for _, c := range fresh {
		st, ok := full[c]
		if !ok {
			continue // sumiu entre o sweep e o lookup — a rotação recaptura
		}
		// M0: emite POR PARTICIPAÇÃO; contadores por nota (estado representante).
		obs = append(obs, obsPorParticipacao(c, now, st)...)
		switch {
		case st.Importado:
			res.Imported++
		case st.ImportIgnorada:
			res.Ignored++
		default:
			// há linha pendente (a dona ainda vai importar) -> NÃO é terminal;
			// registra o avistamento e segue em rotação.
			res.Pending++
		}
	}
	if len(obs) == 0 {
		return res, nil
	}
	accepted, rejected, err := p.st.AppendObservations(ctx, obs)
	if err != nil {
		return res, err
	}
	res.Emitted = accepted
	res.Skipped += rejected
	return res, nil
}

// ReconcileResult reporta um ciclo do reconcile contínuo (acurácia do import como métrica).
type ReconcileResult struct {
	Since, Until  time.Time
	Athena        int      // chaves IMPORTADO=1 no Athenas na janela (por DATAINCLUSAO)
	Tracker       int      // dessas, quantas o tracker já conhece como imported
	Missing       int      // Athenas importou e o tracker não sabia (Athena - Tracker)
	MissingSample []string // até 5 chaves faltantes, p/ diagnóstico no heartbeat
	Fixed         int      // observações 'imported' novas gravadas pelo self-heal (fix=true)
}

// ReconcileOnce mede a acurácia do import: das chaves que o Athenas importou na janela
// deslizante [now-grace-window, now-grace) (TABLISTACHAVEACESSO, IMPORTADO=1 por
// DATAINCLUSAO), quantas o tracker conhece como imported (StatusForChaves). O grace
// desconta o atraso natural de detecção (sweep/rotação): sem ele, toda importação dos
// últimos minutos contaria como "faltando".
//
// A janela existe SÓ do lado do Athenas. Não dá para recortar o tracker por
// imported_at na mesma janela: o imported_at vem do DATAROBO/DATAINCLUSAO, que neste
// Athenas têm granularidade de DATA (hora sempre 00:00) — uma janela rolante de
// relógio nunca casa com valores date-only (medido em prod: tracker=0 com 30k
// importadas no dia). Perguntar o STATUS da chave é imune a granularidade e skew.
//
// Com fix=true, as faltantes passam pelo EmitImportedFor (self-heal): o Athenas é
// re-consultado via Lookup e só o que ele confirmar IMPORTADO=1 vira observação —
// idempotente e seguro por construção.
func (p *Poller) ReconcileOnce(ctx context.Context, window, grace time.Duration, fix bool) (ReconcileResult, error) {
	var res ReconcileResult
	res.Until = p.now().Add(-grace)
	res.Since = res.Until.Add(-window)

	athena, err := p.fb.ImportedSince(ctx, res.Since, res.Until, nil, nil)
	if err != nil {
		return res, err
	}
	res.Athena = len(athena)
	if res.Athena == 0 {
		return res, nil
	}
	chaves := make([]string, 0, len(athena))
	for c := range athena {
		chaves = append(chaves, c)
	}
	sort.Strings(chaves) // amostra de faltantes determinística entre ciclos

	// KnownImported, não StatusForChaves: no modelo M0 uma nota "importada 1/2"
	// (participação de outra empresa ainda pendente) tem status pending_import mas
	// JÁ registrou a importação — pelo status ela contaria como faltante eterna.
	known, err := p.st.KnownImported(ctx, chaves)
	if err != nil {
		return res, err
	}
	var missing []string
	for _, c := range chaves {
		if !known[c] {
			missing = append(missing, c)
		}
	}
	res.Missing = len(missing)
	res.Tracker = res.Athena - res.Missing
	if n := len(missing); n > 0 {
		if n > 5 {
			n = 5
		}
		res.MissingSample = missing[:n]
	}

	if fix && len(missing) > 0 {
		acc, _, err := p.EmitImportedFor(ctx, missing)
		if err != nil {
			return res, err
		}
		res.Fixed = acc
	}
	return res, nil
}

// RunReconcile roda o reconcile contínuo a cada interval até o ctx cancelar (primeiro
// ciclo imediato). Bloqueia — rode em goroutine própria ao lado do RunWithSweep.
func (p *Poller) RunReconcile(
	ctx context.Context,
	interval, window, grace time.Duration,
	fix bool,
	onResult func(ReconcileResult, error),
) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		res, err := p.ReconcileOnce(ctx, window, grace, fix)
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

// RunWithSweep é como Run mas também dispara um sweep ticker independente a cada
// sweepInterval, varrendo o Firebird por DATAINCLUSAO > (now-sweepWindow). O sweep
// captura importações recentes em O(recentes) sem depender da rotação do backlog.
// O sweepInterval=0 desabilita o sweep (equivale a Run).
func (p *Poller) RunWithSweep(
	ctx context.Context,
	interval, sweepInterval, sweepWindow time.Duration,
	onResult func(Result, error),
	onSweep func(SweepResult, error),
) {
	if sweepInterval > 0 {
		go func() {
			t := time.NewTicker(sweepInterval)
			defer t.Stop()
			for {
				since := p.now().Add(-sweepWindow)
				sr, se := p.SweepOnce(ctx, since)
				if onSweep != nil {
					onSweep(sr, se)
				}
				select {
				case <-ctx.Done():
					return
				case <-t.C:
				}
			}
		}()
	}
	p.Run(ctx, interval, onResult)
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

// EmitImportedFor re-consulta as chaves dadas no Athenas e emite observação 'imported'
// para as que estão IMPORTADO=1 (idempotente via dedup). É o motor do `reconcile --fix`:
// dado o conjunto que o Athenas importou mas o tracker não sabia, o tracker se autocorrige.
// Retorna quantas observações foram aceitas (novas) e quantas chaves o Athenas confirmou.
func (p *Poller) EmitImportedFor(ctx context.Context, chaves []string) (accepted, confirmed int, err error) {
	if len(chaves) == 0 {
		return 0, 0, nil
	}
	states, err := p.fb.Lookup(ctx, chaves)
	if err != nil {
		return 0, 0, err
	}
	now := p.now()
	var obs []model.Observation
	for _, c := range chaves {
		st, ok := states[c]
		if !ok || !st.Importado {
			continue // o Athenas não confirma imported -> não força
		}
		confirmed++
		// emite todas as participações (a importada + as ainda pendentes/ignoradas):
		// registra a verdade completa, não só a correção pontual.
		obs = append(obs, obsPorParticipacao(c, now, st)...)
	}
	if len(obs) == 0 {
		return 0, confirmed, nil
	}
	acc, _, err := p.st.AppendObservations(ctx, obs)
	return acc, confirmed, err
}

// obsPorParticipacao monta as observações de UM lookup: uma POR PARTICIPAÇÃO
// (linha empresa/filial do Athenas), cada qual com o evento do SEU estado —
// imported, import_ignored (payload motivo) ou seen_pending (M0). O dedup_key
// carrega a empresa (store.DedupKey), então as participações não colidem entre
// si nem entre ciclos. Linha órfã sem CODIGOEMPRESA (não ancora participação):
// cai numa única observação com o estado representante (selectState), como antes.
func obsPorParticipacao(chave string, now time.Time, st firebird.ImportState) []model.Observation {
	parts := st.Participacoes()
	if len(parts) == 0 {
		return []model.Observation{obsForRow(chave, now, representanteRow(st), st)}
	}
	out := make([]model.Observation, 0, len(parts))
	for _, p := range parts {
		out = append(out, obsForRow(chave, now, p, st))
	}
	return out
}

// representanteRow reconstrói uma EmpresaImport com o estado/metadados do
// representante (selectState) — só usado no fallback sem participações.
func representanteRow(st firebird.ImportState) firebird.EmpresaImport {
	return firebird.EmpresaImport{
		CodigoEmpresa:  st.CodigoEmpresa,
		CodigoFilial:   st.CodigoFilial,
		NomeEmpresa:    st.NomeEmpresa,
		Importado:      st.Importado,
		ImportIgnorada: st.ImportIgnorada,
		Motivo:         st.Motivo,
		CnpjFilial:     st.CnpjFilial,
		DataRobo:       st.DataRobo,
		DataInclusao:   st.DataInclusao,
	}
}

// obsForRow monta a observação de UMA participação (linha empresa/filial).
// Metadados da NOTA (partes, emissão, valor) vêm do estado resolvido st (que os
// backfilla entre as linhas irmãs); os da PARTICIPAÇÃO (empresa, direção, datas
// de importação) vêm da própria linha.
func obsForRow(chave string, now time.Time, row firebird.EmpresaImport, st firebird.ImportState) model.Observation {
	event := model.EventSeenPending
	var payload map[string]any
	switch {
	case row.Importado:
		event = model.EventImported
	case row.ImportIgnorada:
		event = model.EventImportIgnored
		payload = map[string]any{}
		if row.Motivo != "" {
			payload["motivo"] = toUTF8(row.Motivo)
		}
	}
	// ObservedAt = hora real da importação no Athenas, com fallback em cascata:
	//   1. DATAROBO  — mais preciso (hora exata do robô em lote); NULL em importações manuais
	//   2. DATAINCLUSAO — sempre preenchido (quando a linha entrou no Athenas); proxy razoável
	//   3. now()     — hora de detecção pelo poller (fallback seguro)
	observedAt := now
	if event == model.EventImported {
		if row.DataRobo != nil {
			observedAt = *row.DataRobo
		} else if row.DataInclusao != nil {
			observedAt = *row.DataInclusao
		}
	}
	return model.Observation{
		ChaveAcesso: chave,
		Stage:       model.StageImport,
		EventType:   event,
		ObservedAt:  observedAt,
		IngestedAt:  now,
		Source:      "poller:firebird",
		Payload:     payload,
		// enriquece com os dados da linha do Athenas (código do cliente + partes).
		// strings sanitizadas: o Firebird (charset=NONE) devolve Latin-1, inválido em UTF-8.
		CodigoEmpresa:    row.CodigoEmpresa,
		CodigoFilial:     row.CodigoFilial,
		NomeEmpresa:      toUTF8(row.NomeEmpresa),
		CnpjEmitente:     toUTF8(st.CnpjEmitente),
		NomeEmitente:     toUTF8(st.NomeEmitente),
		CnpjDestinatario: toUTF8(st.CnpjDestinatario),
		NomeDestinatario: toUTF8(st.NomeDestinatario),
		DataEmissao:      st.DataEmissao,
		ValorTotal:       st.ValorTotal,
		// direção = lado DESTA empresa: raiz do CNPJ da filial da linha vs emitente/
		// destinatário. CNPJ é numérico (sem acento), não precisa transcodificar.
		Direction: model.DirectionFromCNPJs(row.CnpjFilial, st.CnpjEmitente, st.CnpjDestinatario),
	}
}
