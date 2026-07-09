package main

import (
	"context"
	"log"
	"sort"
	"strconv"
	"time"

	"github.com/EnzzoHosaki/rps-xml-tracker/internal/firebird"
	"github.com/EnzzoHosaki/rps-xml-tracker/internal/model"
	"github.com/EnzzoHosaki/rps-xml-tracker/internal/poller"
	"github.com/EnzzoHosaki/rps-xml-tracker/internal/reconcile"
	"github.com/EnzzoHosaki/rps-xml-tracker/internal/store"
)

type reconcileOpts struct {
	source, since, until, tipo string
	empresa, filial            *int
	fix                        bool
	limit                      int
}

// optInt converte o valor de uma flag int em *int, tratando 0 (não informado) como nil.
func optInt(v int) *int {
	if v <= 0 {
		return nil
	}
	return &v
}

func mustDate(s, nome string) time.Time {
	t, err := time.ParseInLocation("2006-01-02", s, time.Local)
	if err != nil {
		log.Fatalf("--%s inválido (use YYYY-MM-DD): %v", nome, err)
	}
	return t
}

func runReconcile(ctx context.Context, rd *firebird.Reader, pg *store.Postgres, o reconcileOpts) {
	if o.since == "" {
		log.Fatal("--reconcile exige --since YYYY-MM-DD (início da janela)")
	}
	from := mustDate(o.since, "since")
	var until time.Time
	if o.until != "" {
		until = mustDate(o.until, "until")
	} else {
		now := time.Now().In(time.Local)
		until = time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.Local).AddDate(0, 0, 1) // amanhã 00:00
	}
	log.Printf("reconcile [source=%s] janela [%s, %s) empresa=%v filial=%v tipo=%q fix=%v",
		o.source, from.Format("2006-01-02"), until.Format("2006-01-02"), deref(o.empresa), deref(o.filial), o.tipo, o.fix)

	switch o.source {
	case "chaveacesso":
		reconcileChaveAcesso(ctx, rd, pg, from, until, o)
	case "entradasaida":
		reconcileEntradaSaida(ctx, rd, pg, from, until, o)
	default:
		log.Fatalf("--source inválido: %q (use chaveacesso | entradasaida)", o.source)
	}
}

// reconcileChaveAcesso: tracker × TABLISTACHAVEACESSO (IMPORTADO=1, por DATAINCLUSAO).
// Auditoria de corretude — divergência aqui é bug do tracker.
func reconcileChaveAcesso(ctx context.Context, rd *firebird.Reader, pg *store.Postgres, from, until time.Time, o reconcileOpts) {
	athena, err := rd.ImportedSince(ctx, from, until, o.empresa, o.filial)
	if err != nil {
		log.Fatalf("reconcile: ler Athenas (TABLISTACHAVEACESSO): %v", err)
	}
	athenaChaves := make([]string, 0, len(athena))
	for c := range athena {
		athenaChaves = append(athenaChaves, c)
	}
	trackerImported, err := pg.ImportedChavesBetween(ctx, from, until, o.empresa, o.filial)
	if err != nil {
		log.Fatalf("reconcile: ler tracker: %v", err)
	}
	missing, extra := reconcile.Diff(athenaChaves, trackerImported)
	statuses, err := pg.StatusForChaves(ctx, missing)
	if err != nil {
		log.Fatalf("reconcile: status das faltantes: %v", err)
	}

	log.Printf("RESUMO  Athenas(IMPORTADO=1)=%d  Tracker(imported)=%d  batendo=%d  faltando_no_tracker=%d  sobrando_no_tracker=%d",
		len(athenaChaves), len(trackerImported), len(athenaChaves)-len(missing), len(missing), len(extra))

	log.Printf("FALTANDO NO TRACKER (Athenas importou, tracker não sabe): %d", len(missing))
	for _, c := range limitList(missing, o.limit) {
		st := athena[c]
		emp := "?"
		if st.CodigoEmpresa != nil {
			emp = strconv.Itoa(*st.CodigoEmpresa)
		}
		cur := "nunca vista"
		if s, ok := statuses[c]; ok {
			cur = string(s)
		}
		log.Printf("  %s  nº %-9s emp %-6s %-28.28s  estado no tracker: %s", c, model.NumeroNota(c), emp, st.NomeEmpresa, cur)
	}
	if len(missing) > o.limit && o.limit > 0 {
		log.Printf("  ... e mais %d (use -limit 0 p/ listar todas)", len(missing)-o.limit)
	}
	if len(extra) > 0 {
		log.Printf("SOBRANDO NO TRACKER (tracker diz imported, Athenas não tem na janela): %d", len(extra))
		for _, c := range limitList(extra, o.limit) {
			log.Printf("  %s  nº %s", c, model.NumeroNota(c))
		}
	}
	applyFix(ctx, rd, pg, missing, o.fix)
}

// reconcileEntradaSaida: tracker × TABENTRADASAIDA (EFETIVADA=1, por DATAREGISTRO). É o
// "livro fiscal" do painel. Diferenças de lançamentos SEM XML são esperadas (não são
// falha do tracker) e reportadas à parte.
func reconcileEntradaSaida(ctx context.Context, rd *firebird.Reader, pg *store.Postgres, from, until time.Time, o reconcileOpts) {
	movs, err := rd.MovimentosByRegistro(ctx, from, until, o.empresa, o.filial, o.tipo)
	if err != nil {
		log.Fatalf("reconcile: ler Athenas (TABENTRADASAIDA): %v", err)
	}
	comChaveSet := map[string]struct{}{}
	manuais := 0
	for _, m := range movs {
		if m.Chave == "" {
			manuais++
			continue
		}
		comChaveSet[m.Chave] = struct{}{}
	}
	comChave := make([]string, 0, len(comChaveSet))
	for c := range comChaveSet {
		comChave = append(comChave, c)
	}
	statuses, err := pg.StatusForChaves(ctx, comChave)
	if err != nil {
		log.Fatalf("reconcile: status no tracker: %v", err)
	}
	// faltante = sem NENHUMA importação registrada (imported_at). O status agregado
	// não serve de teste: "importada 1/2" (M0) fica pending_import mas já importou.
	known, err := pg.KnownImported(ctx, comChave)
	if err != nil {
		log.Fatalf("reconcile: known-imported no tracker: %v", err)
	}
	var missing []string
	for _, c := range comChave {
		if !known[c] {
			missing = append(missing, c)
		}
	}
	sort.Strings(missing)

	log.Printf("RESUMO  lançamentos no livro=%d  com_XML(rastreáveis)=%d  sem_XML(digitado/TXT/Excel — fora do alcance do tracker)=%d",
		len(movs), len(comChave), manuais)
	log.Printf("COM XML porém NÃO importadas no tracker: %d", len(missing))
	for _, c := range limitList(missing, o.limit) {
		cur := "nunca vista"
		if s, ok := statuses[c]; ok {
			cur = string(s)
		}
		log.Printf("  %s  nº %-9s  estado no tracker: %s", c, model.NumeroNota(c), cur)
	}
	if len(missing) > o.limit && o.limit > 0 {
		log.Printf("  ... e mais %d (use -limit 0 p/ listar todas)", len(missing)-o.limit)
	}
	applyFix(ctx, rd, pg, missing, o.fix)
}

// applyFix emite 'imported' para as chaves faltantes que o Athenas confirmar (só com -fix).
func applyFix(ctx context.Context, rd *firebird.Reader, pg *store.Postgres, missing []string, fix bool) {
	if !fix {
		if len(missing) > 0 {
			log.Printf("(rode de novo com -fix para o tracker se autocorrigir emitindo as 'imported' faltantes)")
		}
		return
	}
	if len(missing) == 0 {
		return
	}
	log.Printf("-fix: reemitindo 'imported' para as %d faltantes...", len(missing))
	acc, confirmed, err := poller.New(pg, rd).EmitImportedFor(ctx, missing)
	if err != nil {
		log.Fatalf("-fix: %v", err)
	}
	log.Printf("-fix concluído: %d confirmadas pelo Athenas, %d observações novas gravadas (o resto já existia)", confirmed, acc)
}

func limitList(s []string, limit int) []string {
	if limit <= 0 || len(s) <= limit {
		return s
	}
	return s[:limit]
}

func deref(p *int) any {
	if p == nil {
		return "todas"
	}
	return *p
}
