// Command repoll é uma correção retroativa ONE-SHOT: re-polla as notas atualmente
// import_ignored (terminais, que o poller normal não revisita) com a lógica nova do
// selectState e emite 'imported' para as que hoje resolvem para a empresa dona
// (IMPORTADO=1). Corrige o caso "ignorada por terceiro antes de a dona importar"
// (ex.: CLW mostrada como ROSEMBERG). Roda uma vez e sai. READ-ONLY no Firebird;
// só faz append (idempotente) no Postgres do tracker.
//
// Config (igual ao poller):
//
//	TRACKER_FB_DSN   firebirdsql DSN (Legacy_Auth, wire_crypt disabled)
//	TRACKER_PG_DSN   Postgres DSN do tracker
//
// Uso em prod: docker compose run --rm tracker-poller tracker-repoll
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"time"

	"github.com/EnzzoHosaki/rps-xml-tracker/internal/firebird"
	"github.com/EnzzoHosaki/rps-xml-tracker/internal/poller"
	"github.com/EnzzoHosaki/rps-xml-tracker/internal/store"
)

func main() {
	// --fix-pending: além de corrigir as que viraram imported, REMOVE a observação
	// import_ignored errada (de terceiro) das que hoje resolvem p/ pendente, fazendo-as
	// voltar a aguardando importação. DESTRUTIVO (apaga observações) — opt-in explícito.
	fixPending := flag.Bool("fix-pending", false, "também corrige as presas em ignorada que na verdade estão pendentes (remove a observação errada)")
	// --fix-imported-at: modo separado — corrige retroativamente o fuso do imported_at
	// das notas importadas desde --since, re-lendo o Firebird. DESTRUTIVO (reescreve
	// observed_at). Não faz o repoll de import_ignored (é um modo à parte).
	fixImportedAt := flag.Bool("fix-imported-at", false, "corrige retroativamente o fuso do imported_at das notas importadas desde --since")
	since := flag.String("since", "", "janela da correção do imported_at (YYYY-MM-DD); vazio = últimos 30 dias")
	// --backfill-direction: modo separado — preenche notas.direction (onde NULL) das
	// notas já gravadas, lendo o CNPJ por filial do Athenas (TABFILIAL) e comparando a
	// raiz com o emitente/destinatário. One-off, idempotente.
	backfillDirection := flag.Bool("backfill-direction", false, "preenche notas.direction retroativamente a partir do CNPJ das filiais (TABFILIAL)")
	// --reconcile: auditoria de acurácia do import — cruza chave a chave o que o Athenas
	// tem como importado contra o que o tracker tem, e lista as divergências.
	reconcileMode := flag.Bool("reconcile", false, "audita o import: compara chaves do Athenas vs tracker e lista divergências")
	source := flag.String("source", "chaveacesso", "fonte no Athenas: chaveacesso (TABLISTACHAVEACESSO, corretude do tracker) | entradasaida (TABENTRADASAIDA, o livro fiscal)")
	until := flag.String("until", "", "fim da janela (YYYY-MM-DD, exclusivo); vazio = amanhã")
	empresa := flag.Int("empresa", 0, "filtra por CODIGOEMPRESA (0 = todas)")
	filial := flag.Int("filial", 0, "filtra por CODIGOFILIAL (0 = todas)")
	tipo := flag.String("tipo", "", "só p/ source=entradasaida: E (entrada) | S (saida) | vazio (ambos)")
	fix := flag.Bool("fix", false, "reconcile: além de reportar, emite as observações 'imported' que faltam (autocorreção)")
	limit := flag.Int("limit", 30, "reconcile: máximo de chaves listadas por seção no relatório (0 = todas)")
	// Modos de INVESTIGAÇÃO do shadow-sync (fase 0, READ-ONLY no Firebird, não tocam o
	// Postgres). Descobrem o INSERT mínimo do DownloadXML, o mecanismo da PK, a
	// prevalência do multi-participação e validam a derivação de URL. Ver design/SHADOW-SYNC.md.
	profileInsert := flag.Bool("profile-insert", false, "F0: perfila a TABLISTACHAVEACESSO (fill-rate por coluna, PK/trigger/generator, multi-participação, janela do robô) desde --since")
	watchChave := flag.String("watch-chave", "", "F0: faz polling de UMA chave (44 díg.) e imprime/diffa a linha inteira a cada ciclo até Ctrl-C")
	checkPath := flag.Bool("check-path", false, "F0: valida a derivação de URL (internal/syncpath) contra as URLs reais recentes, segmento a segmento")
	checkPlans := flag.String("check-plans", "", "F1: compara o syncer-plans.jsonl (dry-run do syncer) com o que o DownloadXML gravou de verdade")
	sample := flag.Int("sample", 2000, "profile-insert/check-path: nº máx. de linhas recentes amostradas")
	watchInterval := flag.Int("watch-interval", 20, "watch-chave: intervalo de polling em segundos")
	flag.Parse()

	ctx := context.Background()

	fbDSN := os.Getenv("TRACKER_FB_DSN")
	if fbDSN == "" {
		log.Fatal("TRACKER_FB_DSN é obrigatório")
	}

	rd, err := firebird.NewReader(ctx, fbDSN)
	if err != nil {
		log.Fatalf("firebird: %v", err)
	}
	defer rd.Close()

	// Modos de investigação (fase 0): só Firebird read-only, sem Postgres. Saem antes
	// de exigir/abrir o TRACKER_PG_DSN.
	switch {
	case *profileInsert:
		runProfileInsert(ctx, rd, *since, *sample)
		return
	case *watchChave != "":
		runWatchChave(ctx, rd, *watchChave, *watchInterval)
		return
	case *checkPath:
		runCheckPath(ctx, rd, *since, *sample)
		return
	case *checkPlans != "":
		runCheckPlans(ctx, rd, *checkPlans, *limit)
		return
	}

	pgDSN := os.Getenv("TRACKER_PG_DSN")
	if pgDSN == "" {
		log.Fatal("TRACKER_PG_DSN é obrigatório")
	}
	pg, err := store.NewPostgres(ctx, pgDSN)
	if err != nil {
		log.Fatalf("postgres: %v", err)
	}
	defer pg.Close()

	if *reconcileMode {
		runReconcile(ctx, rd, pg, reconcileOpts{
			source: *source, since: *since, until: *until,
			empresa: optInt(*empresa), filial: optInt(*filial), tipo: *tipo,
			fix: *fix, limit: *limit,
		})
		return
	}

	if *fixImportedAt {
		from := time.Now().AddDate(0, 0, -30)
		if *since != "" {
			t, err := time.ParseInLocation("2006-01-02", *since, time.Local)
			if err != nil {
				log.Fatalf("--since inválido (use YYYY-MM-DD): %v", err)
			}
			from = t
		}
		log.Printf("repoll: corrigindo o fuso do imported_at das notas importadas desde %s...", from.Format("2006-01-02"))
		res, err := poller.New(pg, rd).FixImportedAt(ctx, from)
		if err != nil {
			log.Fatalf("fix-imported-at: %v (parcial: %+v)", err, res)
		}
		log.Printf("fix-imported-at concluído: checadas=%d corrigidas=%d já_ok=%d sem_data_firebird=%d sumidas=%d",
			res.Checked, res.Corrected, res.AlreadyOK, res.NoFirebird, res.NotFound)
		return
	}

	if *backfillDirection {
		log.Printf("repoll: backfill da direção — lendo CNPJ das filiais (TABFILIAL)...")
		filiais, err := rd.ListFiliais(ctx)
		if err != nil {
			log.Fatalf("backfill-direction: listar filiais: %v", err)
		}
		fc := make([]store.FilialCNPJ, len(filiais))
		for i, f := range filiais {
			fc[i] = store.FilialCNPJ{CodigoEmpresa: f.CodigoEmpresa, CodigoFilial: f.CodigoFilial, Cnpj: f.Cnpj}
		}
		log.Printf("backfill da direção: %d filiais carregadas — varrendo a notas por faixas de página (ctid)...", len(fc))
		n, err := pg.BackfillDirection(ctx, fc, func(done, total int, affected int64) {
			log.Printf("  progresso: %d/%d páginas, %d notas classificadas até aqui", done, total, affected)
		})
		if err != nil {
			log.Fatalf("backfill-direction: %v (parcial: %d notas já classificadas)", err, n)
		}
		log.Printf("backfill-direction concluído: %d filiais lidas, %d notas classificadas (entrada/saida)", len(fc), n)
		return
	}

	mode := "padrão (append-only)"
	if *fixPending {
		mode = "--fix-pending (remove import_ignored errada das pendentes)"
	}
	log.Printf("repoll: re-pollando notas import_ignored com a lógica selectState [%s]...", mode)
	res, err := poller.New(pg, rd).RepollImportIgnored(ctx, *fixPending)
	if err != nil {
		log.Fatalf("repoll: %v (parcial: %+v)", err, res)
	}
	log.Printf("repoll concluído: checadas=%d corrigidas=%d (->imported) corrigidas_pendentes=%d (->pending_import) ainda_ignoradas=%d ainda_pendentes=%d sumidas=%d",
		res.Checked, res.Corrected, res.FixedPending, res.StillIgnored, res.StillPending, res.NotFound)
	if res.StillPending > 0 {
		log.Printf("ATENÇÃO: %d notas resolvem p/ pendente e NÃO foram corrigidas (rode com --fix-pending "+
			"para removê-las da ignorada e devolvê-las a aguardando importação).", res.StillPending)
	}
}
