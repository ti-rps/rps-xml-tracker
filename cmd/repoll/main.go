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
	"log"
	"os"

	"github.com/EnzzoHosaki/rps-xml-tracker/internal/firebird"
	"github.com/EnzzoHosaki/rps-xml-tracker/internal/poller"
	"github.com/EnzzoHosaki/rps-xml-tracker/internal/store"
)

func main() {
	ctx := context.Background()

	fbDSN := os.Getenv("TRACKER_FB_DSN")
	if fbDSN == "" {
		log.Fatal("TRACKER_FB_DSN é obrigatório")
	}
	pgDSN := os.Getenv("TRACKER_PG_DSN")
	if pgDSN == "" {
		log.Fatal("TRACKER_PG_DSN é obrigatório")
	}

	rd, err := firebird.NewReader(ctx, fbDSN)
	if err != nil {
		log.Fatalf("firebird: %v", err)
	}
	defer rd.Close()

	pg, err := store.NewPostgres(ctx, pgDSN)
	if err != nil {
		log.Fatalf("postgres: %v", err)
	}
	defer pg.Close()

	log.Println("repoll: re-pollando notas import_ignored com a lógica selectState...")
	res, err := poller.New(pg, rd).RepollImportIgnored(ctx)
	if err != nil {
		log.Fatalf("repoll: %v (parcial: %+v)", err, res)
	}
	log.Printf("repoll concluído: checadas=%d corrigidas=%d (->imported) ainda_ignoradas=%d ainda_pendentes=%d sumidas=%d",
		res.Checked, res.Corrected, res.StillIgnored, res.StillPending, res.NotFound)
	if res.StillPending > 0 {
		log.Printf("ATENÇÃO: %d notas resolvem p/ pendente e NÃO foram corrigidas por append "+
			"(import_ignored > pending_import). Exigem remoção manual da observação import_ignored errada.", res.StillPending)
	}
}
