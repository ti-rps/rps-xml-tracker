// Command poller closes the import span: it polls the Athenas Firebird
// (read-only, chave-driven) for in-flight notas and emits import observations.
//
// In production it shares the tracker's Postgres with the API. Config:
//
//	TRACKER_FB_DSN   firebirdsql DSN (Legacy_Auth, wire_crypt disabled)
//	TRACKER_STORE    postgres (default) | memory
//	TRACKER_PG_DSN   Postgres DSN (when TRACKER_STORE=postgres)
//	TRACKER_POLL_INTERVAL  e.g. 30s (default)
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/EnzzoHosaki/rps-xml-tracker/internal/firebird"
	"github.com/EnzzoHosaki/rps-xml-tracker/internal/poller"
	"github.com/EnzzoHosaki/rps-xml-tracker/internal/store"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	fbDSN := os.Getenv("TRACKER_FB_DSN")
	if fbDSN == "" {
		log.Fatal("TRACKER_FB_DSN é obrigatório")
	}
	rd, err := firebird.NewReader(ctx, fbDSN)
	if err != nil {
		log.Fatalf("firebird: %v", err)
	}
	defer rd.Close()

	var st store.Store
	switch getenv("TRACKER_STORE", "postgres") {
	case "memory":
		st = store.NewMemory()
		log.Println("store: in-memory (dev) — observações não compartilhadas com a API")
	case "postgres":
		dsn := os.Getenv("TRACKER_PG_DSN")
		if dsn == "" {
			log.Fatal("TRACKER_PG_DSN é obrigatório com TRACKER_STORE=postgres")
		}
		pg, err := store.NewPostgres(ctx, dsn)
		if err != nil {
			log.Fatalf("postgres: %v", err)
		}
		defer pg.Close()
		st = pg
	default:
		log.Fatal("TRACKER_STORE inválido")
	}

	interval := 30 * time.Second
	if v := os.Getenv("TRACKER_POLL_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			interval = d
		}
	}

	log.Printf("poller iniciando (intervalo %s)", interval)
	poller.New(st, rd).Run(ctx, interval, func(r poller.Result, err error) {
		if err != nil {
			log.Printf("ciclo erro: %v", err)
			return
		}
		if r.Checked > 0 {
			log.Printf("ciclo: checadas=%d importadas=%d ignoradas=%d pendentes=%d", r.Checked, r.Imported, r.Ignored, r.Pending)
		}
	})
	log.Println("poller encerrado")
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
