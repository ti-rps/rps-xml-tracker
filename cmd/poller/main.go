// Command poller closes the import span: it polls the Athenas Firebird
// (read-only, chave-driven) for in-flight notas and emits import observations.
//
// In production it shares the tracker's Postgres with the API. Config:
//
//	TRACKER_FB_DSN   firebirdsql DSN (Legacy_Auth, wire_crypt disabled)
//	TRACKER_STORE    postgres (default) | memory
//	TRACKER_PG_DSN   Postgres DSN (when TRACKER_STORE=postgres)
//	TRACKER_POLL_INTERVAL  e.g. 30s (default)
//	TRACKER_POLL_BATCH     chaves in-flight por ciclo (default 8000)
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync"
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

	// Chaves in-flight checadas por ciclo. Default alto (8000) p/ drenar backlogs
	// grandes (milhões de in-flight) — a rotação cai de horas p/ ~1-2h. Tunável.
	batch := 8000
	if v := os.Getenv("TRACKER_POLL_BATCH"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			batch = n
		}
	}

	sweepInterval := 5 * time.Minute
	if v := os.Getenv("TRACKER_SWEEP_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			sweepInterval = d
		}
	}

	sweepWindow := 4 * time.Hour
	if v := os.Getenv("TRACKER_SWEEP_WINDOW"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			sweepWindow = d
		}
	}

	var (
		hbMu      sync.Mutex
		hbPayload = map[string]any{
			"batch":            batch,
			"sweep_interval_s": int(sweepInterval.Seconds()),
			"sweep_window_h":   sweepWindow.Hours(),
		}
	)
	copyPayload := func() map[string]any {
		out := make(map[string]any, len(hbPayload))
		for k, v := range hbPayload {
			out[k] = v
		}
		return out
	}

	p := poller.New(st, rd)
	p.SetBatch(batch)
	log.Printf("poller iniciando (intervalo %s, lote %d, sweep a cada %s janela %s)",
		interval, batch, sweepInterval, sweepWindow)
	p.RunWithSweep(ctx, interval, sweepInterval, sweepWindow,
		func(r poller.Result, err error) {
			if err != nil {
				log.Printf("ciclo erro: %v", err)
				hbMu.Lock()
				hbPayload["poll_error"] = err.Error()
				pay := copyPayload()
				hbMu.Unlock()
				_ = st.UpsertHeartbeat(context.Background(), "poller", pay)
				return
			}
			if r.Checked > 0 {
				log.Printf("ciclo: checadas=%d importadas=%d ignoradas=%d pendentes=%d",
					r.Checked, r.Imported, r.Ignored, r.Pending)
			}
			hbMu.Lock()
			delete(hbPayload, "poll_error")
			hbPayload["poll_checked"] = r.Checked
			hbPayload["poll_imported"] = r.Imported
			hbPayload["poll_ignored"] = r.Ignored
			hbPayload["poll_pending"] = r.Pending
			pay := copyPayload()
			hbMu.Unlock()
			_ = st.UpsertHeartbeat(context.Background(), "poller", pay)
		},
		func(r poller.SweepResult, err error) {
			if err != nil {
				log.Printf("sweep erro: %v", err)
				hbMu.Lock()
				hbPayload["sweep_error"] = err.Error()
				pay := copyPayload()
				hbMu.Unlock()
				_ = st.UpsertHeartbeat(context.Background(), "poller", pay)
				return
			}
			if r.Emitted > 0 {
				log.Printf("sweep: encontradas=%d emitidas=%d dedup=%d", r.Found, r.Emitted, r.Skipped)
			}
			hbMu.Lock()
			delete(hbPayload, "sweep_error")
			hbPayload["sweep_found"] = r.Found
			hbPayload["sweep_emitted"] = r.Emitted
			hbPayload["sweep_skipped"] = r.Skipped
			pay := copyPayload()
			hbMu.Unlock()
			_ = st.UpsertHeartbeat(context.Background(), "poller", pay)
		},
	)
	log.Println("poller encerrado")
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
