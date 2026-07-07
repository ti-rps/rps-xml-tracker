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
//	TRACKER_RECONCILE_INTERVAL  reconcile contínuo (default 30m; 0 desliga)
//	TRACKER_RECONCILE_WINDOW    janela deslizante auditada (default 24h)
//	TRACKER_RECONCILE_GRACE     desconto do atraso de detecção (default 15m)
//	TRACKER_RECONCILE_FIX       self-heal das faltantes (default true; "false" desliga)
package main

import (
	"context"
	"log"
	"math"
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
		// Prioridade da rotação: fatia do lote reservada às notas com synced_at
		// recente (detecta import/ignore em 1-2 ciclos). Defaults 48h/0.7;
		// TRACKER_POLL_HOT_WINDOW=0 desliga (LRU puro, comportamento antigo).
		hotWindow := 48 * time.Hour
		if v := os.Getenv("TRACKER_POLL_HOT_WINDOW"); v != "" {
			if d, err := time.ParseDuration(v); err == nil {
				hotWindow = d
			}
		}
		hotFraction := 0.7
		if v := os.Getenv("TRACKER_POLL_HOT_FRACTION"); v != "" {
			if f, err := strconv.ParseFloat(v, 64); err == nil {
				hotFraction = f
			}
		}
		pg.SetPollPriority(hotWindow, hotFraction)
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

	// Reconcile contínuo (P0.4): a cada intervalo compara a janela deslizante entre o
	// Athenas (IMPORTADO=1 por DATAINCLUSAO) e o tracker (imported_at) e publica a
	// acurácia do import no heartbeat (GET /status). 0 desliga. O grace desconta o
	// atraso natural de detecção (sweep/rotação). Com fix (default), o tracker se
	// autocorrige emitindo as 'imported' que o Athenas confirmar.
	reconcileInterval := 30 * time.Minute
	if v := os.Getenv("TRACKER_RECONCILE_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			reconcileInterval = d
		}
	}
	reconcileWindow := 24 * time.Hour
	if v := os.Getenv("TRACKER_RECONCILE_WINDOW"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			reconcileWindow = d
		}
	}
	reconcileGrace := 15 * time.Minute
	if v := os.Getenv("TRACKER_RECONCILE_GRACE"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			reconcileGrace = d
		}
	}
	reconcileFix := os.Getenv("TRACKER_RECONCILE_FIX") != "false"

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
	log.Printf("poller iniciando (intervalo %s, lote %d, sweep a cada %s janela %s, reconcile a cada %s janela %s grace %s fix=%v)",
		interval, batch, sweepInterval, sweepWindow, reconcileInterval, reconcileWindow, reconcileGrace, reconcileFix)

	if reconcileInterval > 0 {
		go p.RunReconcile(ctx, reconcileInterval, reconcileWindow, reconcileGrace, reconcileFix,
			func(r poller.ReconcileResult, err error) {
				if err != nil {
					log.Printf("reconcile erro: %v", err)
					hbMu.Lock()
					hbPayload["reconcile_error"] = err.Error()
					pay := copyPayload()
					hbMu.Unlock()
					_ = st.UpsertHeartbeat(context.Background(), "poller", pay)
					return
				}
				accuracy := 100.0
				if r.Athena > 0 {
					accuracy = math.Round(10000*float64(r.Athena-r.Missing)/float64(r.Athena)) / 100
				}
				log.Printf("reconcile: janela=[%s, %s) athenas=%d rastreadas=%d faltando=%d corrigidas=%d acuracia=%.2f%%",
					r.Since.Format("01-02 15:04"), r.Until.Format("01-02 15:04"),
					r.Athena, r.Tracker, r.Missing, r.Fixed, accuracy)
				if len(r.MissingSample) > 0 {
					log.Printf("reconcile: amostra faltantes: %v", r.MissingSample)
				}
				hbMu.Lock()
				delete(hbPayload, "reconcile_error")
				hbPayload["reconcile_at"] = time.Now().Format(time.RFC3339)
				hbPayload["reconcile_window_h"] = reconcileWindow.Hours()
				hbPayload["reconcile_athenas"] = r.Athena
				hbPayload["reconcile_tracker"] = r.Tracker
				hbPayload["reconcile_missing"] = r.Missing
				delete(hbPayload, "reconcile_extra") // removido (era ruído de skew; ver ReconcileOnce)
				hbPayload["reconcile_fixed"] = r.Fixed
				hbPayload["reconcile_accuracy_pct"] = accuracy
				if len(r.MissingSample) > 0 {
					hbPayload["reconcile_missing_sample"] = r.MissingSample
				} else {
					delete(hbPayload, "reconcile_missing_sample")
				}
				pay := copyPayload()
				hbMu.Unlock()
				_ = st.UpsertHeartbeat(context.Background(), "poller", pay)
			})
	}

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
				log.Printf("sweep: encontradas=%d importadas=%d ignoradas=%d pendentes=%d emitidas=%d dedup=%d",
					r.Found, r.Imported, r.Ignored, r.Pending, r.Emitted, r.Skipped)
			}
			hbMu.Lock()
			delete(hbPayload, "sweep_error")
			hbPayload["sweep_found"] = r.Found
			hbPayload["sweep_imported"] = r.Imported
			hbPayload["sweep_ignored"] = r.Ignored
			hbPayload["sweep_pending"] = r.Pending
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
