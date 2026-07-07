// Command api runs the rps-xml-tracker HTTP API.
//
// Walking-skeleton slice: it can run with an in-memory store (STORE=memory) so
// the ingest->derive->get flow is exercisable without Postgres. The Postgres
// (pgx) store lands behind the same store.Store interface in the next slice.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/EnzzoHosaki/rps-xml-tracker/internal/api"
	"github.com/EnzzoHosaki/rps-xml-tracker/internal/migrate"
	"github.com/EnzzoHosaki/rps-xml-tracker/internal/store"
	"github.com/EnzzoHosaki/rps-xml-tracker/internal/version"
)

func main() {
	log.Printf("tracker-api build %s (%s)", version.Commit, version.BuiltAt)
	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	var st store.Store
	switch getenv("TRACKER_STORE", "memory") {
	case "memory":
		st = store.NewMemory()
		log.Println("store: in-memory (NÃO persistente — só dev/smoke)")
	case "postgres":
		dsn := os.Getenv("TRACKER_PG_DSN")
		if dsn == "" {
			log.Fatal("store: TRACKER_PG_DSN é obrigatório com TRACKER_STORE=postgres")
		}
		if err := migrate.Up(dsn); err != nil {
			log.Fatalf("migrações: %v", err)
		}
		log.Println("migrações: aplicadas")
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		pg, err := store.NewPostgres(ctx, dsn)
		if err != nil {
			log.Fatalf("store: postgres: %v", err)
		}
		defer pg.Close()
		st = pg
		log.Println("store: postgres")
	default:
		log.Fatalf("store: valor inválido para TRACKER_STORE")
	}

	// Cache dos agregados do dashboard (overview/empresas/timeseries): valor servido
	// na hora + refresh em background. As queries custam ~10s (scan de 14M), então um
	// TTL maior reduz o nº de refreshs pesados; dado de monitoramento pode ficar ~1min
	// atrasado. TTL via TRACKER_DASHBOARD_CACHE_TTL (default 60s); "0" desliga.
	ttl := 60 * time.Second
	if v := os.Getenv("TRACKER_DASHBOARD_CACHE_TTL"); v != "" {
		if d, e := time.ParseDuration(v); e == nil {
			ttl = d
		}
	}
	if ttl > 0 {
		cached := store.NewCached(st, ttl)
		st = cached
		log.Printf("cache do dashboard: TTL=%s", ttl)
		// Aquece os agregados em background ao subir, pra o 1º acesso já achar tudo
		// em cache (sem cold-start lento). Serializado internamente (não afoga o banco).
		go func() {
			cached.Warm(context.Background())
			log.Println("cache do dashboard: aquecido")
		}()
	}

	srv := api.New(st, cfg)
	addr := ":" + getenv("TRACKER_API_PORT", "8090")
	httpSrv := &http.Server{
		Addr:              addr,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	log.Printf("tracker-api ouvindo em %s", addr)
	if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("listen: %v", err)
	}
}

// loadConfig reads secrets from env and FAILS CLOSED: an empty JWT secret is a
// boot error (we do NOT replicate maestro's dev bypass that skips auth).
func loadConfig() (api.Config, error) {
	cfg := api.Config{
		JWTSecret:   os.Getenv("MAESTRO_JWT_SECRET"),
		AgentSecret: os.Getenv("TRACKER_AGENT_SECRET"),
	}
	if o := os.Getenv("TRACKER_CORS_ORIGINS"); o != "" {
		cfg.CORSOrigins = strings.Split(o, ",")
	}
	if cfg.JWTSecret == "" {
		return cfg, fmt.Errorf("MAESTRO_JWT_SECRET é obrigatório (fail-closed)")
	}
	if cfg.AgentSecret == "" {
		return cfg, fmt.Errorf("TRACKER_AGENT_SECRET é obrigatório (autentica o agente)")
	}
	return cfg, nil
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
