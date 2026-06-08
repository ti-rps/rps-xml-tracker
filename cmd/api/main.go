// Command api runs the rps-xml-tracker HTTP API.
//
// Walking-skeleton slice: it can run with an in-memory store (STORE=memory) so
// the ingest->derive->get flow is exercisable without Postgres. The Postgres
// (pgx) store lands behind the same store.Store interface in the next slice.
package main

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/EnzzoHosaki/rps-xml-tracker/internal/api"
	"github.com/EnzzoHosaki/rps-xml-tracker/internal/store"
)

func main() {
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
		log.Fatal("store: postgres ainda não implementado (próxima fatia)")
	default:
		log.Fatalf("store: valor inválido para TRACKER_STORE")
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
