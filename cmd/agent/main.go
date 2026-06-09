// Command agent runs on SRVIMPORT (Windows). It watches the XML folders
// READ-ONLY, extracts the chave from new files, and submits signed observation
// batches to the tracker API. See internal/agent for the design.
//
// Config (env):
//
//	TRACKER_API_URL          ex.: http://192.168.10.46:8090   (obrigatório)
//	TRACKER_AGENT_SECRET     segredo HMAC compartilhado        (obrigatório)
//	TRACKER_AGENT_NAME       default "SRVIMPORT"
//	TRACKER_AGENT_ARRIVAL_ROOT  ex.: F:\Xml_ASincronizar       (obrigatório)
//	TRACKER_AGENT_SYNC_ROOT     ex.: F:\XML SINCRONIZADO        (opcional)
//	TRACKER_AGENT_STATE      default "agent-state.db"
//	TRACKER_AGENT_SPOOL      default "agent-spool"
//	TRACKER_AGENT_SCAN_INTERVAL  default "60s"
//	TRACKER_AGENT_BACKFILL   "true" para processar o backlog (default false)
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/EnzzoHosaki/rps-xml-tracker/internal/agent"
	"github.com/EnzzoHosaki/rps-xml-tracker/internal/ingest"
	"github.com/EnzzoHosaki/rps-xml-tracker/internal/model"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	apiURL := mustEnv("TRACKER_API_URL")
	secret := mustEnv("TRACKER_AGENT_SECRET")
	arrivalRoot := mustEnv("TRACKER_AGENT_ARRIVAL_ROOT")
	name := getenv("TRACKER_AGENT_NAME", "SRVIMPORT")
	spool := getenv("TRACKER_AGENT_SPOOL", "agent-spool")

	client, err := ingest.New(apiURL, name, secret, spool)
	if err != nil {
		log.Fatalf("ingest client: %v", err)
	}

	roots := []agent.Root{
		{Path: arrivalRoot, Stage: model.StageArrival, Event: model.EventFileSeen},
	}
	if sync := os.Getenv("TRACKER_AGENT_SYNC_ROOT"); sync != "" {
		roots = append(roots, agent.Root{Path: sync, Stage: model.StageSync, Event: model.EventFileMoved})
	}

	ag, err := agent.New(agent.Config{
		Name:      name,
		Roots:     roots,
		StatePath: getenv("TRACKER_AGENT_STATE", "agent-state.db"),
		Backfill:  os.Getenv("TRACKER_AGENT_BACKFILL") == "true",
	}, client)
	if err != nil {
		log.Fatalf("agent: %v", err)
	}
	defer ag.Close()

	interval := 60 * time.Second
	if v := os.Getenv("TRACKER_AGENT_SCAN_INTERVAL"); v != "" {
		if d, e := time.ParseDuration(v); e == nil {
			interval = d
		}
	}

	log.Printf("agente %q iniciando — roots=%d intervalo=%s backfill=%v",
		name, len(roots), interval, os.Getenv("TRACKER_AGENT_BACKFILL") == "true")
	ag.Run(ctx, interval, func(r agent.Result, err error) {
		if err != nil {
			log.Printf("scan erro: %v", err)
			return
		}
		if r.Seeded {
			log.Printf("seed inicial do backlog: %d arquivos marcados como vistos (sem emitir)", r.New)
			return
		}
		if r.New > 0 {
			log.Printf("scan: escaneados=%d novos=%d emitidos=%d sem_chave=%d",
				r.Scanned, r.New, r.Emitted, r.SkippedNoChave)
		}
	})
	log.Println("agente encerrado")
}

func mustEnv(k string) string {
	v := os.Getenv(k)
	if v == "" {
		log.Fatalf("%s é obrigatório", k)
	}
	return v
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
