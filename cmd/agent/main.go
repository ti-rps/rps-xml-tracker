// Command agent runs on SRVIMPORT (Windows). It watches the XML folders
// READ-ONLY, extracts the chave from new files, and submits signed observation
// batches to the tracker API. See internal/agent for the design.
//
// It can run in the foreground (no argument) or as a native Windows service:
//
//	agent.exe install      instala o serviço (captura as TRACKER_* atuais) e inicia
//	agent.exe uninstall    remove o serviço
//	agent.exe start|stop|restart|status
//
// Como serviço ele sobe no boot (StartType=Automatic) e reinicia sozinho se cair
// (OnFailure=restart), independente de sessão de login. As variáveis abaixo são
// lidas do ambiente; no install elas são gravadas no registro do serviço, então
// configure-as no shell ANTES de rodar "agent.exe install".
//
// Config (env):
//
//	TRACKER_API_URL          ex.: http://192.168.10.46:8090   (obrigatório)
//	TRACKER_AGENT_SECRET     segredo HMAC compartilhado        (obrigatório)
//	TRACKER_AGENT_ARRIVAL_ROOT  ex.: F:\Xml_ASincronizar       (obrigatório)
//	TRACKER_AGENT_SYNC_ROOT     ex.: F:\XML SINCRONIZADO        (opcional)
//	TRACKER_AGENT_NAME       default "SRVIMPORT"
//	TRACKER_AGENT_STATE      default <dir do exe>\agent-state.db
//	TRACKER_AGENT_SPOOL      default <dir do exe>\agent-spool
//	TRACKER_AGENT_SCAN_INTERVAL  intervalo da varredura de CHEGADA, default "60s"
//	TRACKER_AGENT_SYNC_INTERVAL  intervalo da varredura do SINCRONIZADO, default "1h"
//	TRACKER_AGENT_SYNC_FULL_EVERY  a cada quanto tempo a varredura do SINCRONIZADO é
//	                         COMPLETA (default "24h"). Entre completas, a varredura
//	                         PODA as partições AAAAMM intocadas (pelo mtime do
//	                         diretório): cai de ~21M arquivos para só as partições
//	                         que receberam arquivo. "0" desliga a poda.
//	TRACKER_AGENT_BACKFILL   "true" para processar o backlog (default false)
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/kardianos/service"

	"github.com/EnzzoHosaki/rps-xml-tracker/internal/agent"
	"github.com/EnzzoHosaki/rps-xml-tracker/internal/ingest"
	"github.com/EnzzoHosaki/rps-xml-tracker/internal/model"
	"github.com/EnzzoHosaki/rps-xml-tracker/internal/signing"
	"github.com/EnzzoHosaki/rps-xml-tracker/internal/version"
)

const svcName = "RpsXmlTrackerAgent"

// program implements service.Interface: Start launches the scan loop in the
// background and returns immediately; Stop cancels it and closes the state DB.
type program struct {
	ag              *agent.Agent
	arrivalInterval time.Duration // varredura da pasta de chegada (curto: ela esvazia)
	syncInterval    time.Duration // varredura do SINCRONIZADO (longo: milhões de arquivos)
	ctx             context.Context
	cancel          context.CancelFunc
	wg              sync.WaitGroup
	apiURL          string
	secret          string
	name            string
}

func (p *program) Start(service.Service) error {
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		log.Printf("agente iniciando — build %s, chegada=%s sync=%s backfill=%v",
			version.Commit, p.arrivalInterval, p.syncInterval, os.Getenv("TRACKER_AGENT_BACKFILL") == "true")
		p.ag.RunSplit(p.ctx, p.arrivalInterval, p.syncInterval, func(group string, r agent.Result, err error) {
			switch {
			case err != nil:
				log.Printf("scan[%s] erro: %v", group, err)
				postHeartbeat(p.ctx, p.apiURL, p.secret, p.name, map[string]any{
					"scan_type": group, "error": err.Error(),
				})
			case r.Seeded:
				log.Printf("scan[%s] primeira execução: cutoff de backlog gravado — arquivos anteriores ignorados; emitindo apenas novos a partir de agora", group)
				fallthrough
			default:
				if r.New > 0 || r.PrunedDirs > 0 {
					log.Printf("scan[%s]: escaneados=%d novos=%d emitidos=%d sem_chave=%d podadas=%d completa=%v",
						group, r.Scanned, r.New, r.Emitted, r.SkippedNoChave, r.PrunedDirs, r.FullScan)
				}
				postHeartbeat(p.ctx, p.apiURL, p.secret, p.name, map[string]any{
					"scan_type":          group,
					"escaneados":         r.Scanned,
					"novos":              r.New,
					"emitidos":           r.Emitted,
					"sem_chave":          r.SkippedNoChave,
					"particoes_podadas":  r.PrunedDirs,
					"varredura_completa": r.FullScan,
				})
			}
		})
	}()
	return nil
}

func (p *program) Stop(service.Service) error {
	p.cancel()
	p.wg.Wait()
	log.Println("agente encerrado")
	return p.ag.Close()
}

// build wires the agent from the environment. Fatal if a required var is missing.
func (p *program) build() {
	apiURL := mustEnv("TRACKER_API_URL")
	secret := mustEnv("TRACKER_AGENT_SECRET")
	arrivalRoot := mustEnv("TRACKER_AGENT_ARRIVAL_ROOT")
	name := getenv("TRACKER_AGENT_NAME", "SRVIMPORT")
	base := exeDir()
	spool := getenv("TRACKER_AGENT_SPOOL", filepath.Join(base, "agent-spool"))

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

	// Poda por recência do SINCRONIZADO: completa a cada 24h por default; "0" desliga.
	syncFullEvery := 24 * time.Hour
	if v := os.Getenv("TRACKER_AGENT_SYNC_FULL_EVERY"); v != "" {
		if d, e := time.ParseDuration(v); e == nil {
			syncFullEvery = d
		}
	}

	ag, err := agent.New(agent.Config{
		Name:          name,
		Roots:         roots,
		StatePath:     getenv("TRACKER_AGENT_STATE", filepath.Join(base, "agent-state.db")),
		Backfill:      os.Getenv("TRACKER_AGENT_BACKFILL") == "true",
		SyncFullEvery: syncFullEvery,
	}, client)
	if err != nil {
		log.Fatalf("agent: %v", err)
	}

	p.ag = ag
	p.apiURL = apiURL
	p.secret = secret
	p.name = name
	// Chegada: curto (a pasta esvazia). Sync: longo (milhões de arquivos por passada).
	p.arrivalInterval = 60 * time.Second
	if v := os.Getenv("TRACKER_AGENT_SCAN_INTERVAL"); v != "" {
		if d, e := time.ParseDuration(v); e == nil {
			p.arrivalInterval = d
		}
	}
	p.syncInterval = time.Hour
	if v := os.Getenv("TRACKER_AGENT_SYNC_INTERVAL"); v != "" {
		if d, e := time.ParseDuration(v); e == nil {
			p.syncInterval = d
		}
	}
	p.ctx, p.cancel = context.WithCancel(context.Background())
}

// postHeartbeat envia o heartbeat do agente para a API (mesmo HMAC do ingest).
// Falhas são logadas e descartadas — não interrompem o scan.
func postHeartbeat(ctx context.Context, apiURL, secret, agentName string, payload map[string]any) {
	if apiURL == "" {
		return
	}
	payload["agent_name"] = agentName
	payload["version"] = version.Commit // visível no GET /status — denuncia agent.exe defasado
	body, err := json.Marshal(payload)
	if err != nil {
		return
	}
	sig := signing.Sign(secret, body)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		strings.TrimRight(apiURL, "/")+"/api/v1/ingest/agent/heartbeat",
		bytes.NewReader(body))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Agent-Signature", sig)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("heartbeat: %v", err)
		return
	}
	resp.Body.Close()
}

func main() {
	verb := ""
	if len(os.Args) > 1 {
		verb = os.Args[1]
	}

	svcConfig := &service.Config{
		Name:        svcName,
		DisplayName: "RPS XML Tracker Agent",
		Description: "Rastreia XMLs de chegada/sincronizado (READ-ONLY) e envia observações ao tracker.",
		Option: service.KeyValue{
			"OnFailure":              "restart", // reinicia sozinho se o processo cair
			"OnFailureDelayDuration": "5s",
			"OnFailureResetPeriod":   86400, // zera a contagem de falhas após 1 dia
		},
	}
	// No install, persistimos as TRACKER_* atuais no registro do serviço, para
	// que não dependam da sessão que rodou o install.
	if verb == "install" {
		requireEnv("TRACKER_API_URL", "TRACKER_AGENT_SECRET", "TRACKER_AGENT_ARRIVAL_ROOT")
		svcConfig.EnvVars = captureTrackerEnv()
	}

	prg := &program{}
	svc, err := service.New(prg, svcConfig)
	if err != nil {
		log.Fatalf("service: %v", err)
	}

	if verb != "" {
		if err := control(svc, verb); err != nil {
			log.Fatal(err)
		}
		return
	}

	// Sem argumento: roda (funciona tanto como serviço do Windows quanto em
	// primeiro plano para desenvolvimento). Como serviço, manda o log p/ arquivo.
	setupLog()
	prg.build()
	if err := svc.Run(); err != nil {
		log.Fatalf("run: %v", err)
	}
}

// control executes a lifecycle verb against the installed service.
func control(svc service.Service, verb string) error {
	switch verb {
	case "install":
		if err := service.Control(svc, "install"); err != nil {
			return fmt.Errorf("install: %w", err)
		}
		if err := service.Control(svc, "start"); err != nil {
			return fmt.Errorf("instalado, mas falhou ao iniciar: %w", err)
		}
		log.Printf("serviço %q instalado e iniciado (autostart no boot, restart se cair)", svcName)
		return nil
	case "uninstall", "start", "stop", "restart":
		if err := service.Control(svc, verb); err != nil {
			return fmt.Errorf("%s: %w", verb, err)
		}
		log.Printf("serviço %q: %s ok", svcName, verb)
		return nil
	case "status":
		st, err := svc.Status()
		if err != nil {
			return err
		}
		log.Printf("serviço %q: %s", svcName, statusText(st))
		return nil
	default:
		return fmt.Errorf("comando desconhecido %q (use: install | uninstall | start | stop | restart | status)", verb)
	}
}

func statusText(s service.Status) string {
	switch s {
	case service.StatusRunning:
		return "rodando"
	case service.StatusStopped:
		return "parado"
	default:
		return "desconhecido (não instalado?)"
	}
}

// setupLog redirects the standard logger to a file next to the executable when
// running as a service (no console). Interactive runs keep logging to stderr.
func setupLog() {
	if service.Interactive() {
		return
	}
	f, err := os.OpenFile(filepath.Join(exeDir(), "agent.log"),
		os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err == nil {
		log.SetOutput(f)
	}
}

// captureTrackerEnv snapshots every TRACKER_* var so install can persist them.
func captureTrackerEnv() map[string]string {
	m := map[string]string{}
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, "TRACKER_") {
			continue
		}
		if k, v, ok := strings.Cut(kv, "="); ok {
			m[k] = v
		}
	}
	return m
}

func exeDir() string {
	exe, err := os.Executable()
	if err != nil {
		return "."
	}
	return filepath.Dir(exe)
}

func requireEnv(keys ...string) {
	var missing []string
	for _, k := range keys {
		if os.Getenv(k) == "" {
			missing = append(missing, k)
		}
	}
	if len(missing) > 0 {
		log.Fatalf("install: defina estas variáveis no shell antes de instalar: %s", strings.Join(missing, ", "))
	}
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
