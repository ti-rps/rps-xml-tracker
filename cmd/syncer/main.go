// Command syncer roda no SRVIMPORT (Windows), ao lado do agente, e ASSUME a
// sincronização do DownloadXML (shadow-sync F1): move XMLs de ASINCRONIZAR →
// SINCRONIZADO e insere na TABLISTACHAVEACESSO com IMPORTADO=0. Ver
// design/SHADOW-SYNC.md e internal/syncer.
//
// Serviço Windows nativo (mesmo padrão do agente):
//
//	syncer.exe install      instala (captura as TRACKER_* atuais) e inicia
//	syncer.exe uninstall | start | stop | restart | status
//
// Modos (flags, avaliadas ANTES do modo serviço):
//
//	--dry-run             só planeja e loga/grava o plano; NENHUMA escrita (modo da F1)
//	--chave <44> --file <path>   sincroniza SÓ essa chave (gatilho do piloto F2)
//	--once                uma varredura e sai (sem loop)
//
// Config (env):
//
//	TRACKER_SYNCER_ENABLED      "true" para o binário rodar (default: sai na hora)
//	TRACKER_API_URL             API do tracker (observações + heartbeat)
//	TRACKER_AGENT_SECRET        segredo HMAC do ingest (o mesmo do agente)
//	TRACKER_FB_DSN              Firebird READ-ONLY (resolução + pre-checks)
//	TRACKER_FB_WRITE_DSN        Firebird de ESCRITA (obrigatório fora do dry-run)
//	TRACKER_SYNCER_ARRIVAL_ROOT ex.: F:\Xml_ASincronizar
//	TRACKER_SYNCER_SYNC_ROOT    ex.: F:\XML SINCRONIZADO
//	TRACKER_SYNCER_EMPRESAS     allowlist CODIGOEMPRESA, csv (varredura)
//	TRACKER_SYNCER_DIRS         allowlist de subpastas da ASINCRONIZAR, csv (varredura)
//	TRACKER_SYNCER_MAX_PER_CYCLE  planos/execuções por ciclo (default 1)
//	TRACKER_SYNCER_MAX_SCAN     arquivos examinados por ciclo (default 5000)
//	TRACKER_SYNCER_INTERVAL     intervalo entre varreduras (default "5m")
//	TRACKER_SYNCER_ALLOW_STALE  "true" p/ sincronizar emissão fora da janela do robô
//	TRACKER_SYNCER_NAME         default "SRVIMPORT"
//	TRACKER_SYNCER_STATE        journal bbolt (default <exe>\syncer-state.db)
//	TRACKER_SYNCER_SPOOL        spool do ingest (default <exe>\syncer-spool)
//	TRACKER_SYNCER_PLANS        JSONL dos planos do dry-run (default <exe>\syncer-plans.jsonl)
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/kardianos/service"

	"github.com/EnzzoHosaki/rps-xml-tracker/internal/firebird"
	"github.com/EnzzoHosaki/rps-xml-tracker/internal/ingest"
	"github.com/EnzzoHosaki/rps-xml-tracker/internal/signing"
	"github.com/EnzzoHosaki/rps-xml-tracker/internal/syncer"
	"github.com/EnzzoHosaki/rps-xml-tracker/internal/version"
)

const svcName = "RpsXmlTrackerSyncer"

type program struct {
	sn       *syncer.Syncer
	rd       *firebird.Reader
	wr       *firebird.Writer
	interval time.Duration
	dryRun   bool
	apiURL   string
	secret   string
	name     string
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

func (p *program) Start(service.Service) error {
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		log.Printf("syncer iniciando — build %s, dry-run=%v intervalo=%s", version.Commit, p.dryRun, p.interval)
		t := time.NewTicker(p.interval)
		defer t.Stop()
		for {
			p.cycle()
			select {
			case <-p.ctx.Done():
				return
			case <-t.C:
			}
		}
	}()
	return nil
}

func (p *program) cycle() {
	res, err := p.sn.SweepOnce(p.ctx)
	pay := map[string]any{
		"modo":       modo(p.dryRun),
		"escaneados": res.Scanned,
		"planejados": res.Planned,
		"executados": res.Executed,
		"erros":      res.Errors,
	}
	for k, v := range res.Skips {
		pay["skip_"+k] = v
	}
	if err != nil {
		log.Printf("varredura erro: %v", err)
		pay["error"] = err.Error()
	} else if res.Planned > 0 || res.Errors > 0 {
		log.Printf("varredura: escaneados=%d planejados=%d executados=%d erros=%d skips=%v",
			res.Scanned, res.Planned, res.Executed, res.Errors, res.Skips)
	}
	postHeartbeat(p.ctx, p.apiURL, p.secret, p.name, pay)
}

func (p *program) Stop(service.Service) error {
	p.cancel()
	p.wg.Wait()
	log.Println("syncer encerrado")
	p.closeAll()
	return nil
}

func (p *program) closeAll() {
	if p.sn != nil {
		_ = p.sn.Close()
	}
	if p.rd != nil {
		_ = p.rd.Close()
	}
	if p.wr != nil {
		_ = p.wr.Close()
	}
}

func modo(dry bool) string {
	if dry {
		return "dry-run"
	}
	return "real"
}

// build liga o syncer a partir do ambiente. Fatal se faltar variável obrigatória.
func (p *program) build(dryRun, allowStale bool) {
	apiURL := mustEnv("TRACKER_API_URL")
	secret := mustEnv("TRACKER_AGENT_SECRET")
	fbDSN := mustEnv("TRACKER_FB_DSN")
	arrivalRoot := mustEnv("TRACKER_SYNCER_ARRIVAL_ROOT")
	syncRoot := mustEnv("TRACKER_SYNCER_SYNC_ROOT")
	name := getenv("TRACKER_SYNCER_NAME", "SRVIMPORT")
	base := exeDir()

	ctx := context.Background()
	rd, err := firebird.NewReader(ctx, fbDSN)
	if err != nil {
		log.Fatalf("firebird (ro): %v", err)
	}
	var wr *firebird.Writer
	if !dryRun {
		wr, err = firebird.NewWriter(ctx, mustEnv("TRACKER_FB_WRITE_DSN"))
		if err != nil {
			log.Fatalf("firebird (write): %v", err)
		}
	}
	client, err := ingest.New(apiURL, name, secret, getenv("TRACKER_SYNCER_SPOOL", filepath.Join(base, "syncer-spool")))
	if err != nil {
		log.Fatalf("ingest client: %v", err)
	}

	cfg := syncer.Config{
		Name:        name,
		ArrivalRoot: arrivalRoot,
		SyncRoot:    syncRoot,
		JournalPath: getenv("TRACKER_SYNCER_STATE", filepath.Join(base, "syncer-state.db")),
		PlansPath:   getenv("TRACKER_SYNCER_PLANS", filepath.Join(base, "syncer-plans.jsonl")),
		DryRun:      dryRun,
		AllowStale:  allowStale,
		MaxPerCycle: envInt("TRACKER_SYNCER_MAX_PER_CYCLE", 1),
		MaxScanPer:  envInt("TRACKER_SYNCER_MAX_SCAN", 5000),
		Empresas:    parseEmpresas(os.Getenv("TRACKER_SYNCER_EMPRESAS")),
		Dirs:        parseCSV(os.Getenv("TRACKER_SYNCER_DIRS")),
		Marker:      "sync rps-xml-tracker " + version.Commit,
		Log:         log.Printf,
	}
	var ins syncerInserter
	if wr != nil {
		ins = wr
	}
	sn, err := syncer.New(cfg, rd, ins, client)
	if err != nil {
		log.Fatalf("syncer: %v", err)
	}

	p.sn, p.rd, p.wr = sn, rd, wr
	p.apiURL, p.secret, p.name = apiURL, secret, name
	p.dryRun = dryRun
	p.interval = 5 * time.Minute
	if v := os.Getenv("TRACKER_SYNCER_INTERVAL"); v != "" {
		if d, e := time.ParseDuration(v); e == nil {
			p.interval = d
		}
	}
	p.ctx, p.cancel = context.WithCancel(ctx)
}

// syncerInserter espelha a interface não-exportada do pacote syncer — um wr nil
// tipado (*firebird.Writer)(nil) não pode virar interface não-nil por engano.
type syncerInserter interface {
	NextChaveID(ctx context.Context) (int64, error)
	InsertChaveAcesso(ctx context.Context, id int64, r firebird.InsertRow) error
}

func postHeartbeat(ctx context.Context, apiURL, secret, name string, payload map[string]any) {
	payload["service"] = "syncer" // o handler roteia p/ service_heartbeats["syncer"]
	payload["agent_name"] = name
	payload["version"] = version.Commit
	body, err := json.Marshal(payload)
	if err != nil {
		return
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		strings.TrimRight(apiURL, "/")+"/api/v1/ingest/agent/heartbeat", bytes.NewReader(body))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Agent-Signature", signing.Sign(secret, body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("heartbeat: %v", err)
		return
	}
	resp.Body.Close()
}

func main() {
	dryRun := flag.Bool("dry-run", false, "só planeja e registra o plano; nenhuma escrita (modo da F1)")
	chave := flag.String("chave", "", "sincroniza SÓ esta chave (44 dígitos; exige --file)")
	file := flag.String("file", "", "caminho do XML na ASINCRONIZAR (com --chave)")
	once := flag.Bool("once", false, "uma varredura e sai")
	allowStale := flag.Bool("allow-stale", false, "permite emissão fora da janela do AthenasHorse (mês atual+anterior)")
	flag.Parse()

	verb := flag.Arg(0)

	// A trava geral: sem ENABLED o binário sai — ninguém liga o syncer sem querer.
	// (Verbos de serviço passam: install/uninstall não tocam em nada.)
	if os.Getenv("TRACKER_SYNCER_ENABLED") != "true" && verb == "" {
		log.Fatal("TRACKER_SYNCER_ENABLED != true — o syncer não roda (trava de segurança do shadow-sync)")
	}

	svcConfig := &service.Config{
		Name:        svcName,
		DisplayName: "RPS XML Tracker Syncer",
		Description: "Shadow-sync: move XMLs ASINCRONIZAR -> SINCRONIZADO e insere na TABLISTACHAVEACESSO (IMPORTADO=0).",
		Option: service.KeyValue{
			"OnFailure":              "restart",
			"OnFailureDelayDuration": "5s",
			"OnFailureResetPeriod":   86400,
		},
	}
	if verb == "install" {
		requireEnv("TRACKER_SYNCER_ENABLED", "TRACKER_API_URL", "TRACKER_AGENT_SECRET",
			"TRACKER_FB_DSN", "TRACKER_SYNCER_ARRIVAL_ROOT", "TRACKER_SYNCER_SYNC_ROOT")
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

	setupLog()

	// Gatilho single-key (piloto F2): roda uma vez e sai, sem loop de serviço.
	if *chave != "" {
		prg.build(*dryRun, *allowStale)
		defer prg.closeAll()
		plan, err := prg.sn.RunChave(prg.ctx, *chave, *file)
		if err != nil {
			log.Fatalf("chave %s: %v", *chave, err)
		}
		if *dryRun {
			log.Printf("dry-run concluído — plano com %d participação(ões) registrado; NADA foi escrito", len(plan.Participacoes))
		} else {
			log.Printf("sincronização da chave %s concluída (%d participação(ões))", *chave, len(plan.Participacoes))
		}
		return
	}

	prg.build(*dryRun, *allowStale)
	if *once {
		defer prg.closeAll()
		prg.cycle()
		return
	}
	if err := svc.Run(); err != nil {
		log.Fatalf("run: %v", err)
	}
}

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

func setupLog() {
	if service.Interactive() {
		return
	}
	f, err := os.OpenFile(filepath.Join(exeDir(), "syncer.log"),
		os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err == nil {
		log.SetOutput(f)
	}
}

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

func parseEmpresas(csv string) map[int]bool {
	out := map[int]bool{}
	for _, s := range parseCSV(csv) {
		if n, err := strconv.Atoi(s); err == nil {
			out[n] = true
		}
	}
	return out
}

func parseCSV(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func envInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
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
