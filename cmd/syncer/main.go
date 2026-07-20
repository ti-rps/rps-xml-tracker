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
//	--worklist [--filial-max N]  lê a lista de pendentes do tracker (agent) via PG e sincroniza — SEM varrer o FS
//	--worklist-api [--filial-max N]  idem, mas busca a lista pela API (POST HMAC) — caminho de produção (5432 não exposta)
//	--audit [--since d] [--dump f]  READ-ONLY: conta as nossas linhas (marcador); --since escopa (índice); --dump grava manifesto JSONL
//	--rollback <44> --yes DESTRUTIVO: desfaz o sync dessa chave (§10)
//	--rollback-all --yes  DESTRUTIVO: rollback em lote de TODAS as nossas linhas IMPORTADO=0 (§14.1)
//	--selftest-rollback --yes  insere+desfaz uma isca sintética; testa o rollback na tabela real (§14.3)
//
// Config (env):
//
//	TRACKER_SYNCER_ENABLED      "true" para o binário rodar (default: sai na hora)
//	TRACKER_SYNCER_DRY_RUN      default "true" — o modo REAL exige explicitamente
//	                            "false". O serviço Windows roda SEM flags, então o
//	                            dry-run tem de ser o default por env: um install
//	                            sem a variável nunca pode escrever por omissão.
//	TRACKER_API_URL             API do tracker (observações + heartbeat)
//	TRACKER_AGENT_SECRET        segredo HMAC do ingest (o mesmo do agente)
//	TRACKER_FB_DSN              Firebird READ-ONLY (resolução + pre-checks)
//	TRACKER_FB_WRITE_DSN        Firebird de ESCRITA (obrigatório fora do dry-run)
//	TRACKER_PG_DSN              Postgres do tracker (só p/ --worklist: lê a lista do agent)
//	TRACKER_SYNCER_ARRIVAL_ROOT ex.: F:\Xml_ASincronizar
//	TRACKER_SYNCER_SYNC_ROOT    ex.: F:\XML SINCRONIZADO
//	TRACKER_SYNCER_MODE         "worklist" p/ o serviço rodar por lista (API) em vez de varrer; default varredura
//	TRACKER_SYNCER_FILIAL_MAX   com MODE=worklist: limita codigo_filial<=N (0 = todas)
//	TRACKER_SYNCER_EMPRESAS     allowlist CODIGOEMPRESA, csv (varredura e worklist)
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
	"sort"
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
	sn           *syncer.Syncer
	rd           *firebird.Reader
	wr           *firebird.Writer
	interval     time.Duration
	dryRun       bool
	apiURL       string
	secret       string
	name         string
	worklistMode bool  // TRACKER_SYNCER_MODE=worklist: cycle() busca a lista pela API em vez de varrer
	empresas     []int // allowlist (codigo_empresa) p/ a worklist
	filialMax    int   // limita codigo_filial<=N na worklist (0 = todas)
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
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
	if p.worklistMode {
		p.worklistCycle()
		return
	}
	res, err := p.sn.SweepOnce(p.ctx)
	pay := map[string]any{
		"modo":       modo(p.dryRun),
		"escaneados": res.Scanned,
		"planejados": res.Planned,
		"executados": res.Executed,
		"erros":      res.Errors,
		"cursor":     res.CursorEnd,
		"wrap":       res.Wrapped,
	}
	for k, v := range res.Skips {
		pay["skip_"+k] = v
	}
	if err != nil {
		log.Printf("varredura erro: %v", err)
		pay["error"] = err.Error()
	} else {
		// Uma linha por ciclo: é o que permite ver a rotação do cursor andando
		// pelo backlog (a correção da starvation do MaxScanPer).
		log.Printf("varredura: escaneados=%d planejados=%d executados=%d erros=%d cursor=%q→%q wrap=%v skips=%v",
			res.Scanned, res.Planned, res.Executed, res.Errors, res.CursorStart, res.CursorEnd, res.Wrapped, res.Skips)
	}
	postHeartbeat(p.ctx, p.apiURL, p.secret, p.name, pay)
}

// worklistCycle é UM ciclo do modo worklist: busca a lista de pendentes pela API
// (agent=olhos) e sincroniza (syncer=mãos), SEM varrer o FS. Usado pelo loop do
// serviço (cycle) e pelo one-shot --worklist-api. Erros de config/rede não
// derrubam o serviço — logam e viram heartbeat, e o próximo ciclo tenta de novo.
func (p *program) worklistCycle() {
	pay := map[string]any{"modo": modo(p.dryRun), "fonte": "worklist-api"}
	defer func() { postHeartbeat(p.ctx, p.apiURL, p.secret, p.name, pay) }()

	// empresas vazio = TODAS (paridade com o sweep); com allowlist, mapeia p/
	// CNPJ-base. empresas dadas mas sem CNPJ resolvido = misconfig → pula o ciclo.
	var roots []string
	if len(p.empresas) > 0 {
		r, err := p.sn.RootsForEmpresas(p.ctx, p.empresas)
		if err != nil {
			log.Printf("worklist: mapear empresas->CNPJ: %v", err)
			pay["error"] = err.Error()
			return
		}
		if len(r) == 0 {
			log.Printf("worklist: nenhuma filial com CNPJ p/ as empresas %v — ciclo pulado", p.empresas)
			pay["error"] = "sem CNPJ para a allowlist"
			return
		}
		roots = r
	}
	since := firstDayPrevMonth(time.Now())
	limit := envInt("TRACKER_SYNCER_MAX_SCAN", 5000)
	items, err := syncer.FetchWorklistAPI(p.ctx, p.apiURL, p.secret, roots, p.filialMax, since, limit)
	if err != nil {
		log.Printf("worklist fetch: %v", err)
		pay["error"] = err.Error()
		return
	}
	res, err := p.sn.RunWorklist(p.ctx, items)
	pay["fetched"] = res.Fetched
	pay["planejados"] = res.Planned
	pay["executados"] = res.Executed
	pay["erros"] = res.Errors
	pay["chave_divergente"] = res.Mismatch
	pay["sem_path"] = res.NoPath
	for k, v := range res.Skips {
		pay["skip_"+k] = v
	}
	if err != nil {
		log.Printf("worklist: %v", err)
		pay["error"] = err.Error()
	}
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
	rd.Log = log.Printf // avisos de reconexão/retry visíveis no syncer.log
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

	// Modo worklist como serviço (agent=olhos, syncer=mãos): cada ciclo busca a
	// lista de pendentes pela API em vez de varrer o FS. A allowlist e o teto de
	// filial vêm de env (o serviço roda sem flags). Validação da allowlist é no
	// worklistCycle (não fatal — não queremos crash-loop do serviço por misconfig).
	p.worklistMode = strings.EqualFold(os.Getenv("TRACKER_SYNCER_MODE"), "worklist")
	p.empresas = sortedKeys(parseEmpresas(os.Getenv("TRACKER_SYNCER_EMPRESAS")))
	p.filialMax = envInt("TRACKER_SYNCER_FILIAL_MAX", 0)
}

// syncerInserter espelha a interface não-exportada do pacote syncer — um wr nil
// tipado (*firebird.Writer)(nil) não pode virar interface não-nil por engano.
type syncerInserter interface {
	NextChaveID(ctx context.Context) (int64, error)
	InsertChaveAcesso(ctx context.Context, id int64, r firebird.InsertRow) error
	DeleteOurRows(ctx context.Context, chave, markerPrefix string) (int64, error)
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
	dryRunFlag := flag.Bool("dry-run", false, "força o dry-run (planeja e registra; nenhuma escrita)")
	chave := flag.String("chave", "", "sincroniza SÓ esta chave (44 dígitos; exige --file)")
	file := flag.String("file", "", "caminho do XML na ASINCRONIZAR (com --chave)")
	once := flag.Bool("once", false, "uma varredura e sai")
	allowStale := flag.Bool("allow-stale", false, "permite emissão fora da janela do AthenasHorse (mês atual+anterior)")
	rollbackChave := flag.String("rollback", "", "DESTRUTIVO: desfaz a sincronização desta chave (apaga NOSSAS linhas IMPORTADO=0, restaura o arquivo na ASINCRONIZAR); exige --yes")
	rollbackAll := flag.Bool("rollback-all", false, "DESTRUTIVO: rollback EM LOTE de TODAS as nossas linhas IMPORTADO=0; exige --yes")
	selftest := flag.Bool("selftest-rollback", false, "insere uma linha-isca sintética e a desfaz — testa INSERT+rollback na tabela real; exige --yes")
	worklist := flag.Bool("worklist", false, "lê a lista de pendentes de sync do tracker via PG (dado do agent) e sincroniza, SEM varrer o filesystem (exige TRACKER_PG_DSN + TRACKER_SYNCER_EMPRESAS)")
	worklistAPI := flag.Bool("worklist-api", false, "como --worklist, mas busca a lista pela API do tracker (POST HMAC) — caminho de PRODUÇÃO (a 5432 não é exposta do SRVIMPORT); exige TRACKER_API_URL + TRACKER_AGENT_SECRET + TRACKER_SYNCER_EMPRESAS")
	worklistFile := flag.String("worklist-file", "", "lê a worklist de um arquivo JSONL ({chave,file_path} por linha) e sincroniza — fonte quando o PG não é alcançável do syncer")
	filialMax := flag.Int("filial-max", 0, "com --worklist: limita a codigo_filial <= N (0 = todas)")
	audit := flag.Bool("audit", false, "READ-ONLY: conta as nossas linhas (marcador), split IMPORTADO=0/1")
	dump := flag.String("dump", "", "com --audit: grava manifesto JSONL de TODAS as nossas linhas nesse arquivo")
	since := flag.String("since", "", "com --audit: escopa por DATAINCLUSAO >= YYYY-MM-DD (usa índice; vazio = full scan da tabela)")
	yes := flag.Bool("yes", false, "confirma a operação destrutiva (--rollback / --rollback-all / --selftest-rollback)")
	flag.Parse()

	// Dry-run é o DEFAULT: o modo real exige TRACKER_SYNCER_DRY_RUN=false
	// EXPLÍCITO (além do ENABLED). Como serviço o processo roda sem flags —
	// a omissão tem de cair no modo inofensivo.
	dryRun := *dryRunFlag || getenv("TRACKER_SYNCER_DRY_RUN", "true") != "false"

	verb := flag.Arg(0)

	// --audit é READ-ONLY: roda ANTES da trava ENABLED e só precisa do DSN de
	// leitura (é a ferramenta de "quanto já inserimos", segura a qualquer hora).
	if *audit {
		runAudit(*dump, *since)
		return
	}

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

	// Rollback single-key (§10): única operação destrutiva. Sempre modo REAL
	// (precisa do writer p/ o DELETE); exige --yes além do ENABLED.
	if *rollbackChave != "" {
		if !*yes {
			log.Fatal("--rollback é destrutivo (apaga linhas e mexe em arquivos) — confirme com --yes")
		}
		prg.build(false, true) // dryRun=false força a conexão de escrita; allowStale irrelevante aqui
		defer prg.closeAll()
		res, err := prg.sn.Rollback(prg.ctx, *rollbackChave)
		if err != nil {
			log.Fatalf("rollback %s: %v", *rollbackChave, err)
		}
		log.Printf("rollback concluído: chave=%s linhas_apagadas=%d origem_restaurada=%d destinos_apagados=%d",
			res.Chave, res.RowsDeleted, res.FilesRestored, res.FilesDeleted)
		for _, w := range res.Warnings {
			log.Printf("  aviso: %s", w)
		}
		return
	}

	// Rollback EM LOTE (§14): desfaz TODAS as nossas linhas IMPORTADO=0. Modo
	// REAL (writer p/ o DELETE); exige --yes além do ENABLED.
	if *rollbackAll {
		if !*yes {
			log.Fatal("--rollback-all é destrutivo (apaga linhas e mexe em arquivos) — confirme com --yes")
		}
		prg.build(false, true)
		defer prg.closeAll()
		res, err := prg.sn.RollbackAll(prg.ctx)
		if err != nil {
			log.Fatalf("rollback-all: %v", err)
		}
		log.Printf("rollback-all concluído: desfeitas=%d linhas_apagadas=%d origens_restauradas=%d destinos_apagados=%d puladas=%d falhas=%d",
			res.Chaves, res.RowsDeleted, res.FilesRestored, res.FilesDeleted, res.Skipped, res.Failures)
		for _, w := range res.Warnings {
			log.Printf("  aviso: %s", w)
		}
		if res.Skipped > 0 {
			log.Printf("ATENÇÃO: %d chave(s) PULADA(S) (sem journal local ou já importada) — precisam de ação manual (ver avisos)", res.Skipped)
		}
		return
	}

	// Auto-teste do rollback (§14): insere e desfaz uma isca sintética na tabela
	// real ANTES do 1º sync de verdade. Modo REAL; exige --yes.
	if *selftest {
		if !*yes {
			log.Fatal("--selftest-rollback insere e apaga uma linha real — confirme com --yes")
		}
		prg.build(false, true)
		defer prg.closeAll()
		res, err := prg.sn.SelfTestRollback(prg.ctx)
		if err != nil {
			log.Fatalf("selftest-rollback: %v", err)
		}
		log.Printf("selftest: chave=%s empresa=%d/%d inserida=%v apagadas=%d total_antes=%d total_depois=%d",
			res.Chave, res.Empresa, res.Filial, res.Inserted, res.RowsDeleted, res.TotalBefore, res.TotalAfter)
		if !res.OK {
			log.Fatal("SELFTEST FALHOU — o rollback NÃO se comportou como esperado; NÃO prossiga para o sync real")
		}
		log.Print("SELFTEST OK — INSERT + rollback verificados na tabela real; a contagem geral não mudou")
		return
	}

	// Gatilho single-key (piloto F2): roda uma vez e sai, sem loop de serviço.
	if *chave != "" {
		prg.build(dryRun, *allowStale)
		defer prg.closeAll()
		plan, err := prg.sn.RunChave(prg.ctx, *chave, *file)
		if err != nil {
			log.Fatalf("chave %s: %v", *chave, err)
		}
		if dryRun {
			log.Printf("dry-run concluído — plano com %d participação(ões) registrado; NADA foi escrito", len(plan.Participacoes))
		} else {
			log.Printf("sincronização da chave %s concluída (%d participação(ões))", *chave, len(plan.Participacoes))
		}
		return
	}

	// Worklist a partir de ARQUIVO (JSONL gerado por query no tracker e copiado
	// pra cá): usado quando o PG não é alcançável do syncer. dry-run ou real.
	if *worklistFile != "" {
		prg.build(dryRun, *allowStale)
		defer prg.closeAll()
		items, err := syncer.LoadWorklistFile(*worklistFile)
		if err != nil {
			log.Fatalf("worklist-file: %v", err)
		}
		log.Printf("worklist-file: %d item(ns) lido(s) de %s", len(items), *worklistFile)
		res, err := prg.sn.RunWorklist(prg.ctx, items)
		if err != nil {
			log.Fatalf("worklist: %v", err)
		}
		log.Printf("worklist concluída: fetched=%d planejados=%d executados=%d chave_divergente=%d sem_path=%d erros=%d",
			res.Fetched, res.Planned, res.Executed, res.Mismatch, res.NoPath, res.Errors)
		return
	}

	// Worklist (agent = olhos, syncer = mãos): lê do tracker as pendentes de sync
	// e sincroniza SEM varrer o filesystem. dry-run ou real. Roda uma vez e sai.
	if *worklist {
		empresas := sortedKeys(parseEmpresas(os.Getenv("TRACKER_SYNCER_EMPRESAS")))
		if len(empresas) == 0 {
			log.Fatal("--worklist exige TRACKER_SYNCER_EMPRESAS (allowlist) — não sincronizamos tudo sem cerca")
		}
		pgDSN := mustEnv("TRACKER_PG_DSN")
		prg.build(dryRun, *allowStale)
		defer prg.closeAll()
		// A allowlist vem em codigo_empresa, mas o filtro correto é por CNPJ
		// (notas.codigo_empresa é poluída pelo fan-out do SIEG): converte aqui.
		roots, err := prg.sn.RootsForEmpresas(prg.ctx, empresas)
		if err != nil {
			log.Fatalf("worklist: mapear empresas->CNPJ: %v", err)
		}
		if len(roots) == 0 {
			log.Fatalf("worklist: nenhuma filial com CNPJ encontrada p/ as empresas %v", empresas)
		}
		since := firstDayPrevMonth(time.Now())
		limit := envInt("TRACKER_SYNCER_MAX_SCAN", 5000)
		items, err := syncer.FetchWorklist(prg.ctx, pgDSN, roots, *filialMax, since, limit)
		if err != nil {
			log.Fatalf("worklist fetch: %v", err)
		}
		log.Printf("worklist: %d nota(s) pendente(s) de sync no tracker (empresas=%v cnpj-base=%v filial<=%d emissão>=%s)",
			len(items), empresas, roots, *filialMax, since.Format("2006-01-02"))
		res, err := prg.sn.RunWorklist(prg.ctx, items)
		if err != nil {
			log.Fatalf("worklist: %v", err)
		}
		log.Printf("worklist concluída: fetched=%d planejados=%d executados=%d chave_divergente=%d sem_path=%d erros=%d",
			res.Fetched, res.Planned, res.Executed, res.Mismatch, res.NoPath, res.Errors)
		return
	}

	// Worklist via API (produção): one-shot do MESMO worklistCycle que o serviço
	// roda em loop (build lê allowlist/filial-max do env; --filial-max sobrepõe).
	if *worklistAPI {
		prg.build(dryRun, *allowStale)
		defer prg.closeAll()
		if *filialMax > 0 {
			prg.filialMax = *filialMax
		}
		prg.worklistCycle() // empresas vazio = todas
		return
	}

	prg.build(dryRun, *allowStale)
	if *once {
		defer prg.closeAll()
		prg.cycle()
		return
	}
	if err := svc.Run(); err != nil {
		log.Fatalf("run: %v", err)
	}
}

// runAudit abre SÓ a conexão de leitura (TRACKER_FB_DSN), conta as nossas linhas
// e, com --dump, grava o manifesto. Nenhuma escrita — não exige ENABLED nem o
// TRACKER_FB_WRITE_DSN, então roda em qualquer máquina com o DSN de leitura.
// Sem --since a query varre a tabela inteira (OBSERVACOES não é indexado) — passe
// --since em horário de expediente; nossas linhas só existem do F2 em diante.
func runAudit(dumpPath, since string) {
	var sinceT time.Time
	if since != "" {
		t, err := time.Parse("2006-01-02", since)
		if err != nil {
			log.Fatalf("--since inválido (use YYYY-MM-DD): %v", err)
		}
		sinceT = t
	} else {
		log.Print("AVISO: --audit sem --since varre a TABLISTACHAVEACESSO inteira (tabela grande) — rode em horário calmo ou passe --since YYYY-MM-DD")
	}
	ctx := context.Background()
	rd, err := firebird.NewReader(ctx, mustEnv("TRACKER_FB_DSN"))
	if err != nil {
		log.Fatalf("firebird (ro): %v", err)
	}
	defer rd.Close()
	if _, err := syncer.AuditRows(ctx, rd, dumpPath, sinceT, log.Printf); err != nil {
		log.Fatalf("audit: %v", err)
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

// sortedKeys extrai as chaves de um set em ordem (allowlist -> slice p/ a query).
func sortedKeys(m map[int]bool) []int {
	out := make([]int, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Ints(out)
	return out
}

// firstDayPrevMonth: 1º dia do mês ANTERIOR (janela do AthenasHorse = atual+anterior).
// Piso do fetch da worklist p/ não trazer backlog stale (que o PlanFile pularia).
func firstDayPrevMonth(t time.Time) time.Time {
	y, m := t.Year(), t.Month()
	return time.Date(y, m, 1, 0, 0, 0, 0, t.Location()).AddDate(0, -1, 0)
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
