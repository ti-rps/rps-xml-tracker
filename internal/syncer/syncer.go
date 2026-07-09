// Package syncer implementa o shadow-sync (F1): assumir do DownloadXML a
// sincronização ASINCRONIZAR → SINCRONIZADO + INSERT na TABLISTACHAVEACESSO com
// IMPORTADO=0 (design/SHADOW-SYNC.md §2). A unidade de trabalho é a PARTICIPAÇÃO
// (chave, empresa, filial) — uma nota entre dois clientes gera uma CÓPIA e uma
// LINHA por participante (M0), e a origem só é removida quando TODAS completam.
//
// A validação do piloto é por triangulação: cada efeito do syncer é confirmado
// por um componente que não sabe que ele existe — o AGENTE vê o arquivo aparecer
// no SINCRONIZADO (file_moved) e o POLLER vê a linha IMPORTADO=0 (seen_pending).
package syncer

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/EnzzoHosaki/rps-xml-tracker/internal/firebird"
	"github.com/EnzzoHosaki/rps-xml-tracker/internal/model"
	"github.com/EnzzoHosaki/rps-xml-tracker/internal/syncpath"
	"github.com/EnzzoHosaki/rps-xml-tracker/internal/xmlparse"
)

// resolver é a visão READ-ONLY do Firebird que o planejamento precisa
// (interface p/ testes; implementada por *firebird.Reader).
type resolver interface {
	ListFiliais(ctx context.Context) ([]firebird.Filial, error)
	EmpresaNomes(ctx context.Context) (map[int]string, error)
	HasRow(ctx context.Context, chave string, codigoEmpresa, codigoFilial int) (bool, error)
}

// inserter é a escrita no Firebird (implementada por *firebird.Writer; nil no dry-run).
type inserter interface {
	NextChaveID(ctx context.Context) (int64, error)
	InsertChaveAcesso(ctx context.Context, id int64, r firebird.InsertRow) error
}

// submitter envia observações ao tracker (implementada por *ingest.Client).
type submitter interface {
	Submit(ctx context.Context, batch []model.Observation) error
}

// Config liga o syncer. Tudo validado em New.
type Config struct {
	Name        string       // nome do serviço nas observações (source "syncer:<name>")
	ArrivalRoot string       // raiz da ASINCRONIZAR
	SyncRoot    string       // raiz do SINCRONIZADO
	JournalPath string       // bbolt
	PlansPath   string       // JSONL com os planos do dry-run ("" = não grava)
	DryRun      bool         // executa só o planejamento e registra o plano
	AllowStale  bool         // permite emissão fora da janela do AthenasHorse (mês atual+anterior)
	MaxPerCycle int          // quantos planos/execuções por varredura (piloto: 1)
	MaxScanPer  int          // teto de arquivos examinados por varredura (limita o custo do backlog)
	Empresas    map[int]bool // allowlist de CODIGOEMPRESA (varredura); vazia = allowlist por Dirs
	Dirs        []string     // allowlist de subpastas da ASINCRONIZAR (varredura)
	Marker      string       // OBSERVACOES gravada no INSERT (autoria/rollback)
	Now         func() time.Time
	Log         func(format string, args ...any)
}

// Syncer orquestra plano+execução. O cache de resolução (filiais/nomes) é
// recarregado a cada varredura.
type Syncer struct {
	cfg Config
	rd  resolver
	wr  inserter
	sub submitter
	jr  *journal

	filiais []firebird.Filial
	nomes   map[int]string
}

func New(cfg Config, rd resolver, wr inserter, sub submitter) (*Syncer, error) {
	if cfg.ArrivalRoot == "" || cfg.SyncRoot == "" {
		return nil, fmt.Errorf("syncer: ArrivalRoot e SyncRoot são obrigatórios")
	}
	if cfg.Marker == "" {
		return nil, fmt.Errorf("syncer: Marker (OBSERVACOES) é obrigatório — sem ele não há rollback limpo")
	}
	if !cfg.DryRun && wr == nil {
		return nil, fmt.Errorf("syncer: modo real exige a conexão de escrita (TRACKER_FB_WRITE_DSN)")
	}
	if cfg.MaxPerCycle <= 0 {
		cfg.MaxPerCycle = 1
	}
	if cfg.MaxScanPer <= 0 {
		cfg.MaxScanPer = 5000
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Log == nil {
		cfg.Log = func(string, ...any) {}
	}
	jr, err := openJournal(cfg.JournalPath)
	if err != nil {
		return nil, fmt.Errorf("syncer: journal: %w", err)
	}
	return &Syncer{cfg: cfg, rd: rd, wr: wr, sub: sub, jr: jr}, nil
}

func (s *Syncer) Close() error { return s.jr.Close() }

// Participacao é UMA cópia+INSERT planejados (empresa/filial cliente).
type Participacao struct {
	CodigoEmpresa int    `json:"codigo_empresa"`
	CodigoFilial  int    `json:"codigo_filial"`
	NomeEmpresa   string `json:"nome_empresa"`
	CnpjFilial    string `json:"cnpj_filial"`
	Direction     string `json:"direction"`
	DestRel       string `json:"dest_rel"` // URL relativa (vai na coluna URL)
	DestAbs       string `json:"dest_abs"` // caminho absoluto no SyncRoot
}

// Plan é o resultado do planejamento de UM arquivo (passos 0-4 do fluxo).
// Skip != "" significa "não sincronizar" com o motivo — nunca um erro fatal.
type Plan struct {
	Origem        string         `json:"origem"`
	Chave         string         `json:"chave"`
	DocType       model.DocType  `json:"doc_type"`
	DataEmissao   string         `json:"data_emissao"`
	Participacoes []Participacao `json:"participacoes,omitempty"`
	Skip          string         `json:"skip,omitempty"`
	PlannedAt     time.Time      `json:"planned_at"`
	parse         xmlparse.Result
}

// pilotDocTypes: o piloto cobre só NFe/NFCe "normais" (F0: NFSe tem chave de 50
// dígitos e PRESTADO/TOMADO; eventos/CTeOS/BPe têm padrões próprios).
var pilotDocTypes = map[model.DocType]bool{model.DocNFe: true, model.DocNFCe: true}

// refreshResolve recarrega filiais e nomes de empresa (tabelas pequenas).
func (s *Syncer) refreshResolve(ctx context.Context) error {
	filiais, err := s.rd.ListFiliais(ctx)
	if err != nil {
		return fmt.Errorf("listar filiais: %w", err)
	}
	nomes, err := s.rd.EmpresaNomes(ctx)
	if err != nil {
		return fmt.Errorf("nomes de empresa: %w", err)
	}
	s.filiais, s.nomes = filiais, nomes
	return nil
}

// PlanFile roda os passos 0-4 para um arquivo: parse → janela → resolve
// participações (match EXATO do CNPJ/CPF da filial com emitente/destinatário) →
// deriva o destino de cada uma. enforceAllowlist=true (varredura) descarta o
// arquivo inteiro se QUALQUER participação estiver fora da allowlist — no
// multi-participação não se sincroniza pela metade.
func (s *Syncer) PlanFile(ctx context.Context, path string, enforceAllowlist bool) Plan {
	plan := Plan{Origem: path, PlannedAt: s.cfg.Now()}
	res, err := xmlparse.ParseFile(path)
	if err != nil {
		plan.Skip = "parse falhou: " + err.Error()
		return plan
	}
	plan.parse = res
	plan.Chave, plan.DocType, plan.DataEmissao = res.Chave, res.DocType, res.DataEmissao
	if res.Chave == "" {
		plan.Skip = "sem chave no XML"
		return plan
	}
	if !pilotDocTypes[res.DocType] {
		plan.Skip = "doc_type fora do piloto: " + string(res.DocType)
		return plan
	}
	if res.DataEmissao == "" {
		plan.Skip = "sem data de emissão"
		return plan
	}
	if !s.cfg.AllowStale && !dentroDaJanela(res.DataEmissao, s.cfg.Now()) {
		// F0: o AthenasHorse só importa emissão do mês atual/anterior — sincronizar
		// fora da janela cria linha IMPORTADO=0 eterna (lixo no banco).
		plan.Skip = "emissão fora da janela do AthenasHorse (use --allow-stale para forçar)"
		return plan
	}

	emitD, destD := digits(res.CnpjEmitente), digits(res.CnpjDestinatario)
	for _, f := range s.filiais {
		fd := digits(f.Cnpj)
		if len(fd) != 14 && len(fd) != 11 {
			continue
		}
		var dir string
		switch fd {
		case emitD:
			dir = model.DirSaida
		case destD:
			dir = model.DirEntrada
		default:
			continue
		}
		nome := s.nomes[f.CodigoEmpresa]
		if nome == "" {
			plan.Skip = fmt.Sprintf("empresa %d sem nome na TABEMPRESAS", f.CodigoEmpresa)
			return plan
		}
		rel, err := syncpath.Derive(syncpath.Input{
			NomeEmpresa: nome, CnpjFilial: fd, DocType: res.DocType,
			Direction: dir, DataEmissao: res.DataEmissao, Chave: res.Chave,
		})
		if err != nil {
			plan.Skip = "derivação do destino falhou: " + err.Error()
			return plan
		}
		plan.Participacoes = append(plan.Participacoes, Participacao{
			CodigoEmpresa: f.CodigoEmpresa, CodigoFilial: f.CodigoFilial,
			NomeEmpresa: nome, CnpjFilial: fd, Direction: dir,
			DestRel: rel, DestAbs: relToAbs(s.cfg.SyncRoot, rel),
		})
	}
	if len(plan.Participacoes) == 0 {
		plan.Skip = "nenhuma filial cliente com CNPJ/CPF exato (emitente/destinatário)"
		return plan
	}
	if enforceAllowlist && len(s.cfg.Empresas) > 0 {
		for _, p := range plan.Participacoes {
			if !s.cfg.Empresas[p.CodigoEmpresa] {
				plan.Skip = fmt.Sprintf("empresa %d fora da allowlist", p.CodigoEmpresa)
				return plan
			}
		}
	}
	return plan
}

// SweepResult é o resumo de uma varredura (vai no heartbeat).
type SweepResult struct {
	Scanned  int            // arquivos examinados
	Planned  int            // planos produzidos (dry-run) / execuções iniciadas
	Executed int            // execuções completas (modo real)
	Skips    map[string]int // motivo (classe) -> quantos
	Errors   int
}

// SweepOnce varre a ASINCRONIZAR (subpastas da allowlist Dirs, ou a raiz toda)
// e planeja/executa até MaxPerCycle arquivos. Exige allowlist não-vazia
// (Empresas e/ou Dirs) — varredura sem cerca não existe no piloto.
func (s *Syncer) SweepOnce(ctx context.Context) (SweepResult, error) {
	res := SweepResult{Skips: map[string]int{}}
	if len(s.cfg.Empresas) == 0 && len(s.cfg.Dirs) == 0 {
		return res, fmt.Errorf("varredura exige allowlist (TRACKER_SYNCER_EMPRESAS e/ou TRACKER_SYNCER_DIRS)")
	}
	if err := s.refreshResolve(ctx); err != nil {
		return res, err
	}
	roots := []string{s.cfg.ArrivalRoot}
	if len(s.cfg.Dirs) > 0 {
		roots = roots[:0]
		for _, d := range s.cfg.Dirs {
			roots = append(roots, filepath.Join(s.cfg.ArrivalRoot, d))
		}
	}
	for _, root := range roots {
		err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return nil // subpasta inacessível não derruba a varredura
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if d.IsDir() || !strings.EqualFold(filepath.Ext(path), ".xml") {
				return nil
			}
			if res.Scanned >= s.cfg.MaxScanPer || res.Planned >= s.cfg.MaxPerCycle {
				return fs.SkipAll
			}
			res.Scanned++
			s.handleFile(ctx, path, &res)
			return nil
		})
		if err != nil && err != fs.SkipAll {
			return res, err
		}
	}
	return res, nil
}

// handleFile planeja (e no modo real executa) um arquivo da varredura.
func (s *Syncer) handleFile(ctx context.Context, path string, res *SweepResult) {
	plan := s.PlanFile(ctx, path, true)
	if plan.Chave != "" && (s.jr.isDone(plan.Chave) || (s.cfg.DryRun && s.jr.isDryPlanned(plan.Chave))) {
		res.Skips["ja_processada"]++
		return
	}
	if plan.Skip != "" {
		res.Skips[skipClass(plan.Skip)]++
		return
	}
	res.Planned++
	s.logPlan(plan)
	if s.cfg.DryRun {
		_ = s.jr.markDryPlanned(plan.Chave)
		s.recordPlan(plan)
		return
	}
	if err := s.Execute(ctx, plan); err != nil {
		res.Errors++
		s.cfg.Log("execução %s: %v", plan.Chave, err)
		return
	}
	res.Executed++
}

// logPlan imprime o plano completo (uma linha por participação + o INSERT).
func (s *Syncer) logPlan(p Plan) {
	s.cfg.Log("PLANO chave=%s doc=%s emissão=%s origem=%s participações=%d",
		p.Chave, p.DocType, p.DataEmissao, p.Origem, len(p.Participacoes))
	for _, part := range p.Participacoes {
		row := s.insertRowFor(p, part)
		s.cfg.Log("  -> emp %d/%d (%s, %s) dest=%s", part.CodigoEmpresa, part.CodigoFilial,
			part.NomeEmpresa, part.Direction, part.DestRel)
		s.cfg.Log("     INSERT: emitente=%q dest=%q valor=%v serie+numero da chave, DATA=1º dia %s, OBSERVACOES=%q",
			row.Emitente, row.Destinatario, res2str(row.ValorTotal), p.DataEmissao[:7], row.Observacoes)
	}
}

// recordPlan grava o plano no JSONL do dry-run (diff posterior contra o que o
// DownloadXML fizer de verdade — via repoll --check-path).
func (s *Syncer) recordPlan(p Plan) {
	if s.cfg.PlansPath == "" {
		return
	}
	f, err := os.OpenFile(s.cfg.PlansPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		s.cfg.Log("plans.jsonl: %v", err)
		return
	}
	defer f.Close()
	b, _ := json.Marshal(p)
	_, _ = f.Write(append(b, '\n'))
}

// insertRowFor monta a linha do INSERT de uma participação (compartilhado entre
// o log do dry-run e a execução real — o que se imprime é o que se grava).
func (s *Syncer) insertRowFor(p Plan, part Participacao) firebird.InsertRow {
	emissao, _ := time.Parse("2006-01-02", p.DataEmissao)
	var valor *float64
	if v, err := strconv.ParseFloat(strings.TrimSpace(p.parse.ValorTotal), 64); err == nil {
		valor = &v
	}
	return firebird.InsertRow{
		Chave:            p.Chave,
		CodigoEmpresa:    part.CodigoEmpresa,
		CodigoFilial:     part.CodigoFilial,
		CnpjEmitente:     digits(p.parse.CnpjEmitente),
		CnpjDestinatario: digits(p.parse.CnpjDestinatario),
		Emitente:         p.parse.NomeEmitente,
		Destinatario:     p.parse.NomeDestinatario,
		DataEmissao:      emissao,
		ValorTotal:       valor,
		URL:              part.DestRel,
		CaminhoOriginal:  p.Origem,
		Observacoes:      s.cfg.Marker,
	}
}

// dentroDaJanela reporta se a emissão está no mês atual ou anterior (a janela
// que o AthenasHorse importa — regra levantada com o fiscal e batida na F0).
func dentroDaJanela(emissao string, now time.Time) bool {
	t, err := time.Parse("2006-01-02", emissao)
	if err != nil {
		return false
	}
	meses := (now.Year()*12 + int(now.Month())) - (t.Year()*12 + int(t.Month()))
	return meses == 0 || meses == 1
}

// relToAbs converte a URL relativa (separador '\', prefixo '\') num caminho
// absoluto sob root, com o separador do SO (testável fora do Windows).
func relToAbs(root, rel string) string {
	parts := strings.Split(strings.TrimPrefix(rel, `\`), `\`)
	return filepath.Join(append([]string{root}, parts...)...)
}

// skipClass agrupa motivos de skip em classes estáveis p/ o heartbeat.
func skipClass(reason string) string {
	switch {
	case strings.HasPrefix(reason, "parse"):
		return "parse_falhou"
	case strings.HasPrefix(reason, "sem chave"):
		return "sem_chave"
	case strings.HasPrefix(reason, "doc_type"):
		return "doc_type_fora_do_piloto"
	case strings.HasPrefix(reason, "emissão fora"):
		return "fora_da_janela"
	case strings.HasPrefix(reason, "nenhuma filial"):
		return "sem_filial_cliente"
	case strings.HasPrefix(reason, "empresa") && strings.Contains(reason, "allowlist"):
		return "fora_da_allowlist"
	default:
		return "outros"
	}
}

func digits(v string) string {
	var b strings.Builder
	for _, r := range v {
		if r >= '0' && r <= '9' {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func res2str(v *float64) string {
	if v == nil {
		return "∅"
	}
	return strconv.FormatFloat(*v, 'f', 2, 64)
}
