package syncer

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/EnzzoHosaki/rps-xml-tracker/internal/model"
	"github.com/EnzzoHosaki/rps-xml-tracker/internal/signing"
	"github.com/EnzzoHosaki/rps-xml-tracker/internal/store"
)

// WorklistItem é UMA nota pendente de sync — o contrato vive em model (partilhado
// com store e API). Alias p/ o pacote syncer continuar usando o nome curto.
type WorklistItem = model.WorklistItem

// FetchWorklist lê do Postgres do tracker as notas pendentes de sync via
// store.Worklist (dono único da SQL). Filtra por CNPJ-base (roots), não por
// codigo_empresa. É o caminho PG-direto, usado quando a 5432 é alcançável (dev);
// em produção o syncer no SRVIMPORT usa FetchWorklistAPI (a 5432 não é exposta).
func FetchWorklist(ctx context.Context, pgDSN string, roots []string, filialMax int, since time.Time, limit int) ([]WorklistItem, error) {
	// roots vazio = todas as empresas (paridade com o sweep); store.Worklist trata.
	pg, err := store.NewPostgres(ctx, pgDSN)
	if err != nil {
		return nil, fmt.Errorf("conectar no tracker (pg): %w", err)
	}
	defer pg.Close()
	return pg.Worklist(ctx, model.WorklistQuery{Roots: roots, FilialMax: filialMax, Since: since, Limit: limit})
}

// FetchWorklistAPI busca a worklist pela API do tracker (POST HMAC), o caminho de
// produção: o syncer no SRVIMPORT não alcança o Postgres (só a 8090). Assina o
// body cru com o MESMO segredo do agente (agentHMAC), igual ao heartbeat.
func FetchWorklistAPI(ctx context.Context, apiURL, secret string, roots []string, filialMax int, since time.Time, limit int) ([]WorklistItem, error) {
	// roots vazio = todas as empresas (paridade com o sweep). Garante array (não
	// null) no JSON p/ o contrato ser estável.
	if roots == nil {
		roots = []string{}
	}
	body, err := json.Marshal(map[string]any{
		"roots":      roots,
		"filial_max": filialMax,
		"since":      since.Format("2006-01-02"),
		"limit":      limit,
	})
	if err != nil {
		return nil, err
	}
	url := strings.TrimRight(apiURL, "/") + "/api/v1/ingest/worklist"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Agent-Signature", signing.Sign(secret, body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("chamar worklist API: %w", err)
	}
	defer resp.Body.Close()
	rb, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<20)) // até 64 MiB de itens
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("worklist API status %d: %s", resp.StatusCode, strings.TrimSpace(string(rb)))
	}
	var out struct {
		Items []WorklistItem `json:"items"`
	}
	if err := json.Unmarshal(rb, &out); err != nil {
		return nil, fmt.Errorf("decodificar worklist API: %w", err)
	}
	return out.Items, nil
}

// RootsForEmpresas mapeia a allowlist de codigo_empresa para os CNPJ-base (8
// primeiros dígitos) das filiais dessas empresas, lidos do Firebird (s.filiais,
// autoritativo). É o insumo do filtro da FetchWorklist: convertemos "quais
// empresas" (código) em "quais CNPJ" (identidade real), contornando o fan-out do
// SIEG que polui notas.codigo_empresa. Carrega as filiais se ainda não estiverem
// em cache. Retorna a lista ordenada e sem duplicatas.
func (s *Syncer) RootsForEmpresas(ctx context.Context, empresas []int) ([]string, error) {
	if len(s.filiais) == 0 {
		if err := s.refreshResolve(ctx); err != nil {
			return nil, err
		}
	}
	allow := make(map[int]bool, len(empresas))
	for _, e := range empresas {
		allow[e] = true
	}
	set := map[string]bool{}
	for _, f := range s.filiais {
		if !allow[f.CodigoEmpresa] {
			continue
		}
		if d := digits(f.Cnpj); len(d) >= 8 {
			set[d[:8]] = true
		}
	}
	out := make([]string, 0, len(set))
	for r := range set {
		out = append(out, r)
	}
	sort.Strings(out)
	return out, nil
}

// LoadWorklistFile lê uma worklist de um arquivo JSONL (um objeto por linha, com
// pelo menos "chave" e "file_path"). É a fonte usada quando o Postgres do tracker
// não é alcançável da máquina do syncer (o deploy não expõe a 5432): a lista é
// gerada por uma query no SRVRPS03 e copiada pra cá. Linhas vazias são ignoradas.
func LoadWorklistFile(path string) ([]WorklistItem, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []WorklistItem
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024) // linhas de path UNC podem ser longas
	ln := 0
	for sc.Scan() {
		ln++
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var it WorklistItem
		if err := json.Unmarshal([]byte(line), &it); err != nil {
			return nil, fmt.Errorf("linha %d inválida: %w", ln, err)
		}
		if it.Chave == "" || it.FilePath == "" {
			return nil, fmt.Errorf("linha %d: chave e file_path são obrigatórios", ln)
		}
		out = append(out, it)
	}
	return out, sc.Err()
}

// WorklistResult resume um ciclo de worklist.
type WorklistResult struct {
	Fetched  int            // itens recebidos do tracker
	Planned  int            // planos válidos (dry-run) / execuções iniciadas
	Executed int            // sincronizações completas (modo real)
	Errors   int            // falhas de execução
	Mismatch int            // file_path apontava para OUTRA chave (arquivo movido/stale)
	NoPath   int            // item sem file_path na observação de chegada
	Skips    map[string]int // motivo (classe) -> quantos
}

// RunWorklist processa uma worklist: refreshResolve UMA vez e, por item, planeja
// (PlanFile com allowlist) e — no modo real — executa. Mesma lógica do handleFile
// da varredura, mas dirigida pela lista do tracker em vez do walk do filesystem;
// confere a chave do arquivo contra a esperada (proteção contra file_path stale).
// Respeita MaxPerCycle (execuções reais por ciclo).
func (s *Syncer) RunWorklist(ctx context.Context, items []WorklistItem) (WorklistResult, error) {
	res := WorklistResult{Fetched: len(items), Skips: map[string]int{}}
	if err := s.refreshResolve(ctx); err != nil {
		return res, err
	}
	for _, it := range items {
		if it.FilePath == "" {
			res.NoPath++
			continue
		}
		// arrived∧¬synced superconta: o DownloadXML movia o arquivo sem avisar o
		// tracker, então o file_path pode apontar p/ algo que já sumiu da origem.
		// Classe própria: NÃO é "parse falhou" (XML corrompido) — é benigno.
		if _, err := os.Stat(it.FilePath); os.IsNotExist(err) {
			res.Skips["arquivo_sumiu"]++
			continue
		}
		plan := s.PlanFile(ctx, it.FilePath, true)
		if plan.Chave != "" && (s.jr.isDone(plan.Chave) || (s.cfg.DryRun && s.jr.isDryPlanned(plan.Chave))) {
			res.Skips["ja_processada"]++
			continue
		}
		if plan.Skip != "" {
			res.Skips[skipClass(plan.Skip)]++
			continue
		}
		if plan.Chave != it.Chave {
			res.Mismatch++
			s.cfg.Log("worklist: arquivo %s tem chave %s, esperava %s — pulado", it.FilePath, plan.Chave, it.Chave)
			continue
		}
		res.Planned++
		s.logPlan(plan)
		if s.cfg.DryRun {
			_ = s.jr.markDryPlanned(plan.Chave)
			s.recordPlan(plan)
			continue
		}
		if err := s.Execute(ctx, plan); err != nil {
			res.Errors++
			s.cfg.Log("worklist execução %s: %v", plan.Chave, err)
			continue
		}
		res.Executed++
		if s.cfg.MaxPerCycle > 0 && res.Executed >= s.cfg.MaxPerCycle {
			s.cfg.Log("worklist: atingiu MAX_PER_CYCLE=%d — para o ciclo", s.cfg.MaxPerCycle)
			break
		}
	}
	skipped := 0
	for _, n := range res.Skips {
		skipped += n
	}
	s.cfg.Log("WORKLIST fetched=%d planejados=%d executados=%d pulados=%d sem_path=%d chave_divergente=%d erros=%d skips=%v",
		res.Fetched, res.Planned, res.Executed, skipped, res.NoPath, res.Mismatch, res.Errors, res.Skips)
	return res, nil
}
