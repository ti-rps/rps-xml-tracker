package syncer

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// WorklistItem é UMA nota que o AGENT já viu chegar e que ainda NÃO foi
// sincronizada (arrived_at ∧ ¬synced_at no tracker) — a "lista de separação" que
// o syncer executa SEM varrer o filesystem (o agent já varreu, esse é o ponto).
// file_path vem da observação de chegada gravada pelo agent.
type WorklistItem struct {
	Chave         string `json:"chave"`
	FilePath      string `json:"file_path"`
	CodigoEmpresa int    `json:"codigo_empresa"`
	CodigoFilial  int    `json:"codigo_filial"`
	DataEmissao   string `json:"data_emissao"`
}

// FetchWorklist lê do Postgres do tracker as notas pendentes de sync (arrived,
// não synced) para a allowlist de empresas — opcionalmente limitando a filial
// (filialMax>0) e a emissão (since). O file_path vem da última observação de
// chegada (stage=arrival). READ-ONLY. É o oposto do SweepOnce: nada de andar no
// A_SINCRONIZAR; o agent já fez isso e gravou aqui, então não repetimos o scan.
func FetchWorklist(ctx context.Context, pgDSN string, empresas []int, filialMax int, since time.Time, limit int) ([]WorklistItem, error) {
	if len(empresas) == 0 {
		return nil, fmt.Errorf("worklist exige allowlist de empresas (não varremos tudo)")
	}
	if limit <= 0 {
		limit = 100000
	}
	pool, err := pgxpool.New(ctx, pgDSN)
	if err != nil {
		return nil, fmt.Errorf("conectar no tracker (pg): %w", err)
	}
	defer pool.Close()

	const q = `
		SELECT n.chave_acesso, n.codigo_empresa, n.codigo_filial, n.data_emissao,
		       (SELECT o.file_path FROM observations o
		         WHERE o.chave_acesso = n.chave_acesso
		           AND o.stage = 'arrival'::stage AND o.file_path IS NOT NULL
		         ORDER BY o.observed_at DESC LIMIT 1) AS file_path
		FROM notas n
		WHERE n.arrived_at IS NOT NULL AND n.synced_at IS NULL
		  AND n.codigo_empresa = ANY($1)
		  AND ($2 = 0 OR n.codigo_filial <= $2)
		  AND n.data_emissao >= $3
		ORDER BY n.data_emissao, n.chave_acesso
		LIMIT $4`
	rows, err := pool.Query(ctx, q, empresas, filialMax, since, limit)
	if err != nil {
		return nil, fmt.Errorf("query worklist: %w", err)
	}
	defer rows.Close()
	var out []WorklistItem
	for rows.Next() {
		var it WorklistItem
		var emp, fil *int
		var de *time.Time
		var fp *string
		if err := rows.Scan(&it.Chave, &emp, &fil, &de, &fp); err != nil {
			return nil, err
		}
		if emp != nil {
			it.CodigoEmpresa = *emp
		}
		if fil != nil {
			it.CodigoFilial = *fil
		}
		if de != nil {
			it.DataEmissao = de.Format("2006-01-02")
		}
		if fp != nil {
			it.FilePath = *fp
		}
		out = append(out, it)
	}
	return out, rows.Err()
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
	s.cfg.Log("WORKLIST fetched=%d planejados=%d executados=%d pulados=%d sem_path=%d chave_divergente=%d erros=%d",
		res.Fetched, res.Planned, res.Executed, skipped, res.NoPath, res.Mismatch, res.Errors)
	return res, nil
}
