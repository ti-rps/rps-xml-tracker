package syncer

import (
	"context"
	"fmt"
	"time"

	"github.com/EnzzoHosaki/rps-xml-tracker/internal/firebird"
)

// selfTestChave é a chave-isca do --selftest-rollback: 44 NOVES. Começa em "99"
// (código de UF inexistente), então JAMAIS coincide com uma NF-e/NFC-e real e o
// AthenasHorse nunca a importa (não está no livro) — a linha fica IMPORTADO=0 e é
// apagada pelo próprio teste. Só dígitos: InsertChaveAcesso deriva SERIE/NÚMERO
// das posições da chave.
const selfTestChave = "99999999999999999999999999999999999999999999"

// SelfTestResult resume o auto-teste do rollback (§14 item ④).
type SelfTestResult struct {
	Chave       string
	Empresa     int
	Filial      int
	TotalBefore int64
	Inserted    bool
	RowsDeleted int64
	TotalAfter  int64
	OK          bool // inserida, apagada, ausente ao fim E contagem TOTAL intacta
}

// SelfTestRollback exercita, na tabela REAL, o caminho INSERT → rollback antes do
// 1º sync de verdade: (1) insere UMA linha-isca sintética (marcador nosso,
// IMPORTADO=0); (2) confirma que apareceu; (3) roda o DELETE-por-marcador do
// rollback; (4) confirma que sumiu E que a contagem TOTAL das nossas linhas
// voltou ao ponto de partida (prova que o filtro só pegou a isca, nunca linha de
// terceiro). Não mexe em arquivo nem emite timeline. Rode com o SERVIÇO PARADO —
// senão um INSERT concorrente do próprio syncer moveria a contagem.
//
// O gate compara o TOTAL, não os pendentes: o AthenasHorse é independente (o
// operador não o para) e pode importar uma linha nossa no meio do teste
// (IMPORTADO 0→1) — isso mexe no split pendente/importada mas NÃO no total, então
// o robô não gera falso negativo que travaria o go-live (achado do code-review).
func (s *Syncer) SelfTestRollback(ctx context.Context) (SelfTestResult, error) {
	res := SelfTestResult{Chave: selfTestChave}
	if s.cfg.DryRun {
		return res, fmt.Errorf("selftest não roda em dry-run (precisa da escrita real)")
	}
	if s.wr == nil {
		return res, fmt.Errorf("selftest exige a conexão de escrita (TRACKER_FB_WRITE_DSN)")
	}

	// Ancora a isca numa filial REAL (evita esbarrar em FK de CODIGOEMPRESA); a
	// chave sintética é o que a torna inofensiva, não a empresa.
	filiais, err := s.rd.ListFiliais(ctx)
	if err != nil {
		return res, fmt.Errorf("listar filiais: %w", err)
	}
	if len(filiais) == 0 {
		return res, fmt.Errorf("sem filiais para ancorar a linha-isca")
	}
	f := filiais[0]
	res.Empresa, res.Filial = f.CodigoEmpresa, f.CodigoFilial

	// Limpeza defensiva: se uma execução anterior abortou e deixou a isca, remove
	// antes de medir a linha-base.
	if _, err := s.wr.DeleteOurRows(ctx, selfTestChave, MarkerPrefix); err != nil {
		return res, fmt.Errorf("limpeza pré-teste: %w", err)
	}
	before, err := s.rd.CountOurRows(ctx, MarkerPrefix, time.Time{})
	if err != nil {
		return res, fmt.Errorf("contagem inicial: %w", err)
	}
	res.TotalBefore = before.Total

	// INSERT da isca.
	id, err := s.wr.NextChaveID(ctx)
	if err != nil {
		return res, fmt.Errorf("GEN_ID: %w", err)
	}
	row := firebird.InsertRow{
		Chave:           selfTestChave,
		CodigoEmpresa:   f.CodigoEmpresa,
		CodigoFilial:    f.CodigoFilial,
		Emitente:        "SELFTEST rps-xml-tracker (linha de teste - apagar)",
		DataEmissao:     s.cfg.Now(),
		URL:             `\__SELFTEST__\` + selfTestChave + ".xml",
		CaminhoOriginal: "SELFTEST rps-xml-tracker",
		Observacoes:     s.cfg.Marker,
	}
	if err := s.wr.InsertChaveAcesso(ctx, id, row); err != nil {
		return res, fmt.Errorf("INSERT da isca: %w", err)
	}
	has, err := s.rd.HasRow(ctx, selfTestChave, f.CodigoEmpresa, f.CodigoFilial)
	if err != nil {
		return res, fmt.Errorf("pre-check pós-insert: %w", err)
	}
	res.Inserted = has
	if !has {
		return res, fmt.Errorf("isca inserida mas HasRow=false — ABORTA (NÃO prossiga para o sync real)")
	}

	// ROLLBACK: o mesmo DELETE-por-marcador do --rollback.
	n, err := s.wr.DeleteOurRows(ctx, selfTestChave, MarkerPrefix)
	if err != nil {
		return res, fmt.Errorf("DELETE da isca: %w", err)
	}
	res.RowsDeleted = n

	has, err = s.rd.HasRow(ctx, selfTestChave, f.CodigoEmpresa, f.CodigoFilial)
	if err != nil {
		return res, fmt.Errorf("pre-check pós-delete: %w", err)
	}
	after, err := s.rd.CountOurRows(ctx, MarkerPrefix, time.Time{})
	if err != nil {
		return res, fmt.Errorf("contagem final: %w", err)
	}
	res.TotalAfter = after.Total
	res.OK = res.Inserted && !has && n >= 1 && res.TotalAfter == res.TotalBefore
	s.cfg.Log("SELFTEST chave=%s empresa=%d/%d inserida=%v apagadas=%d total_antes=%d total_depois=%d OK=%v",
		selfTestChave, f.CodigoEmpresa, f.CodigoFilial, res.Inserted, n, res.TotalBefore, res.TotalAfter, res.OK)
	return res, nil
}
