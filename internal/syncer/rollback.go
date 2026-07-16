package syncer

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/EnzzoHosaki/rps-xml-tracker/internal/model"
)

// MarkerPrefix é o prefixo ESTÁVEL do marcador OBSERVACOES (sem o commit). O
// rollback filtra por ele no DELETE, então casa linhas de qualquer versão do
// tracker. Precisa bater com o começo de Config.Marker.
const MarkerPrefix = "sync rps-xml-tracker"

// RollbackResult resume o que o rollback fez, para log/CLI.
type RollbackResult struct {
	Chave         string
	RowsDeleted   int64
	FilesRestored int
	FilesDeleted  int
	Skipped       bool   // não desfez (sem journal no bulk, ou já importada) — ver SkipReason
	SkipReason    string // motivo do skip (vazio se não pulou)
	Warnings      []string
}

// Rollback desfaz, por chave, uma sincronização feita por ESTE tracker enquanto
// IMPORTADO=0 (§10 do plano): (1) apaga as NOSSAS linhas ainda não importadas,
// (2) restaura o arquivo na ASINCRONIZAR e apaga a(s) cópia(s) no SINCRONIZADO,
// (3) emite sync_failed "rollback manual" na timeline. É a única operação
// destrutiva do syncer — o chamador exige --yes. Se QUALQUER participação da chave
// já foi importada, a guarda de importada (rollbackOne) apaga só as pendentes e
// PRESERVA os arquivos (a linha importada referencia o XML) — reporta parcial.
func (s *Syncer) Rollback(ctx context.Context, chave string) (RollbackResult, error) {
	return s.rollbackOne(ctx, chave, false)
}

// rollbackOne é o rollback de UMA chave. requireJournal=true (usado pelo
// RollbackAll em lote) recusa desfazer uma chave que não está no journal local:
// sem o journal não dá para restaurar o arquivo, e apagar só a linha do banco
// deixaria o XML órfão no SINCRONIZADO, fora da fila de importação (achado do
// code-review). O single-key (requireJournal=false) mantém o comportamento antigo
// — o operador mira uma chave conhecida e aceita o aviso de arquivo manual.
func (s *Syncer) rollbackOne(ctx context.Context, chave string, requireJournal bool) (RollbackResult, error) {
	res := RollbackResult{Chave: chave}
	if s.cfg.DryRun {
		return res, fmt.Errorf("rollback não roda em dry-run (precisa da escrita real)")
	}
	if s.wr == nil {
		return res, fmt.Errorf("rollback exige a conexão de escrita (TRACKER_FB_WRITE_DSN)")
	}
	if len(chave) != 44 {
		res.Skipped, res.SkipReason = true, "chave sem 44 dígitos — pulada (não toca em nada)"
		res.Warnings = append(res.Warnings, res.SkipReason)
		return res, nil
	}

	parts := s.jr.partsForChave(chave)
	if len(parts) == 0 {
		if requireJournal {
			res.Skipped, res.SkipReason = true,
				"sem registro no journal local — pulada no lote; use --rollback <chave> single-key com o arquivo à mão"
			res.Warnings = append(res.Warnings, res.SkipReason)
			return res, nil
		}
		res.Warnings = append(res.Warnings,
			"sem registro no journal — só o DELETE do banco será tentado; qualquer arquivo é intervenção manual")
	}

	// Guarda de importada: se QUALQUER participação nossa já entrou no livro
	// (IMPORTADO=1), os arquivos NÃO podem ser mexidos — a linha importada
	// referencia o XML no SINCRONIZADO, e restaurar a origem arriscaria duplicata.
	// Ainda apagamos as linhas pendentes (DeleteOurRows filtra IMPORTADO=0), mas
	// sem tocar em arquivo e sem timeline de "desfeito".
	hasImported, err := s.rd.HasImportedRow(ctx, chave, MarkerPrefix)
	if err != nil {
		return res, fmt.Errorf("pre-check de importada: %w", err)
	}
	if hasImported {
		n, derr := s.wr.DeleteOurRows(ctx, chave, MarkerPrefix)
		if derr != nil {
			return res, fmt.Errorf("DELETE das linhas pendentes: %w", derr)
		}
		res.RowsDeleted = n
		res.Skipped, res.SkipReason = true,
			fmt.Sprintf("chave com participação IMPORTADA — apagadas %d linha(s) pendente(s); ARQUIVOS preservados; verifique manualmente (estorno é fiscal)", n)
		res.Warnings = append(res.Warnings, res.SkipReason)
		s.cfg.Log("ROLLBACK chave=%s PARCIAL (importada): linhas_pendentes_apagadas=%d arquivos_preservados", chave, n)
		return res, nil
	}

	// passo 1: apaga só as NOSSAS linhas ainda não importadas.
	n, err := s.wr.DeleteOurRows(ctx, chave, MarkerPrefix)
	if err != nil {
		return res, fmt.Errorf("DELETE das linhas: %w", err)
	}
	res.RowsDeleted = n

	// passo 2: restaura a origem UMA vez (todas as participações vêm do mesmo
	// arquivo) e apaga cada cópia no destino.
	origemRestored := false
	for _, p := range parts {
		st := p.State
		if !origemRestored && st.Origem != "" {
			if _, err := os.Stat(st.Origem); err == nil {
				origemRestored = true // origem ainda existe (sync não chegou ao delete)
			} else if os.IsNotExist(err) && st.DestAbs != "" {
				if _, derr := os.Stat(st.DestAbs); derr == nil {
					if e := os.MkdirAll(filepath.Dir(st.Origem), 0o755); e != nil {
						res.Warnings = append(res.Warnings, "criar dir da origem: "+e.Error())
					} else if e := copyFile(st.DestAbs, st.Origem); e != nil {
						res.Warnings = append(res.Warnings, "restaurar origem: "+e.Error())
					} else {
						origemRestored = true
						res.FilesRestored++
					}
				}
			}
		}
		if st.DestAbs != "" {
			if err := os.Remove(st.DestAbs); err == nil {
				res.FilesDeleted++
			} else if !os.IsNotExist(err) {
				res.Warnings = append(res.Warnings, "apagar destino "+st.DestAbs+": "+err.Error())
			}
		}
		// passo 3: timeline (auditoria). sync_failed é não-progresso no derive.
		s.emitRollback(ctx, chave, p)
	}

	if len(parts) > 0 && !origemRestored {
		res.Warnings = append(res.Warnings,
			"origem NÃO restaurada (nem a origem nem alguma cópia foi encontrada) — verifique o arquivo manualmente")
	}
	if err := s.jr.clearChave(chave); err != nil {
		res.Warnings = append(res.Warnings, "limpar journal: "+err.Error())
	}
	s.cfg.Log("ROLLBACK chave=%s linhas_apagadas=%d origem_restaurada=%d destinos_apagados=%d avisos=%d",
		chave, res.RowsDeleted, res.FilesRestored, res.FilesDeleted, len(res.Warnings))
	return res, nil
}

// RollbackAllResult resume o rollback em massa (§14 item ①).
type RollbackAllResult struct {
	Chaves        int      // chaves distintas EFETIVAMENTE desfeitas (linha apagada + arquivo tratado)
	RowsDeleted   int64    // total de linhas apagadas
	FilesRestored int      // origens restauradas
	FilesDeleted  int      // destinos apagados
	Skipped       int      // chaves puladas (sem journal / já importada) — precisam de ação manual
	Failures      int      // chaves cujo rollback deu erro
	Warnings      []string // avisos (prefixados com a chave)
}

// RollbackAll desfaz, EM LOTE, todas as NOSSAS linhas ainda não importadas
// (IMPORTADO=0 + marcador): enumera as chaves pendentes pelo banco e roda o
// rollbackOne(requireJournal=true) por chave — cada DELETE segue chave-scoped
// (nunca um único DELETE que varre a tabela). Conservador de propósito: chave sem
// journal local ou com participação já importada é PULADA (contada em Skipped, com
// aviso), nunca desfeita pela metade. Mesmo "all" é limitado às nossas linhas
// não-importadas. O chamador exige --yes.
func (s *Syncer) RollbackAll(ctx context.Context) (RollbackAllResult, error) {
	var res RollbackAllResult
	if s.cfg.DryRun {
		return res, fmt.Errorf("rollback-all não roda em dry-run (precisa da escrita real)")
	}
	if s.wr == nil {
		return res, fmt.Errorf("rollback-all exige a conexão de escrita (TRACKER_FB_WRITE_DSN)")
	}
	rows, err := s.rd.ListOurRows(ctx, MarkerPrefix, true, time.Time{}) // só IMPORTADO=0
	if err != nil {
		return res, fmt.Errorf("listar nossas linhas pendentes: %w", err)
	}
	seen := map[string]bool{}
	var chaves []string
	for _, r := range rows {
		if r.Chave == "" || seen[r.Chave] {
			continue
		}
		seen[r.Chave] = true
		chaves = append(chaves, r.Chave)
	}
	s.cfg.Log("ROLLBACK-ALL iniciando: %d linha(s) pendente(s) em %d chave(s) distinta(s)", len(rows), len(chaves))
	for _, ch := range chaves {
		r, err := s.rollbackOne(ctx, ch, true)
		if err != nil {
			res.Failures++
			res.Warnings = append(res.Warnings, ch+": "+err.Error())
			continue
		}
		res.RowsDeleted += r.RowsDeleted
		res.FilesRestored += r.FilesRestored
		res.FilesDeleted += r.FilesDeleted
		for _, w := range r.Warnings {
			res.Warnings = append(res.Warnings, ch+": "+w)
		}
		if r.Skipped {
			res.Skipped++
		} else {
			res.Chaves++
		}
	}
	s.cfg.Log("ROLLBACK-ALL concluído: desfeitas=%d linhas_apagadas=%d origens_restauradas=%d destinos_apagados=%d puladas=%d falhas=%d",
		res.Chaves, res.RowsDeleted, res.FilesRestored, res.FilesDeleted, res.Skipped, res.Failures)
	return res, nil
}

// emitRollback registra o sync_failed "rollback manual" (por participação) para a
// timeline. Campos mínimos: sync_failed não muda status (não seta SyncedAt), só
// conta a história; a nota já tem doc_type/empresa dos sync_moved anteriores.
func (s *Syncer) emitRollback(ctx context.Context, chave string, p journaledPart) {
	if s.sub == nil {
		return
	}
	emp, fil := p.Emp, p.Fil
	obs := model.Observation{
		ChaveAcesso:   chave,
		Stage:         model.StageSync,
		EventType:     model.EventSyncFailed,
		ObservedAt:    s.cfg.Now(),
		Source:        "syncer:" + s.cfg.Name,
		FilePath:      p.State.DestAbs,
		Payload:       map[string]any{"erro": "rollback manual", "rollback": true, "url": p.State.DestRel},
		CodigoEmpresa: &emp,
		CodigoFilial:  &fil,
	}
	if err := s.sub.Submit(ctx, []model.Observation{obs}); err != nil {
		s.cfg.Log("observação rollback (%s): %v", chave, err)
	}
}
