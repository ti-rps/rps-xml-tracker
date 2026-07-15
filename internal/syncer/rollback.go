package syncer

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

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
	Warnings      []string
}

// Rollback desfaz, por chave, uma sincronização feita por ESTE tracker enquanto
// IMPORTADO=0 (§10 do plano): (1) apaga as NOSSAS linhas ainda não importadas,
// (2) restaura o arquivo na ASINCRONIZAR e apaga a(s) cópia(s) no SINCRONIZADO,
// (3) emite sync_failed "rollback manual" na timeline. É a única operação
// destrutiva do syncer — o chamador exige --yes. Se o Horse já importou, o
// filtro IMPORTADO=0 protege a linha (não some) e o resultado reporta 0 apagadas.
func (s *Syncer) Rollback(ctx context.Context, chave string) (RollbackResult, error) {
	res := RollbackResult{Chave: chave}
	if s.cfg.DryRun {
		return res, fmt.Errorf("rollback não roda em dry-run (precisa da escrita real)")
	}
	if s.wr == nil {
		return res, fmt.Errorf("rollback exige a conexão de escrita (TRACKER_FB_WRITE_DSN)")
	}

	parts := s.jr.partsForChave(chave)
	if len(parts) == 0 {
		res.Warnings = append(res.Warnings,
			"sem registro no journal — só o DELETE do banco será tentado; qualquer arquivo é intervenção manual")
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
