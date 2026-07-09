package syncer

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/EnzzoHosaki/rps-xml-tracker/internal/model"
	"github.com/EnzzoHosaki/rps-xml-tracker/internal/xmlparse"
)

// Execute roda os passos 5-10 do fluxo para um plano válido, POR PARTICIPAÇÃO:
// journal(planned) → copy(.tracker-tmp) → verify → rename → INSERT — e o delete
// da origem SÓ depois de todas as participações completarem. Qualquer falha
// deixa o sistema num estado seguro (a origem nunca some antes de o destino
// estar verificado E a linha registrada) e é retomável: os pre-checks detectam o
// que já foi feito (destino ok? linha existe?) e pulam o passo.
func (s *Syncer) Execute(ctx context.Context, plan Plan) error {
	if s.cfg.DryRun {
		return fmt.Errorf("Execute chamado em dry-run — bug do chamador")
	}
	if plan.Skip != "" {
		return fmt.Errorf("plano não-executável: %s", plan.Skip)
	}
	for _, part := range plan.Participacoes {
		if err := s.executePart(ctx, plan, part); err != nil {
			s.emitFailed(ctx, plan, part, err)
			return fmt.Errorf("participação %d/%d: %w", part.CodigoEmpresa, part.CodigoFilial, err)
		}
	}
	// passo 10: origem só sai depois de re-verificar TODAS as participações
	// (destino íntegro + linha presente) — a regra de ouro do fluxo.
	for _, part := range plan.Participacoes {
		if err := s.verifyComplete(ctx, plan, part); err != nil {
			s.emitFailed(ctx, plan, part, fmt.Errorf("verificação final: %w", err))
			return err
		}
	}
	if err := os.Remove(plan.Origem); err != nil && !os.IsNotExist(err) {
		s.emitFailed(ctx, plan, plan.Participacoes[0], fmt.Errorf("delete origem: %w", err))
		return err
	}
	if err := s.jr.markDone(plan.Chave); err != nil {
		return err
	}
	s.cfg.Log("SYNC concluído chave=%s participações=%d origem removida", plan.Chave, len(plan.Participacoes))
	return nil
}

// executePart leva UMA participação até "inserted".
func (s *Syncer) executePart(ctx context.Context, plan Plan, part Participacao) error {
	// passo 5: journal "planned" (intenção registrada antes de qualquer efeito)
	if _, found := s.jr.getPart(plan.Chave, part.CodigoEmpresa, part.CodigoFilial); !found {
		if err := s.jr.setPart(plan.Chave, part.CodigoEmpresa, part.CodigoFilial, partState{
			State: statePlanned, Origem: plan.Origem, DestRel: part.DestRel, DestAbs: part.DestAbs,
		}); err != nil {
			return fmt.Errorf("journal planned: %w", err)
		}
	}

	// passos 6-8: posicionar o arquivo (idempotente — destino já ok = pulado)
	moved, err := s.ensureMoved(plan, part)
	if err != nil {
		return err
	}
	if moved {
		if err := s.jr.setPart(plan.Chave, part.CodigoEmpresa, part.CodigoFilial, partState{
			State: stateMoved, Origem: plan.Origem, DestRel: part.DestRel, DestAbs: part.DestAbs,
		}); err != nil {
			return fmt.Errorf("journal moved: %w", err)
		}
		s.emit(ctx, plan, part, model.EventSyncMoved, map[string]any{
			"url": part.DestRel, "origem": plan.Origem,
		})
	}

	// passo 9: INSERT (idempotente — linha já existente = pulado)
	has, err := s.rd.HasRow(ctx, plan.Chave, part.CodigoEmpresa, part.CodigoFilial)
	if err != nil {
		return fmt.Errorf("pre-check da linha: %w", err)
	}
	if !has {
		id, err := s.wr.NextChaveID(ctx)
		if err != nil {
			return fmt.Errorf("GEN_ID: %w", err)
		}
		if err := s.wr.InsertChaveAcesso(ctx, id, s.insertRowFor(plan, part)); err != nil {
			return fmt.Errorf("INSERT: %w", err)
		}
		if err := s.jr.setPart(plan.Chave, part.CodigoEmpresa, part.CodigoFilial, partState{
			State: stateInserted, Origem: plan.Origem, DestRel: part.DestRel, DestAbs: part.DestAbs, InsertID: id,
		}); err != nil {
			return fmt.Errorf("journal inserted: %w", err)
		}
		s.emit(ctx, plan, part, model.EventSyncDBInserted, map[string]any{
			"codigo_chaveacesso": id, "url": part.DestRel,
		})
	}
	return nil
}

// ensureMoved garante o arquivo no destino. Retorna moved=true quando ESTA
// chamada posicionou o arquivo (emite sync_moved); false quando o destino já
// estava ok (retomada de crash / DownloadXML chegou antes — nada a emitir de
// novo, o dedup seguraria de qualquer jeito).
func (s *Syncer) ensureMoved(plan Plan, part Participacao) (bool, error) {
	if ok, err := s.destOK(plan, part.DestAbs); err != nil {
		return false, err
	} else if ok {
		return false, nil
	}
	if err := os.MkdirAll(filepath.Dir(part.DestAbs), 0o755); err != nil {
		return false, fmt.Errorf("criar diretórios: %w", err)
	}
	// passo 6: copy com sufixo .tracker-tmp — o DownloadXML/AthenasHorse nunca
	// disputam um .tracker-tmp; o conteúdo só aparece com o nome final no rename.
	tmp := part.DestAbs + ".tracker-tmp"
	if err := copyFile(plan.Origem, tmp); err != nil {
		_ = os.Remove(tmp)
		return false, fmt.Errorf("copy: %w", err)
	}
	// passo 7: verify — re-parse do tmp tem de dar a MESMA chave, e o tamanho
	// tem de bater com a origem.
	if err := verifyCopy(plan.Origem, tmp, plan.Chave); err != nil {
		_ = os.Remove(tmp)
		return false, fmt.Errorf("verify: %w", err)
	}
	// passo 8: rename atômico (mesmo volume NTFS).
	if err := os.Rename(tmp, part.DestAbs); err != nil {
		_ = os.Remove(tmp)
		return false, fmt.Errorf("rename: %w", err)
	}
	return true, nil
}

// destOK inspeciona um destino existente: ausente = (false, nil); presente com a
// MESMA chave = (true, nil); presente com conteúdo DIVERGENTE = erro de conflito
// (nunca sobrescrever — intervenção manual).
func (s *Syncer) destOK(plan Plan, destAbs string) (bool, error) {
	if _, err := os.Stat(destAbs); os.IsNotExist(err) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	res, err := xmlparse.ParseFile(destAbs)
	if err != nil || res.Chave != plan.Chave {
		return false, fmt.Errorf("conflito: destino %s já existe com conteúdo divergente (chave %q) — intervenção manual", destAbs, res.Chave)
	}
	return true, nil
}

// verifyComplete é a checagem final pré-delete: destino íntegro E linha presente.
func (s *Syncer) verifyComplete(ctx context.Context, plan Plan, part Participacao) error {
	if ok, err := s.destOK(plan, part.DestAbs); err != nil || !ok {
		if err == nil {
			err = fmt.Errorf("destino %s ausente", part.DestAbs)
		}
		return err
	}
	has, err := s.rd.HasRow(ctx, plan.Chave, part.CodigoEmpresa, part.CodigoFilial)
	if err != nil {
		return err
	}
	if !has {
		return fmt.Errorf("linha (chave, %d/%d) ausente na TABLISTACHAVEACESSO", part.CodigoEmpresa, part.CodigoFilial)
	}
	return nil
}

// emit envia UMA observação do syncer (stage sync, POR PARTICIPAÇÃO — M0).
// Falha de envio não interrompe a sincronização: o ingest client spoola.
func (s *Syncer) emit(ctx context.Context, plan Plan, part Participacao, event string, payload map[string]any) {
	if s.sub == nil {
		return
	}
	emp, fil := part.CodigoEmpresa, part.CodigoFilial
	obs := model.Observation{
		ChaveAcesso:      plan.Chave,
		Stage:            model.StageSync,
		EventType:        event,
		ObservedAt:       s.cfg.Now(),
		Source:           "syncer:" + s.cfg.Name,
		DocType:          plan.DocType,
		FilePath:         part.DestAbs,
		Payload:          payload,
		CodigoEmpresa:    &emp,
		CodigoFilial:     &fil,
		NomeEmpresa:      part.NomeEmpresa,
		Direction:        part.Direction,
		CnpjEmitente:     digits(plan.parse.CnpjEmitente),
		NomeEmitente:     plan.parse.NomeEmitente,
		CnpjDestinatario: digits(plan.parse.CnpjDestinatario),
		NomeDestinatario: plan.parse.NomeDestinatario,
		DataEmissao:      plan.DataEmissao,
	}
	if err := s.sub.Submit(ctx, []model.Observation{obs}); err != nil {
		s.cfg.Log("observação %s (%s): %v", event, plan.Chave, err)
	}
}

// emitFailed registra a falha na timeline (payload: passo/erro).
func (s *Syncer) emitFailed(ctx context.Context, plan Plan, part Participacao, cause error) {
	s.emit(ctx, plan, part, model.EventSyncFailed, map[string]any{"erro": cause.Error()})
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	if err := out.Sync(); err != nil { // durável ANTES do rename
		out.Close()
		return err
	}
	return out.Close()
}

// verifyCopy confere tamanho origem==cópia e re-parseia a cópia (mesma chave).
func verifyCopy(src, tmp, chave string) error {
	si, err := os.Stat(src)
	if err != nil {
		return err
	}
	ti, err := os.Stat(tmp)
	if err != nil {
		return err
	}
	if si.Size() != ti.Size() {
		return fmt.Errorf("tamanho divergente (origem %d, cópia %d)", si.Size(), ti.Size())
	}
	res, err := xmlparse.ParseFile(tmp)
	if err != nil {
		return fmt.Errorf("re-parse da cópia: %w", err)
	}
	if res.Chave != chave {
		return fmt.Errorf("chave da cópia divergente (%q)", res.Chave)
	}
	return nil
}

// RunChave sincroniza (ou planeja, em dry-run) UMA chave específica — o gatilho
// do piloto (F2). file aponta direto o XML na ASINCRONIZAR (o nome do arquivo
// NÃO contém a chave, então sem ele seria preciso varrer a pasta inteira; o
// file_path da observação de chegada dá o caminho). A chave é conferida contra
// o parse — proteção contra apontar o arquivo errado.
func (s *Syncer) RunChave(ctx context.Context, chave, file string) (Plan, error) {
	if err := s.refreshResolve(ctx); err != nil {
		return Plan{}, err
	}
	if file == "" {
		return Plan{}, fmt.Errorf("--chave exige --file (o nome do arquivo não contém a chave; pegue o file_path da observação de chegada)")
	}
	plan := s.PlanFile(ctx, file, false) // single-key não passa pela allowlist
	if plan.Skip != "" {
		return plan, fmt.Errorf("não sincronizável: %s", plan.Skip)
	}
	if plan.Chave != chave {
		return plan, fmt.Errorf("o arquivo %s tem a chave %s, não %s — arquivo errado?", file, plan.Chave, chave)
	}
	s.logPlan(plan)
	if s.cfg.DryRun {
		s.recordPlan(plan)
		return plan, nil
	}
	return plan, s.Execute(ctx, plan)
}
