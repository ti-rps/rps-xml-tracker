package syncer

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/EnzzoHosaki/rps-xml-tracker/internal/firebird"
)

// auditReader é a fatia READ-ONLY que a auditoria precisa (implementada por
// *firebird.Reader). Fica numa interface pequena para o --audit rodar só com a
// credencial de leitura, sem o writer nem os roots do syncer.
type auditReader interface {
	CountOurRows(ctx context.Context, markerPrefix string, since time.Time) (firebird.OurRowsCount, error)
	ListOurRows(ctx context.Context, markerPrefix string, onlyPending bool, since time.Time) ([]firebird.OurRow, error)
}

// AuditResult resume a auditoria (§14 item ①/②).
type AuditResult struct {
	Count  firebird.OurRowsCount
	Dumped int // linhas gravadas no manifesto (0 se sem --dump)
}

// AuditRows conta as nossas linhas (marcador), split por IMPORTADO, e — se
// dumpPath != "" — grava um manifesto JSONL com cada linha nossa (a rede de
// redundância: registro durável de tudo que inserimos). READ-ONLY. since != zero
// escopa por DATAINCLUSAO (índice); zero varre a tabela inteira — ver CountOurRows.
func AuditRows(ctx context.Context, rd auditReader, dumpPath string, since time.Time, log func(format string, args ...any)) (AuditResult, error) {
	c, err := rd.CountOurRows(ctx, MarkerPrefix, since)
	if err != nil {
		return AuditResult{}, err
	}
	res := AuditResult{Count: c}
	escopo := "TODAS as datas (full scan)"
	if !since.IsZero() {
		escopo = "DATAINCLUSAO >= " + since.Format("2006-01-02")
	}
	log("AUDIT nossas linhas [%s]: total=%d pendentes(IMPORTADO=0)=%d importadas(IMPORTADO=1)=%d marcador=%q",
		escopo, c.Total, c.Pending, c.Imported, MarkerPrefix)
	if dumpPath == "" {
		return res, nil
	}
	rows, err := rd.ListOurRows(ctx, MarkerPrefix, false, since) // manifesto = TUDO (pendentes + importadas)
	if err != nil {
		return res, fmt.Errorf("listar para o manifesto: %w", err)
	}
	f, err := os.Create(dumpPath)
	if err != nil {
		return res, fmt.Errorf("criar %s: %w", dumpPath, err)
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	for _, r := range rows {
		if err := enc.Encode(r); err != nil {
			return res, fmt.Errorf("gravar manifesto: %w", err)
		}
		res.Dumped++
	}
	log("AUDIT manifesto: %d linha(s) gravada(s) em %s", res.Dumped, dumpPath)
	return res, nil
}
