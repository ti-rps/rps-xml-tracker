// Package firebird provides a READ-ONLY reader of the Athenas import signal in
// TABLISTACHAVEACESSO. It is chave-driven (Fase 0): it looks up the import
// status of a given set of chaves by the indexed CHAVEACESSO column — instant,
// no full scans of the 23.5M-row table. It issues ONLY SELECT statements and
// never writes anything.
package firebird

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/nakagami/firebirdsql"
)

// Reader holds a read-only connection pool to the Athenas Firebird DB.
type Reader struct {
	db *sql.DB
}

// EmpresaImport is ONE TABLISTACHAVEACESSO row for a chave — the import signal as
// seen by a SINGLE empresa. The table is keyed by CHAVEACESSO but NOT unique on
// it: the same chave routinely has several rows — the empresa that emitted it
// plus the same nota listed "de terceiros" by other empresas — each carrying its
// OWN IMPORTADO / IMPORTACAOIGNORADA / MOTIVO. They must NOT be blindly merged
// (doing so attributed the nota to an arbitrary empresa and let a third party's
// "importação ignorada" mask the real owner's import). See selectState.
type EmpresaImport struct {
	CodigoEmpresa  *int
	CodigoFilial   *int
	NomeEmpresa    string
	Importado      bool
	ImportIgnorada bool
	Motivo         string
	Situacao       *int
	// metadados da nota (idênticos entre as linhas — são da nota, não da empresa)
	TipoDocumento    string
	CnpjEmitente     string
	NomeEmitente     string
	CnpjDestinatario string
	NomeDestinatario string
	DataEmissao      string // yyyy-mm-dd
	ValorTotal       *float64
}

// ImportState is the import status of one chave, resolved to the ONE empresa row
// that represents the nota's real lifecycle (see selectState). Its scalar fields
// are that selected row's; Rows holds every per-empresa row for transparency.
type ImportState struct {
	Chave          string
	Found          bool
	Importado      bool
	ImportIgnorada bool
	Motivo         string
	Situacao       *int
	TipoDocumento  string
	// metadados (enriquecem a nota): código do cliente no Athenas + partes
	CodigoEmpresa    *int
	CodigoFilial     *int
	NomeEmpresa      string
	CnpjEmitente     string
	NomeEmitente     string
	CnpjDestinatario string
	NomeDestinatario string
	DataEmissao      string // yyyy-mm-dd
	ValorTotal       *float64
	// Rows são TODAS as linhas (uma por empresa) que o Athenas tem para a chave.
	Rows []EmpresaImport
}

// NewReader opens the pool. The DSN must enable Legacy_Auth and disable wire
// encryption for Firebird 3+ (see Fase 0):
//
//	SYSDBA:masterkey@host:3050//path/to.fdb?charset=NONE&auth_plugin_name=Legacy_Auth&wire_crypt=disabled
func NewReader(ctx context.Context, dsn string) (*Reader, error) {
	db, err := sql.Open("firebirdsql", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(2)
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping firebird: %w", err)
	}
	return &Reader{db: db}, nil
}

func (r *Reader) Close() error { return r.db.Close() }

// chunkSize keeps each IN (...) well under Firebird's parameter limit.
const chunkSize = 400

// Lookup returns the import state for each chave found. Chaves absent from the
// result map were not found in Athenas. READ-ONLY (SELECT only). A chave may have
// several rows (one per empresa); they are collected and then resolved to one
// representative state by selectState.
func (r *Reader) Lookup(ctx context.Context, chaves []string) (map[string]ImportState, error) {
	rowsByChave := make(map[string][]EmpresaImport, len(chaves))
	for start := 0; start < len(chaves); start += chunkSize {
		end := start + chunkSize
		if end > len(chaves) {
			end = len(chaves)
		}
		if err := r.lookupChunk(ctx, chaves[start:end], rowsByChave); err != nil {
			return nil, err
		}
	}
	out := make(map[string]ImportState, len(rowsByChave))
	for chave, rows := range rowsByChave {
		out[chave] = selectState(chave, rows)
	}
	return out, nil
}

func (r *Reader) lookupChunk(ctx context.Context, chaves []string, rowsByChave map[string][]EmpresaImport) error {
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(chaves)), ",")
	args := make([]any, len(chaves))
	for i, c := range chaves {
		args[i] = c
	}
	q := `SELECT t.CHAVEACESSO, t.IMPORTADO, t.IMPORTACAOIGNORADA, t.MOTIVOIGNORADOIMPORTACAO, t.SITUACAO,
	             t.TIPODOCUMENTO, t.CODIGOEMPRESA, t.CODIGOFILIAL, t.CNPJEMITENTE, t.CNPJDESTINATARIO,
	             t.EMITENTE, t.DESTINATARIO, t.DATAEMISSAO, t.VALORTOTAL, e.NOME
	      FROM TABLISTACHAVEACESSO t
	      LEFT JOIN TABEMPRESAS e ON e.CODIGO = t.CODIGOEMPRESA
	      WHERE t.CHAVEACESSO IN (` + placeholders + `)`
	rows, err := r.db.QueryContext(ctx, q, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	return scanRows(rows, rowsByChave)
}

// SweepImported retorna todas as chaves com IMPORTADO=1 e DATAROBO > since.
// É O(importadas_recentes), independente do tamanho do backlog in-flight, e usa
// o índice IDX4 (LOTEROBO, DATAROBO) do Firebird para a varredura por data.
// Notas com DATAROBO=NULL (importadas sem passar pelo robô) NÃO são retornadas —
// o poller rotacional as captura via ListInflightChaves.
func (r *Reader) SweepImported(ctx context.Context, since time.Time) (map[string]ImportState, error) {
	q := `SELECT FIRST 10000
	             t.CHAVEACESSO, t.IMPORTADO, t.IMPORTACAOIGNORADA, t.MOTIVOIGNORADOIMPORTACAO, t.SITUACAO,
	             t.TIPODOCUMENTO, t.CODIGOEMPRESA, t.CODIGOFILIAL, t.CNPJEMITENTE, t.CNPJDESTINATARIO,
	             t.EMITENTE, t.DESTINATARIO, t.DATAEMISSAO, t.VALORTOTAL, e.NOME
	      FROM TABLISTACHAVEACESSO t
	      LEFT JOIN TABEMPRESAS e ON e.CODIGO = t.CODIGOEMPRESA
	      WHERE t.IMPORTADO = 1
	        AND t.DATAROBO > ?`
	rows, err := r.db.QueryContext(ctx, q, since)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	rowsByChave := make(map[string][]EmpresaImport)
	if err := scanRows(rows, rowsByChave); err != nil {
		return nil, err
	}
	out := make(map[string]ImportState, len(rowsByChave))
	for chave, rowList := range rowsByChave {
		out[chave] = selectState(chave, rowList)
	}
	return out, nil
}

// scanRows escaneia as colunas padrão de TABLISTACHAVEACESSO (SELECT t.CHAVEACESSO,
// t.IMPORTADO, ..., e.NOME) em dst. Compartilhado por lookupChunk e SweepImported.
func scanRows(rows *sql.Rows, dst map[string][]EmpresaImport) error {
	for rows.Next() {
		var (
			chave          string
			imp, ign, sit  sql.NullInt64
			motivo, tipo   sql.NullString
			codEmp, codFil sql.NullInt64
			cnpjE, cnpjD   sql.NullString
			nomeE, nomeD   sql.NullString
			emissao        sql.NullTime
			valor          sql.NullFloat64
			nomeEmpresa    sql.NullString
		)
		if err := rows.Scan(&chave, &imp, &ign, &motivo, &sit, &tipo,
			&codEmp, &codFil, &cnpjE, &cnpjD, &nomeE, &nomeD, &emissao, &valor, &nomeEmpresa); err != nil {
			return err
		}
		chave = strings.TrimSpace(chave)
		e := EmpresaImport{
			Importado:        imp.Valid && imp.Int64 == 1,
			ImportIgnorada:   ign.Valid && ign.Int64 == 1,
			Motivo:           trimNull(motivo),
			TipoDocumento:    trimNull(tipo),
			NomeEmpresa:      trimNull(nomeEmpresa),
			CnpjEmitente:     trimNull(cnpjE),
			NomeEmitente:     trimNull(nomeE),
			CnpjDestinatario: trimNull(cnpjD),
			NomeDestinatario: trimNull(nomeD),
		}
		if sit.Valid {
			v := int(sit.Int64)
			e.Situacao = &v
		}
		if codEmp.Valid {
			v := int(codEmp.Int64)
			e.CodigoEmpresa = &v
		}
		if codFil.Valid {
			v := int(codFil.Int64)
			e.CodigoFilial = &v
		}
		if emissao.Valid {
			e.DataEmissao = emissao.Time.Format("2006-01-02")
		}
		if valor.Valid {
			v := valor.Float64
			e.ValorTotal = &v
		}
		dst[chave] = append(dst[chave], e)
	}
	return rows.Err()
}

// selectState resolves the per-empresa rows of one chave to the single row that
// represents the nota's real import lifecycle. A chave has one row per empresa
// (own emission + the same nota copied "de terceiros" by others); each row has
// its own IMPORTADO/IMPORTACAOIGNORADA/MOTIVO. Priority:
//
//  1. a row with IMPORTADO=1 — the empresa that ACTUALLY imported the nota is its
//     owner; attribute the nota (empresa, status, metadata) to that row;
//  2. else a row still pending (neither imported nor ignored) — the nota is still
//     em trânsito, NOT terminal: emit nothing and keep polling. This stops a third
//     party's "importação ignorada" from prematurely ending a nota the owner will
//     still import;
//  3. else (every row is IMPORTACAOIGNORADA=1) — the nota is genuinely ignored
//     everywhere; report it ignored with that row's motivo.
//
// Ties are broken by the lowest CODIGOEMPRESA so the result is deterministic
// (the old code took whatever row Firebird returned first — non-deterministic).
func selectState(chave string, rows []EmpresaImport) ImportState {
	st := ImportState{Chave: chave, Found: true, Rows: rows}

	rep, ok := lowestCodigo(rows, func(e EmpresaImport) bool { return e.Importado })
	switch {
	case ok:
		st.Importado = true
	default:
		if pend, okp := lowestCodigo(rows, func(e EmpresaImport) bool {
			return !e.Importado && !e.ImportIgnorada
		}); okp {
			rep, ok = pend, true // pendente -> em trânsito (sem flags terminais)
		} else if ign, oki := lowestCodigo(rows, func(e EmpresaImport) bool {
			return e.ImportIgnorada
		}); oki {
			rep, ok, st.ImportIgnorada = ign, true, true
		}
	}
	if !ok && len(rows) > 0 {
		rep = rows[0] // sem nenhum predicado (não deveria ocorrer) — fallback estável
	}
	applyMeta(&st, rep, rows)
	return st
}

// lowestCodigo returns the matching row with the smallest CODIGOEMPRESA (nil
// sorts last), plus whether any row matched.
func lowestCodigo(rows []EmpresaImport, pred func(EmpresaImport) bool) (EmpresaImport, bool) {
	var best EmpresaImport
	found := false
	for _, e := range rows {
		if !pred(e) {
			continue
		}
		if !found || lessCodigo(e, best) {
			best, found = e, true
		}
	}
	return best, found
}

func lessCodigo(a, b EmpresaImport) bool {
	if a.CodigoEmpresa == nil {
		return false
	}
	if b.CodigoEmpresa == nil {
		return true
	}
	return *a.CodigoEmpresa < *b.CodigoEmpresa
}

// applyMeta copies the representative row's fields into st. Empresa-specific
// fields (empresa code/name, motivo) come ONLY from the selected row. Nota-level
// fields (emitente/destinatário/data/valor/tipo) are identical across rows, so a
// gap on the selected row is backfilled from any sibling row.
func applyMeta(st *ImportState, rep EmpresaImport, rows []EmpresaImport) {
	st.CodigoEmpresa = rep.CodigoEmpresa
	st.CodigoFilial = rep.CodigoFilial
	st.NomeEmpresa = rep.NomeEmpresa
	st.Motivo = rep.Motivo
	st.Situacao = rep.Situacao
	st.TipoDocumento = rep.TipoDocumento
	st.CnpjEmitente = rep.CnpjEmitente
	st.NomeEmitente = rep.NomeEmitente
	st.CnpjDestinatario = rep.CnpjDestinatario
	st.NomeDestinatario = rep.NomeDestinatario
	st.DataEmissao = rep.DataEmissao
	st.ValorTotal = rep.ValorTotal
	for _, e := range rows {
		setIfEmpty(&st.TipoDocumento, e.TipoDocumento)
		setIfEmpty(&st.CnpjEmitente, e.CnpjEmitente)
		setIfEmpty(&st.NomeEmitente, e.NomeEmitente)
		setIfEmpty(&st.CnpjDestinatario, e.CnpjDestinatario)
		setIfEmpty(&st.NomeDestinatario, e.NomeDestinatario)
		setIfEmpty(&st.DataEmissao, e.DataEmissao)
		if st.ValorTotal == nil && e.ValorTotal != nil {
			st.ValorTotal = e.ValorTotal
		}
	}
}

func setIfEmpty(dst *string, v string) {
	if *dst == "" && v != "" {
		*dst = v
	}
}

func trimNull(s sql.NullString) string {
	if !s.Valid {
		return ""
	}
	return strings.TrimSpace(s.String)
}
