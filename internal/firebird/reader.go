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

	_ "github.com/nakagami/firebirdsql"
)

// Reader holds a read-only connection pool to the Athenas Firebird DB.
type Reader struct {
	db *sql.DB
}

// ImportState is the aggregated import status of one chave. A chave may have
// more than one row (nota + events); we OR the flags so "imported" wins.
type ImportState struct {
	Chave          string
	Found          bool
	Importado      bool
	ImportIgnorada bool
	Motivo         string
	Situacao       *int
	TipoDocumento  string
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
// result map were not found in Athenas. READ-ONLY (SELECT only).
func (r *Reader) Lookup(ctx context.Context, chaves []string) (map[string]ImportState, error) {
	out := make(map[string]ImportState, len(chaves))
	for start := 0; start < len(chaves); start += chunkSize {
		end := start + chunkSize
		if end > len(chaves) {
			end = len(chaves)
		}
		if err := r.lookupChunk(ctx, chaves[start:end], out); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (r *Reader) lookupChunk(ctx context.Context, chaves []string, out map[string]ImportState) error {
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(chaves)), ",")
	args := make([]any, len(chaves))
	for i, c := range chaves {
		args[i] = c
	}
	q := `SELECT CHAVEACESSO, IMPORTADO, IMPORTACAOIGNORADA, MOTIVOIGNORADOIMPORTACAO, SITUACAO, TIPODOCUMENTO
	      FROM TABLISTACHAVEACESSO WHERE CHAVEACESSO IN (` + placeholders + `)`
	rows, err := r.db.QueryContext(ctx, q, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			chave  string
			imp    sql.NullInt64
			ign    sql.NullInt64
			motivo sql.NullString
			sit    sql.NullInt64
			tipo   sql.NullString
		)
		if err := rows.Scan(&chave, &imp, &ign, &motivo, &sit, &tipo); err != nil {
			return err
		}
		chave = strings.TrimSpace(chave)
		st := out[chave] // zero value first time
		st.Chave = chave
		st.Found = true
		if imp.Valid && imp.Int64 == 1 {
			st.Importado = true
		}
		if ign.Valid && ign.Int64 == 1 {
			st.ImportIgnorada = true
		}
		if motivo.Valid && st.Motivo == "" {
			st.Motivo = strings.TrimSpace(motivo.String)
		}
		if sit.Valid {
			v := int(sit.Int64)
			st.Situacao = &v
		}
		if tipo.Valid && st.TipoDocumento == "" {
			st.TipoDocumento = strings.TrimSpace(tipo.String)
		}
		out[chave] = st
	}
	return rows.Err()
}
