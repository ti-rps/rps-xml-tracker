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
	CnpjFilial       string // CNPJ da filial dona desta linha (TABFILIAL); define a direção
	DataEmissao      string // yyyy-mm-dd
	ValorTotal       *float64
	DataRobo         *time.Time // quando o robô importou (DATAROBO); nil se não passou pelo robô
	DataInclusao     *time.Time // quando a linha foi inserida (sempre preenchido, IDX12)
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
	CnpjFilial       string // CNPJ da filial selecionada (TABFILIAL); o poller deriva a direção dele
	DataEmissao      string // yyyy-mm-dd
	ValorTotal       *float64
	DataRobo         *time.Time // quando o robô importou (DATAROBO da linha selecionada); nil = não passou pelo robô
	DataInclusao     *time.Time // data de inserção da linha (DATAINCLUSAO; sempre presente — IDX12)
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

// brLoc é o fuso do Athenas (horário de Brasília). Carregado uma vez; sem tzdata no
// container, cai num offset fixo -03:00 (o Brasil não tem mais horário de verão desde
// 2019, então é constante para as datas que importam aqui).
var brLoc = func() *time.Location {
	if loc, err := time.LoadLocation("America/Sao_Paulo"); err == nil {
		return loc
	}
	return time.FixedZone("-03", -3*3600)
}()

// fbLocalTime reinterpreta um TIMESTAMP do Firebird como horário de Brasília. Os
// TIMESTAMP do Athenas são naive (sem fuso) e gravados em horário local; o driver os
// devolve com os componentes corretos do wall-clock mas rotulados como UTC, o que
// deslocaria o INSTANTE em 3h ao gravar no timestamptz do Postgres (e cruzaria a virada
// do dia em valores date-only/madrugada — ex.: a data 19/06 virava 18/06 21:00 BRT).
// Reconstruímos a partir dos componentes (que o driver preserva) fixando o fuso BRT, o
// que é idempotente independentemente do fuso que o driver tenha atribuído.
func fbLocalTime(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), brLoc)
}

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
	             t.EMITENTE, t.DESTINATARIO, t.DATAEMISSAO, t.VALORTOTAL, e.NOME, t.DATAROBO, t.DATAINCLUSAO,
	             fil.CNPJ
	      FROM TABLISTACHAVEACESSO t
	      LEFT JOIN TABEMPRESAS e ON e.CODIGO = t.CODIGOEMPRESA
	      LEFT JOIN TABFILIAL fil ON fil.CODIGOEMPRESA = t.CODIGOEMPRESA AND fil.CODIGO = t.CODIGOFILIAL
	      WHERE t.CHAVEACESSO IN (` + placeholders + `)`
	rows, err := r.db.QueryContext(ctx, q, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	return scanRows(rows, rowsByChave)
}

// SweepRecent retorna as chaves com linha TERMINAL (IMPORTADO=1 OU
// IMPORTACAOIGNORADA=1) e DATAINCLUSAO > since. Usa o índice IDX12 (DATAINCLUSAO,
// standalone, sempre preenchido) para varrer notas inseridas recentemente no Athenas
// — O(recentes), independente do tamanho do backlog in-flight. DATAROBO (só
// preenchido em lotes de robô) é lido como metadado mas NÃO é usado no filtro porque
// é NULL em muitos registros; o poller rotacional captura o que sobrar via
// ListInflightChaves.
//
// ATENÇÃO (chamador): o resultado vê SÓ as linhas terminais de cada chave. Para as
// importadas isso basta (imported vence tudo no selectState); mas uma candidata a
// IGNORADA pode ter linha PENDENTE de outra empresa (a dona) fora deste recorte —
// o poller re-resolve essas com Lookup completo antes de emitir (ver SweepOnce).
func (r *Reader) SweepRecent(ctx context.Context, since time.Time) (map[string]ImportState, error) {
	// Sweep por DATAINCLUSAO (IDX12 — standalone, sempre preenchido) em vez de
	// DATAROBO (IDX4 — só preenchido em importações via robô em lote; NULL nas demais).
	q := `SELECT FIRST 10000
	             t.CHAVEACESSO, t.IMPORTADO, t.IMPORTACAOIGNORADA, t.MOTIVOIGNORADOIMPORTACAO, t.SITUACAO,
	             t.TIPODOCUMENTO, t.CODIGOEMPRESA, t.CODIGOFILIAL, t.CNPJEMITENTE, t.CNPJDESTINATARIO,
	             t.EMITENTE, t.DESTINATARIO, t.DATAEMISSAO, t.VALORTOTAL, e.NOME, t.DATAROBO, t.DATAINCLUSAO,
	             fil.CNPJ
	      FROM TABLISTACHAVEACESSO t
	      LEFT JOIN TABEMPRESAS e ON e.CODIGO = t.CODIGOEMPRESA
	      LEFT JOIN TABFILIAL fil ON fil.CODIGOEMPRESA = t.CODIGOEMPRESA AND fil.CODIGO = t.CODIGOFILIAL
	      WHERE (t.IMPORTADO = 1 OR t.IMPORTACAOIGNORADA = 1)
	        AND t.DATAINCLUSAO > ?`
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

// ImportedSince retorna as chaves IMPORTADO=1 com DATAINCLUSAO na janela [since, until),
// opcionalmente de uma empresa. Diferente do SweepRecent (que é FIRST 10000, para o
// ticker), aqui NÃO há teto — a completude é o que importa para o reconcile; a janela +
// empresa é que limitam o volume. Usa o índice IDX12 (DATAINCLUSAO). READ-ONLY.
func (r *Reader) ImportedSince(ctx context.Context, since, until time.Time, codigoEmpresa, codigoFilial *int) (map[string]ImportState, error) {
	q := `SELECT t.CHAVEACESSO, t.IMPORTADO, t.IMPORTACAOIGNORADA, t.MOTIVOIGNORADOIMPORTACAO, t.SITUACAO,
	             t.TIPODOCUMENTO, t.CODIGOEMPRESA, t.CODIGOFILIAL, t.CNPJEMITENTE, t.CNPJDESTINATARIO,
	             t.EMITENTE, t.DESTINATARIO, t.DATAEMISSAO, t.VALORTOTAL, e.NOME, t.DATAROBO, t.DATAINCLUSAO,
	             fil.CNPJ
	      FROM TABLISTACHAVEACESSO t
	      LEFT JOIN TABEMPRESAS e ON e.CODIGO = t.CODIGOEMPRESA
	      LEFT JOIN TABFILIAL fil ON fil.CODIGOEMPRESA = t.CODIGOEMPRESA AND fil.CODIGO = t.CODIGOFILIAL
	      WHERE t.IMPORTADO = 1 AND t.DATAINCLUSAO >= ? AND t.DATAINCLUSAO < ?`
	args := []any{since, until}
	if codigoEmpresa != nil {
		q += ` AND t.CODIGOEMPRESA = ?`
		args = append(args, *codigoEmpresa)
	}
	if codigoFilial != nil {
		q += ` AND t.CODIGOFILIAL = ?`
		args = append(args, *codigoFilial)
	}
	rows, err := r.db.QueryContext(ctx, q, args...)
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

// Movimento é um lançamento fiscal do Athenas (TABENTRADASAIDA) — o "livro" que o painel
// de Entradas/Saídas mostra. Chave vazia = lançamento sem XML (digitado/TXT/Excel), que o
// tracker não tem como rastrear.
type Movimento struct {
	Chave         string // NFECHAVEACESSO; "" quando não veio de XML
	CodigoEmpresa *int
	CodigoFilial  *int
	Tipo          string // 'E' | 'S'
}

// MovimentosByRegistro retorna os lançamentos efetivados (EFETIVADA=1) da TABENTRADASAIDA
// com DATAREGISTRO na janela [from, until), opcionalmente por empresa e tipo (E/S). É a
// fonte do painel de Entradas/Saídas (data de MOVIMENTO). READ-ONLY. ATENÇÃO: inclui
// lançamentos sem XML (Chave=="") — o chamador separa os rastreáveis dos manuais.
func (r *Reader) MovimentosByRegistro(ctx context.Context, from, until time.Time, codigoEmpresa, codigoFilial *int, tipo string) ([]Movimento, error) {
	q := `SELECT S.NFECHAVEACESSO, S.CODIGOEMPRESA, S.CODIGOFILIAL, S.TIPO
	      FROM TABENTRADASAIDA S
	      WHERE S.EFETIVADA = 1 AND S.DATAREGISTRO >= ? AND S.DATAREGISTRO < ?`
	args := []any{from, until}
	if codigoEmpresa != nil {
		q += ` AND S.CODIGOEMPRESA = ?`
		args = append(args, *codigoEmpresa)
	}
	if codigoFilial != nil {
		q += ` AND S.CODIGOFILIAL = ?`
		args = append(args, *codigoFilial)
	}
	if tipo != "" {
		q += ` AND S.TIPO = ?`
		args = append(args, tipo)
	}
	rows, err := r.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Movimento
	for rows.Next() {
		var chave, t sql.NullString
		var ce, cf sql.NullInt64
		if err := rows.Scan(&chave, &ce, &cf, &t); err != nil {
			return nil, err
		}
		m := Movimento{Chave: strings.TrimSpace(trimNull(chave)), Tipo: trimNull(t)}
		if ce.Valid {
			v := int(ce.Int64)
			m.CodigoEmpresa = &v
		}
		if cf.Valid {
			v := int(cf.Int64)
			m.CodigoFilial = &v
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// Filial é uma filial cadastrada no Athenas (TABFILIAL): a chave composta
// (CODIGOEMPRESA, CODIGO) e o CNPJ do estabelecimento.
type Filial struct {
	CodigoEmpresa int
	CodigoFilial  int
	Cnpj          string
}

// ListFiliais lê todas as filiais (TABFILIAL) com seu CNPJ, para o backfill retroativo
// da direção. São poucas centenas de linhas — uma varredura barata. READ-ONLY.
func (r *Reader) ListFiliais(ctx context.Context) ([]Filial, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT CODIGOEMPRESA, CODIGO, CNPJ FROM TABFILIAL`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Filial
	for rows.Next() {
		var ce, cf sql.NullInt64
		var cnpj sql.NullString
		if err := rows.Scan(&ce, &cf, &cnpj); err != nil {
			return nil, err
		}
		if !ce.Valid || !cf.Valid {
			continue // sem chave composta -> não dá p/ casar com a nota
		}
		out = append(out, Filial{CodigoEmpresa: int(ce.Int64), CodigoFilial: int(cf.Int64), Cnpj: trimNull(cnpj)})
	}
	return out, rows.Err()
}

// validChave reporta se s tem a forma de uma chave de acesso NF-e/NFC-e/CT-e:
// exatamente 44 dígitos.
func validChave(s string) bool {
	if len(s) != 44 {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// scanRows escaneia as colunas padrão de TABLISTACHAVEACESSO (SELECT t.CHAVEACESSO,
// t.IMPORTADO, ..., e.NOME, t.DATAROBO, t.DATAINCLUSAO) em dst.
// Compartilhado por lookupChunk e SweepImported.
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
			dataRobo       sql.NullTime
			dataInclusao   sql.NullTime
			cnpjFil        sql.NullString
		)
		if err := rows.Scan(&chave, &imp, &ign, &motivo, &sit, &tipo,
			&codEmp, &codFil, &cnpjE, &cnpjD, &nomeE, &nomeD, &emissao, &valor,
			&nomeEmpresa, &dataRobo, &dataInclusao, &cnpjFil); err != nil {
			return err
		}
		chave = strings.TrimSpace(chave)
		if !validChave(chave) {
			// A TABLISTACHAVEACESSO aceita texto livre e tem linhas com chave
			// malformada (digitada errada, >44 chars). Elas nunca casam uma chave
			// real do tracker e estourariam o varchar(44) do Postgres na inserção
			// (22001, derrubando o lote inteiro) — descartadas na borda.
			continue
		}
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
			CnpjFilial:       trimNull(cnpjFil),
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
		if dataRobo.Valid {
			t := fbLocalTime(dataRobo.Time)
			e.DataRobo = &t
		}
		if dataInclusao.Valid {
			t := fbLocalTime(dataInclusao.Time)
			e.DataInclusao = &t
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
	st.CnpjFilial = rep.CnpjFilial // empresa-specific (da filial selecionada), como o nome
	st.Motivo = rep.Motivo
	st.Situacao = rep.Situacao
	st.TipoDocumento = rep.TipoDocumento
	st.CnpjEmitente = rep.CnpjEmitente
	st.NomeEmitente = rep.NomeEmitente
	st.CnpjDestinatario = rep.CnpjDestinatario
	st.NomeDestinatario = rep.NomeDestinatario
	st.DataEmissao = rep.DataEmissao
	st.ValorTotal = rep.ValorTotal
	st.DataRobo = rep.DataRobo
	st.DataInclusao = rep.DataInclusao
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
