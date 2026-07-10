// Queries de INVESTIGAÇÃO da fase 0 do shadow-sync (design/SHADOW-SYNC.md §5-§7):
// descobrir o INSERT mínimo compatível com o DownloadXML, o mecanismo da PK
// (trigger/generator), a prevalência do multi-participação e validar a derivação
// de URL. Tudo aqui é READ-ONLY (SELECT, inclusive GEN_ID com incremento 0) e é
// consumido pelos modos --profile-insert / --watch-chave / --check-path do repoll.
package firebird

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode/utf8"
)

// toUTF8 garante UTF-8 válido nos textos que vamos IMPRIMIR/COMPARAR: a conexão é
// charset=NONE e o banco fala Latin-1 (mesma situação do toUTF8 do poller, que
// transcodifica na borda do Postgres — aqui a borda é o relatório/comparação).
func toUTF8(s string) string {
	if utf8.ValidString(s) {
		return s
	}
	r := make([]rune, 0, len(s))
	for i := 0; i < len(s); i++ {
		r = append(r, rune(s[i]))
	}
	return string(r)
}

// identRe valida identificadores Firebird que interpolamos em SQL (nomes de
// tabela/coluna/generator vindos do NOSSO código ou do próprio RDB$ — nunca do
// usuário final; a validação é cinto de segurança).
var identRe = regexp.MustCompile(`^[A-Z][A-Z0-9_$]*$`)

func checkIdent(name string) (string, error) {
	up := strings.ToUpper(strings.TrimSpace(name))
	if !identRe.MatchString(up) {
		return "", fmt.Errorf("identificador inválido: %q", name)
	}
	return up, nil
}

// ColumnInfo descreve uma coluna da tabela (via RDB$RELATION_FIELDS/RDB$FIELDS).
type ColumnInfo struct {
	Name     string
	Type     string // ex.: VARCHAR(255), INTEGER, TIMESTAMP, BLOB TEXT
	NotNull  bool
	Position int
}

// TableColumns lê o DDL efetivo (nome, tipo, NOT NULL) direto do dicionário —
// não dependemos de um dump de DDL desatualizado.
func (r *Reader) TableColumns(ctx context.Context, table string) ([]ColumnInfo, error) {
	tbl, err := checkIdent(table)
	if err != nil {
		return nil, err
	}
	q := `SELECT rf.RDB$FIELD_NAME, f.RDB$FIELD_TYPE, COALESCE(f.RDB$FIELD_SUB_TYPE,0),
	             COALESCE(f.RDB$CHARACTER_LENGTH, f.RDB$FIELD_LENGTH, 0), COALESCE(f.RDB$FIELD_SCALE,0),
	             COALESCE(rf.RDB$NULL_FLAG, 0), rf.RDB$FIELD_POSITION
	      FROM RDB$RELATION_FIELDS rf
	      JOIN RDB$FIELDS f ON f.RDB$FIELD_NAME = rf.RDB$FIELD_SOURCE
	      WHERE rf.RDB$RELATION_NAME = ?
	      ORDER BY rf.RDB$FIELD_POSITION`
	rows, err := r.db.QueryContext(ctx, q, tbl)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ColumnInfo
	for rows.Next() {
		var name string
		var ftype, subtype, length, scale, notNull, pos int
		if err := rows.Scan(&name, &ftype, &subtype, &length, &scale, &notNull, &pos); err != nil {
			return nil, err
		}
		out = append(out, ColumnInfo{
			Name:     strings.TrimSpace(name),
			Type:     fbTypeName(ftype, subtype, length, scale),
			NotNull:  notNull != 0,
			Position: pos,
		})
	}
	return out, rows.Err()
}

// fbTypeName traduz os códigos de RDB$FIELDS para o nome SQL usual.
func fbTypeName(ftype, subtype, length, scale int) string {
	if scale < 0 {
		return fmt.Sprintf("NUMERIC(?,%d)", -scale)
	}
	switch ftype {
	case 7:
		return "SMALLINT"
	case 8:
		return "INTEGER"
	case 10:
		return "FLOAT"
	case 12:
		return "DATE"
	case 13:
		return "TIME"
	case 14:
		return fmt.Sprintf("CHAR(%d)", length)
	case 16:
		return "BIGINT"
	case 27:
		return "DOUBLE PRECISION"
	case 35:
		return "TIMESTAMP"
	case 37:
		return fmt.Sprintf("VARCHAR(%d)", length)
	case 261:
		if subtype == 1 {
			return "BLOB TEXT"
		}
		return "BLOB"
	}
	return fmt.Sprintf("TYPE_%d", ftype)
}

// TriggerInfo é uma trigger de usuário da tabela (para descobrir se a PK vem de
// um BEFORE INSERT com GEN_ID ou se o app preenche client-side).
type TriggerInfo struct {
	Name     string
	Inactive bool
	Source   string
}

// TableTriggers lista as triggers NÃO-sistema da tabela, com o fonte.
func (r *Reader) TableTriggers(ctx context.Context, table string) ([]TriggerInfo, error) {
	tbl, err := checkIdent(table)
	if err != nil {
		return nil, err
	}
	q := `SELECT RDB$TRIGGER_NAME, COALESCE(RDB$TRIGGER_INACTIVE,0), RDB$TRIGGER_SOURCE
	      FROM RDB$TRIGGERS
	      WHERE RDB$RELATION_NAME = ? AND COALESCE(RDB$SYSTEM_FLAG, 0) = 0`
	rows, err := r.db.QueryContext(ctx, q, tbl)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []TriggerInfo
	for rows.Next() {
		var name string
		var inactive int
		var src sql.NullString
		if err := rows.Scan(&name, &inactive, &src); err != nil {
			return nil, err
		}
		out = append(out, TriggerInfo{
			Name:     strings.TrimSpace(name),
			Inactive: inactive != 0,
			Source:   toUTF8(strings.TrimSpace(src.String)),
		})
	}
	return out, rows.Err()
}

// Generators lista os generators de usuário cujo nome contém o termo dado.
func (r *Reader) Generators(ctx context.Context, containing string) ([]string, error) {
	q := `SELECT RDB$GENERATOR_NAME FROM RDB$GENERATORS
	      WHERE COALESCE(RDB$SYSTEM_FLAG,0) = 0 AND RDB$GENERATOR_NAME CONTAINING ?`
	rows, err := r.db.QueryContext(ctx, q, containing)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		out = append(out, strings.TrimSpace(name))
	}
	return out, rows.Err()
}

// GeneratorValue lê o valor ATUAL do generator sem consumi-lo (GEN_ID com
// incremento 0 — read-only). Comparar com MAX(PK) diz qual generator o app usa.
func (r *Reader) GeneratorValue(ctx context.Context, name string) (int64, error) {
	gen, err := checkIdent(name)
	if err != nil {
		return 0, err
	}
	var v int64
	err = r.db.QueryRowContext(ctx, `SELECT GEN_ID(`+gen+`, 0) FROM RDB$DATABASE`).Scan(&v)
	return v, err
}

// MaxBigint retorna MAX(col) — usado no MAX da PK (indexada, instantâneo).
func (r *Reader) MaxBigint(ctx context.Context, table, col string) (*int64, error) {
	tbl, err := checkIdent(table)
	if err != nil {
		return nil, err
	}
	c, err := checkIdent(col)
	if err != nil {
		return nil, err
	}
	var v sql.NullInt64
	if err := r.db.QueryRowContext(ctx, `SELECT MAX(`+c+`) FROM `+tbl).Scan(&v); err != nil {
		return nil, err
	}
	if !v.Valid {
		return nil, nil
	}
	return &v.Int64, nil
}

// FillGroup é o fill-rate de um grupo (ou da tabela toda, Group==""): quantas
// linhas na janela e, por coluna, quantas têm a coluna preenchida (COUNT(col)).
type FillGroup struct {
	Group  string
	Total  int64
	Filled map[string]int64
}

// fillChunk limita quantos COUNT(col) agregamos por SELECT.
const fillChunk = 30

// FillProfile mede o fill-rate de TODAS as colunas dadas nas linhas com
// DATAINCLUSAO >= since, opcionalmente agrupado por uma coluna (TIPODOCUMENTO,
// TIPO...). É a query que revela o "INSERT mínimo" do DownloadXML: colunas
// sempre preenchidas vs às vezes vs nunca.
func (r *Reader) FillProfile(ctx context.Context, table string, cols []string, since time.Time, groupBy string) ([]FillGroup, error) {
	tbl, err := checkIdent(table)
	if err != nil {
		return nil, err
	}
	var grp string
	if groupBy != "" {
		if grp, err = checkIdent(groupBy); err != nil {
			return nil, err
		}
	}
	byKey := map[string]*FillGroup{}
	for start := 0; start < len(cols); start += fillChunk {
		end := min(start+fillChunk, len(cols))
		chunk := make([]string, 0, end-start)
		for _, c := range cols[start:end] {
			cc, err := checkIdent(c)
			if err != nil {
				return nil, err
			}
			chunk = append(chunk, cc)
		}
		if err := r.fillProfileChunk(ctx, tbl, chunk, since, grp, byKey); err != nil {
			return nil, err
		}
	}
	out := make([]FillGroup, 0, len(byKey))
	for _, g := range byKey {
		out = append(out, *g)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Total > out[j].Total })
	return out, nil
}

func (r *Reader) fillProfileChunk(ctx context.Context, tbl string, cols []string, since time.Time, grp string, byKey map[string]*FillGroup) error {
	sel := "COUNT(*)"
	for _, c := range cols {
		sel += ", COUNT(" + c + ")"
	}
	q := "SELECT " + sel + " FROM " + tbl + " WHERE DATAINCLUSAO >= ?"
	if grp != "" {
		q = "SELECT " + grp + ", " + sel + " FROM " + tbl + " WHERE DATAINCLUSAO >= ? GROUP BY " + grp
	}
	rows, err := r.db.QueryContext(ctx, q, dateFloor(since))
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		counts := make([]int64, len(cols)+1)
		dest := make([]any, 0, len(cols)+2)
		var gv any
		if grp != "" {
			dest = append(dest, &gv)
		}
		for i := range counts {
			dest = append(dest, &counts[i])
		}
		if err := rows.Scan(dest...); err != nil {
			return err
		}
		key := ""
		if grp != "" {
			key, _ = formatVal(gv)
		}
		g := byKey[key]
		if g == nil {
			g = &FillGroup{Group: key, Filled: map[string]int64{}}
			byKey[key] = g
		}
		g.Total = counts[0]
		for i, c := range cols {
			g.Filled[c] = counts[i+1]
		}
	}
	return rows.Err()
}

// RawRow é UMA linha da tabela com todas as colunas como texto (nil = NULL) —
// para o --watch-chave imprimir/diffar exatamente o que o DownloadXML gravou.
type RawRow struct {
	Columns []string
	Values  []*string
}

// RawRowsByChave retorna todas as linhas da chave (uma por empresa) com as
// colunas dadas, formatadas como texto.
func (r *Reader) RawRowsByChave(ctx context.Context, table string, cols []string, chave string) ([]RawRow, error) {
	tbl, err := checkIdent(table)
	if err != nil {
		return nil, err
	}
	checked := make([]string, len(cols))
	for i, c := range cols {
		if checked[i], err = checkIdent(c); err != nil {
			return nil, err
		}
	}
	q := "SELECT " + strings.Join(checked, ", ") + " FROM " + tbl + " WHERE CHAVEACESSO = ?"
	rows, err := r.db.QueryContext(ctx, q, chave)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []RawRow
	for rows.Next() {
		raw := make([]any, len(checked))
		dest := make([]any, len(checked))
		for i := range raw {
			dest[i] = &raw[i]
		}
		if err := rows.Scan(dest...); err != nil {
			return nil, err
		}
		row := RawRow{Columns: checked, Values: make([]*string, len(checked))}
		for i, v := range raw {
			if s, ok := formatVal(v); ok {
				row.Values[i] = &s
			}
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// formatVal formata um valor escaneado genericamente. ok=false para NULL.
func formatVal(v any) (string, bool) {
	switch t := v.(type) {
	case nil:
		return "", false
	case []byte:
		return toUTF8(strings.TrimSpace(string(t))), true
	case string:
		return toUTF8(strings.TrimSpace(t)), true
	case time.Time:
		// timestamps do Athenas são naive; exibimos o wall-clock como gravado
		if t.Hour() == 0 && t.Minute() == 0 && t.Second() == 0 {
			return t.Format("2006-01-02"), true
		}
		return t.Format("2006-01-02 15:04:05"), true
	default:
		return fmt.Sprint(t), true
	}
}

// URLRow é uma linha recente com URL preenchida + o contexto para a derivação
// (--check-path): dela saem os insumos do syncpath.Derive E o gabarito (URL real).
type URLRow struct {
	Chave            string
	URL              string
	CodigoEmpresa    *int
	CodigoFilial     *int
	NomeEmpresa      string // TABEMPRESAS.NOME (fallback)
	NomeFilial       string // TABFILIAL.NOME — a fonte real do 1º segmento da URL
	CnpjFilial       string // TABFILIAL.CNPJ
	TipoDocumento    string
	Tipo             string // semântica E/S a confirmar — o relatório cruza com o segmento da URL
	CnpjEmitente     string
	CnpjDestinatario string
	DataEmissao      string // yyyy-mm-dd
	DataInclusao     *time.Time
	TpEvento         string // preenchido => evento (padrão de URL próprio, fora do piloto)
	ChaveSubs        string // preenchido => substituta
}

// RecentURLRows amostra linhas com URL preenchida e DATAINCLUSAO >= since.
// Sem ORDER BY de propósito: ordenar milhões de linhas da janela só para
// amostrar N não vale o custo — a amostra sai do índice IDX12 na ordem que vier.
// hasTpEvento/hasChaveSubs vêm do TableColumns (colunas que podem não existir
// nesta versão do Athenas).
func (r *Reader) RecentURLRows(ctx context.Context, since time.Time, limit int, hasTpEvento, hasChaveSubs bool) ([]URLRow, error) {
	if limit <= 0 {
		limit = 1000
	}
	extra := ""
	if hasTpEvento {
		extra += ", t.TPEVENTO"
	}
	if hasChaveSubs {
		extra += ", t.CHAVEACESSOSUBS"
	}
	q := fmt.Sprintf(`SELECT FIRST %d t.CHAVEACESSO, t.URL, t.CODIGOEMPRESA, t.CODIGOFILIAL, e.NOME, fil.NOME, fil.CNPJ,
	             t.TIPODOCUMENTO, t.TIPO, t.CNPJEMITENTE, t.CNPJDESTINATARIO, t.DATAEMISSAO, t.DATAINCLUSAO%s
	      FROM TABLISTACHAVEACESSO t
	      LEFT JOIN TABEMPRESAS e ON e.CODIGO = t.CODIGOEMPRESA
	      LEFT JOIN TABFILIAL fil ON fil.CODIGOEMPRESA = t.CODIGOEMPRESA AND fil.CODIGO = t.CODIGOFILIAL
	      WHERE t.DATAINCLUSAO >= ? AND t.URL IS NOT NULL AND t.URL <> ''`, limit, extra)
	rows, err := r.db.QueryContext(ctx, q, dateFloor(since))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []URLRow
	for rows.Next() {
		var (
			chave, url                            string
			ce, cf                                sql.NullInt64
			nome, nomeFil, cnpjFil, tipoDoc, tipo sql.NullString
			cnpjE, cnpjD                          sql.NullString
			emissao, inclusao                     sql.NullTime
			tpEvento, chaveSubs                   sql.NullString
		)
		dest := []any{&chave, &url, &ce, &cf, &nome, &nomeFil, &cnpjFil, &tipoDoc, &tipo, &cnpjE, &cnpjD, &emissao, &inclusao}
		if hasTpEvento {
			dest = append(dest, &tpEvento)
		}
		if hasChaveSubs {
			dest = append(dest, &chaveSubs)
		}
		if err := rows.Scan(dest...); err != nil {
			return nil, err
		}
		u := URLRow{
			Chave:            strings.TrimSpace(chave),
			URL:              toUTF8(strings.TrimSpace(url)),
			NomeEmpresa:      toUTF8(trimNull(nome)),
			NomeFilial:       toUTF8(trimNull(nomeFil)),
			CnpjFilial:       trimNull(cnpjFil),
			TipoDocumento:    trimNull(tipoDoc),
			Tipo:             trimNull(tipo),
			CnpjEmitente:     trimNull(cnpjE),
			CnpjDestinatario: trimNull(cnpjD),
			TpEvento:         trimNull(tpEvento),
			ChaveSubs:        trimNull(chaveSubs),
		}
		if ce.Valid {
			v := int(ce.Int64)
			u.CodigoEmpresa = &v
		}
		if cf.Valid {
			v := int(cf.Int64)
			u.CodigoFilial = &v
		}
		if emissao.Valid {
			u.DataEmissao = emissao.Time.Format("2006-01-02")
		}
		if inclusao.Valid {
			t := fbLocalTime(inclusao.Time)
			u.DataInclusao = &t
		}
		out = append(out, u)
	}
	return out, rows.Err()
}

// MultiPartStats dimensiona o multi-participação (§0 do design): quantas chaves
// da janela têm 2+ empresas e em que combinação de status as participações estão
// (importada+pendente é o ponto cego do modelo atual).
type MultiPartStats struct {
	ChavesTotal int64
	ChavesMulti int64
	Combos      map[string]int64 // "1 importada + 1 pendente (2 part.)" -> nº de chaves
}

// MultiParticipacao roda o levantamento na janela DATAINCLUSAO >= since.
// ATENÇÃO: agrega por chave (GROUP BY) sobre a janela inteira — pode levar
// minutos; é ferramenta one-off de investigação.
func (r *Reader) MultiParticipacao(ctx context.Context, since time.Time) (MultiPartStats, error) {
	st := MultiPartStats{Combos: map[string]int64{}}
	floor := dateFloor(since)
	if err := r.db.QueryRowContext(ctx,
		`SELECT COUNT(DISTINCT CHAVEACESSO) FROM TABLISTACHAVEACESSO WHERE DATAINCLUSAO >= ?`,
		floor).Scan(&st.ChavesTotal); err != nil {
		return st, err
	}
	q := `SELECT SUM(CASE WHEN IMPORTADO = 1 THEN 1 ELSE 0 END),
	             SUM(CASE WHEN IMPORTADO <> 1 AND COALESCE(IMPORTACAOIGNORADA,0) = 0 THEN 1 ELSE 0 END),
	             SUM(CASE WHEN COALESCE(IMPORTACAOIGNORADA,0) = 1 THEN 1 ELSE 0 END),
	             COUNT(*)
	      FROM TABLISTACHAVEACESSO
	      WHERE DATAINCLUSAO >= ?
	      GROUP BY CHAVEACESSO
	      HAVING COUNT(DISTINCT CODIGOEMPRESA) > 1`
	rows, err := r.db.QueryContext(ctx, q, floor)
	if err != nil {
		return st, err
	}
	defer rows.Close()
	for rows.Next() {
		var imp, pend, ign, part int64
		if err := rows.Scan(&imp, &pend, &ign, &part); err != nil {
			return st, err
		}
		st.ChavesMulti++
		st.Combos[comboLabel(imp, pend, ign, part)]++
	}
	return st, rows.Err()
}

func comboLabel(imp, pend, ign, part int64) string {
	var parts []string
	if imp > 0 {
		parts = append(parts, fmt.Sprintf("%d importada(s)", imp))
	}
	if pend > 0 {
		parts = append(parts, fmt.Sprintf("%d pendente(s)", pend))
	}
	if ign > 0 {
		parts = append(parts, fmt.Sprintf("%d ignorada(s)", ign))
	}
	return fmt.Sprintf("%s (%d part.)", strings.Join(parts, " + "), part)
}

// MultiURLRow é uma linha-irmã de uma chave multi-empresa, para responder: as
// URLs divergem entre as empresas (uma CÓPIA física por participante)?
type MultiURLRow struct {
	Chave         string
	CodigoEmpresa *int
	Tipo          string
	URL           string
}

// MultiParticipacaoURLs amostra as linhas-irmãs (com todas as colunas de
// interesse) das chaves multi-empresa da janela, ordenadas por chave.
func (r *Reader) MultiParticipacaoURLs(ctx context.Context, since time.Time, limit int) ([]MultiURLRow, error) {
	if limit <= 0 {
		limit = 200
	}
	q := fmt.Sprintf(`SELECT FIRST %d t.CHAVEACESSO, t.CODIGOEMPRESA, t.TIPO, t.URL
	      FROM TABLISTACHAVEACESSO t
	      WHERE t.CHAVEACESSO IN (SELECT CHAVEACESSO FROM TABLISTACHAVEACESSO
	                              WHERE DATAINCLUSAO >= ?
	                              GROUP BY CHAVEACESSO
	                              HAVING COUNT(DISTINCT CODIGOEMPRESA) > 1)
	        AND t.DATAINCLUSAO >= ?
	      ORDER BY t.CHAVEACESSO, t.CODIGOEMPRESA`, limit)
	floor := dateFloor(since)
	rows, err := r.db.QueryContext(ctx, q, floor, floor)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []MultiURLRow
	for rows.Next() {
		var chave string
		var ce sql.NullInt64
		var tipo, url sql.NullString
		if err := rows.Scan(&chave, &ce, &tipo, &url); err != nil {
			return nil, err
		}
		m := MultiURLRow{Chave: strings.TrimSpace(chave), Tipo: trimNull(tipo), URL: toUTF8(trimNull(url))}
		if ce.Valid {
			v := int(ce.Int64)
			m.CodigoEmpresa = &v
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// MesesCount é um bucket do histograma emissão→importação em meses-calendário.
type MesesCount struct {
	Meses *int // nil quando DATAEMISSAO é NULL
	Count int64
}

// ImportWindowMeses confirma a janela do AthenasHorse: para as importadas com
// DATAROBO >= since, quantos meses-calendário entre a EMISSÃO e a importação.
// A tese (fiscal): só mês atual (0) e anterior (1).
func (r *Reader) ImportWindowMeses(ctx context.Context, since time.Time) ([]MesesCount, error) {
	q := `SELECT (EXTRACT(YEAR FROM DATAROBO)*12 + EXTRACT(MONTH FROM DATAROBO))
	           - (EXTRACT(YEAR FROM DATAEMISSAO)*12 + EXTRACT(MONTH FROM DATAEMISSAO)),
	             COUNT(*)
	      FROM TABLISTACHAVEACESSO
	      WHERE IMPORTADO = 1 AND DATAROBO >= ?
	      GROUP BY (EXTRACT(YEAR FROM DATAROBO)*12 + EXTRACT(MONTH FROM DATAROBO))
	             - (EXTRACT(YEAR FROM DATAEMISSAO)*12 + EXTRACT(MONTH FROM DATAEMISSAO))
	      ORDER BY 1`
	rows, err := r.db.QueryContext(ctx, q, dateFloor(since))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []MesesCount
	for rows.Next() {
		var meses sql.NullInt64
		var count int64
		if err := rows.Scan(&meses, &count); err != nil {
			return nil, err
		}
		mc := MesesCount{Count: count}
		if meses.Valid {
			v := int(meses.Int64)
			mc.Meses = &v
		}
		out = append(out, mc)
	}
	return out, rows.Err()
}

// ComboCount é uma combinação de valores + contagem (perfil pendente vs importada).
type ComboCount struct {
	Importado int
	Values    []*string // SITUACAO, CODIGOTIPOMOVIMENTO, SEMDEPARA (nil = NULL)
	Count     int64
}

// PendingVsImportedCols são as colunas cruzadas com IMPORTADO no perfil dos
// "descartes silenciosos" (pendente eterna DENTRO da janela — o que a separa?).
var PendingVsImportedCols = []string{"SITUACAO", "CODIGOTIPOMOVIMENTO", "SEMDEPARA"}

// PendingVsImported compara pendentes × importadas com EMISSÃO >= since (dentro
// da janela do robô), não-ignoradas, agrupando pelas colunas candidatas a
// explicar por que uma pendente nunca importa.
func (r *Reader) PendingVsImported(ctx context.Context, emissaoSince time.Time) ([]ComboCount, error) {
	q := `SELECT IMPORTADO, SITUACAO, CODIGOTIPOMOVIMENTO, SEMDEPARA, COUNT(*)
	      FROM TABLISTACHAVEACESSO
	      WHERE DATAEMISSAO >= ? AND COALESCE(IMPORTACAOIGNORADA,0) = 0
	      GROUP BY IMPORTADO, SITUACAO, CODIGOTIPOMOVIMENTO, SEMDEPARA
	      ORDER BY 5 DESC`
	rows, err := r.db.QueryContext(ctx, q, dateFloor(emissaoSince))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ComboCount
	for rows.Next() {
		var imp sql.NullInt64
		vals := make([]any, len(PendingVsImportedCols))
		dest := []any{&imp}
		for i := range vals {
			dest = append(dest, &vals[i])
		}
		var count int64
		dest = append(dest, &count)
		if err := rows.Scan(dest...); err != nil {
			return nil, err
		}
		cc := ComboCount{Importado: int(imp.Int64), Count: count, Values: make([]*string, len(vals))}
		for i, v := range vals {
			if s, ok := formatVal(v); ok {
				cc.Values[i] = &s
			}
		}
		out = append(out, cc)
	}
	return out, rows.Err()
}

// ChaveURLRow é a visão enxuta de uma linha p/ o --check-plans: quem o
// DownloadXML criou (empresa/filial), com que URL, e se já importou.
type ChaveURLRow struct {
	Chave         string
	CodigoEmpresa *int
	CodigoFilial  *int
	URL           string
	Importado     bool
}

// URLsByChaves retorna as linhas (todas as participações) das chaves dadas, só
// com as colunas que o diff plano×realidade precisa. Chunked, READ-ONLY.
func (r *Reader) URLsByChaves(ctx context.Context, chaves []string) (map[string][]ChaveURLRow, error) {
	out := make(map[string][]ChaveURLRow, len(chaves))
	for start := 0; start < len(chaves); start += chunkSize {
		end := min(start+chunkSize, len(chaves))
		placeholders := strings.TrimSuffix(strings.Repeat("?,", end-start), ",")
		args := make([]any, 0, end-start)
		for _, c := range chaves[start:end] {
			args = append(args, c)
		}
		rows, err := r.db.QueryContext(ctx, `
			SELECT CHAVEACESSO, CODIGOEMPRESA, CODIGOFILIAL, URL, IMPORTADO
			FROM TABLISTACHAVEACESSO WHERE CHAVEACESSO IN (`+placeholders+`)`, args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var chave string
			var ce, cf, imp sql.NullInt64
			var url sql.NullString
			if err := rows.Scan(&chave, &ce, &cf, &url, &imp); err != nil {
				rows.Close()
				return nil, err
			}
			row := ChaveURLRow{
				Chave:     strings.TrimSpace(chave),
				URL:       toUTF8(trimNull(url)),
				Importado: imp.Valid && imp.Int64 == 1,
			}
			if ce.Valid {
				v := int(ce.Int64)
				row.CodigoEmpresa = &v
			}
			if cf.Valid {
				v := int(cf.Int64)
				row.CodigoFilial = &v
			}
			out[row.Chave] = append(out[row.Chave], row)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, err
		}
		rows.Close()
	}
	return out, nil
}

// ValueCount é um valor distinto de uma coluna + quantas linhas o têm.
type ValueCount struct {
	Value *string // nil = NULL
	Count int64
}

// DistinctCounts perfila os valores de uma coluna de semântica desconhecida
// (ORIGEM, SITUACAO, DOWNLOAD...) nas linhas com DATAINCLUSAO >= since.
func (r *Reader) DistinctCounts(ctx context.Context, table, col string, since time.Time) ([]ValueCount, error) {
	tbl, err := checkIdent(table)
	if err != nil {
		return nil, err
	}
	c, err := checkIdent(col)
	if err != nil {
		return nil, err
	}
	q := "SELECT " + c + ", COUNT(*) FROM " + tbl + " WHERE DATAINCLUSAO >= ? GROUP BY " + c + " ORDER BY 2 DESC"
	rows, err := r.db.QueryContext(ctx, q, dateFloor(since))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ValueCount
	for rows.Next() {
		var v any
		var count int64
		if err := rows.Scan(&v, &count); err != nil {
			return nil, err
		}
		vc := ValueCount{Count: count}
		if s, ok := formatVal(v); ok {
			vc.Value = &s
		}
		out = append(out, vc)
	}
	return out, rows.Err()
}
