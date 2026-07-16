// Writer é a ÚNICA porta de ESCRITA do tracker no Firebird do Athenas
// (shadow-sync F1): insere linhas na TABLISTACHAVEACESSO com IMPORTADO=0 para o
// AthenasHorse importar — exatamente o que o DownloadXML faz, com o INSERT
// mínimo mapeado empiricamente na F0 (design/SHADOW-SYNC-F0-ACHADOS.md §3/§9).
//
// Usa um DSN PRÓPRIO (TRACKER_FB_WRITE_DSN): o poller continua com a credencial
// read-only e o raio de dano da credencial de escrita fica restrito ao syncer.
//
// Decisões herdadas da F0:
//   - PK client-side via GEN_ID(GEN_CHAVEACESSOXML, 1) — não há trigger que a gere;
//   - NUNCA gravar TIPODOCUMENTO/TIPO: a trigger CHECK_FORCAIMPORTACAO auto-importa
//     NFe com ORIGEM=1 se a nota já está no livro, e o DownloadXML os deixa NULL;
//   - texto transcodificado UTF-8 → Latin-1 (conexão charset=NONE, banco Latin-1) —
//     o inverso do toUTF8 do poller; sem isso o Athenas exibe mojibake;
//   - marcador de autoria em OBSERVACOES ("sync rps-xml-tracker ...") — é o que
//     permite rollback limpo (DELETE só das NOSSAS linhas com IMPORTADO=0).
package firebird

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// Writer segura o pool de escrita. Uma conexão basta: o syncer processa uma
// participação por vez (MAX_PER_CYCLE do piloto é 1).
type Writer struct {
	db *sql.DB
}

// generatorChave é o generator que o Athenas usa para a PK (F0: valor == MAX(PK)).
const generatorChave = "GEN_CHAVEACESSOXML"

// NewWriter abre o pool de escrita. Mesmo formato de DSN do reader
// (Legacy_Auth, wire_crypt=disabled), mas com a credencial DE ESCRITA.
func NewWriter(ctx context.Context, dsn string) (*Writer, error) {
	db, err := sql.Open("firebirdsql", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	// Mesma prevenção do Reader: conexão ociosa é presa fácil do kill
	// administrativo do Firebird — expira antes de reutilizar. SEM retry de
	// escrita aqui: repetir um INSERT cuja resposta se perdeu arriscaria linha
	// duplicada; a falha sobe e o Execute retoma com segurança (pre-checks).
	db.SetConnMaxIdleTime(2 * time.Minute)
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping firebird (write dsn): %w", err)
	}
	return &Writer{db: db}, nil
}

func (w *Writer) Close() error { return w.db.Close() }

// NextChaveID consome o próximo valor do generator da PK (GEN_ID com incremento
// 1). É a única forma segura: nenhuma trigger preenche a PK neste banco.
func (w *Writer) NextChaveID(ctx context.Context) (int64, error) {
	var id int64
	err := w.db.QueryRowContext(ctx,
		`SELECT GEN_ID(`+generatorChave+`, 1) FROM RDB$DATABASE`).Scan(&id)
	return id, err
}

// InsertRow são os valores de UMA participação (chave + empresa/filial) para o
// INSERT. Campos de texto em UTF-8 — a transcodificação para Latin-1 acontece
// aqui dentro, na borda da escrita.
type InsertRow struct {
	Chave            string
	CodigoEmpresa    int
	CodigoFilial     int
	CnpjEmitente     string
	CnpjDestinatario string // vazio em NFCe/saída sem destinatário identificado
	Emitente         string
	Destinatario     string
	DataEmissao      time.Time
	ValorTotal       *float64
	URL              string // caminho relativo derivado (internal/syncpath)
	CaminhoOriginal  string // caminho UNC de origem na ASINCRONIZAR (vai em CAMINHOORIGINAL e MENSAGEM)
	Observacoes      string // marcador de autoria do tracker
}

// InsertChaveAcesso grava a linha da participação com IMPORTADO=0. O conjunto de
// colunas e os valores fixos espelham o INSERT real do DownloadXML capturado na
// F0 (--watch-chave): ORIGEM=1, DOWNLOAD=1, UPLOAD=0, contadores zerados, DATA =
// 1º dia do mês da emissão, SERIE = posições 23-25 da chave, NUMERODOCUMENTO =
// nNF. IMPORTADO=0 explícito (a trigger BI1 força de qualquer jeito).
func (w *Writer) InsertChaveAcesso(ctx context.Context, id int64, r InsertRow) error {
	if len(r.Chave) != 44 {
		return fmt.Errorf("insert: chave precisa ter 44 dígitos: %q", r.Chave)
	}
	serie := r.Chave[22:25]
	numero := strings.TrimLeft(r.Chave[25:34], "0")
	dataCompetencia := time.Date(r.DataEmissao.Year(), r.DataEmissao.Month(), 1, 0, 0, 0, 0, time.UTC)
	hoje := time.Now()
	dataInclusao := time.Date(hoje.Year(), hoje.Month(), hoje.Day(), 0, 0, 0, 0, time.UTC)

	_, err := w.db.ExecContext(ctx, `
		INSERT INTO TABLISTACHAVEACESSO
		  (CODIGO_CHAVEACESSO, CHAVEACESSO, CODIGOEMPRESA, CODIGOFILIAL,
		   CNPJEMITENTE, CNPJDESTINATARIO, EMITENTE, DESTINATARIO,
		   "DATA", DATAEMISSAO, DATAINCLUSAO, SERIE, NUMERODOCUMENTO, VALORTOTAL,
		   URL, CAMINHOORIGINAL, MENSAGEM, OBSERVACOES,
		   ORIGEM, DOWNLOAD, UPLOAD, IMPORTADO, IMPORTACAOIGNORADA,
		   CCEPOSSUI, NOTATRANSP, CODIGOROTINA_AGEN, CODIGOSITUACAOSAIDA,
		   CODIGOTIPOMOVIMENTO, SITUACAOMANIFESTO)
		VALUES (?,?,?,?, ?,?,?,?, ?,?,?,?,?,?, ?,?,?,?, 1,1,0,0,0, 0,0,0,0, 0,0)`,
		id, r.Chave, r.CodigoEmpresa, r.CodigoFilial,
		nullIfEmpty(toLatin1(r.CnpjEmitente)), nullIfEmpty(toLatin1(r.CnpjDestinatario)),
		nullIfEmpty(toLatin1(r.Emitente)), nullIfEmpty(toLatin1(r.Destinatario)),
		dataCompetencia, dateOnlyUTC(r.DataEmissao), dataInclusao, serie, atoiOr0(numero), r.ValorTotal,
		toLatin1(r.URL), toLatin1(r.CaminhoOriginal), toLatin1(r.CaminhoOriginal), toLatin1(r.Observacoes))
	return err
}

// DeleteOurRows apaga as linhas que ESTE tracker inseriu para uma chave e que o
// AthenasHorse ainda NÃO importou. Os dois filtros são a rede de segurança do
// rollback (§10): IMPORTADO=0 nunca toca numa nota já importada (aí é estorno
// fiscal, fora do escopo) e OBSERVACOES STARTING WITH garante que só as NOSSAS
// linhas somem — jamais uma linha do DownloadXML. Retorna quantas apagou.
func (w *Writer) DeleteOurRows(ctx context.Context, chave, markerPrefix string) (int64, error) {
	if len(chave) != 44 {
		return 0, fmt.Errorf("rollback: chave precisa ter 44 dígitos: %q", chave)
	}
	if markerPrefix == "" {
		return 0, fmt.Errorf("rollback: markerPrefix vazio apagaria linhas de terceiros")
	}
	res, err := w.db.ExecContext(ctx, `
		DELETE FROM TABLISTACHAVEACESSO
		WHERE CHAVEACESSO = ? AND IMPORTADO = 0 AND OBSERVACOES STARTING WITH ?`,
		chave, toLatin1(markerPrefix))
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// HasRow reporta se já existe linha para (chave, empresa, filial) — o pre-check
// de idempotência do syncer (retomada de crash / corrida com o DownloadXML).
// READ-ONLY (com retry de conexão); fica no Reader para o syncer poder checar
// pela credencial RO.
func (r *Reader) HasRow(ctx context.Context, chave string, codigoEmpresa, codigoFilial int) (bool, error) {
	var has bool
	err := r.retry(ctx, "pre-check da linha", func(ctx context.Context) error {
		var one int
		err := r.db.QueryRowContext(ctx, `
			SELECT FIRST 1 1 FROM TABLISTACHAVEACESSO
			WHERE CHAVEACESSO = ? AND CODIGOEMPRESA = ? AND CODIGOFILIAL = ?`,
			chave, codigoEmpresa, codigoFilial).Scan(&one)
		if err == sql.ErrNoRows {
			has = false
			return nil
		}
		has = err == nil
		return err
	})
	return has, err
}

// OurRowsCount é a contagem das linhas que ESTE tracker inseriu (marcador em
// OBSERVACOES), separada pelo estado de importação — a auditoria em massa (§14).
type OurRowsCount struct {
	Total    int64 // todas as nossas linhas
	Pending  int64 // IMPORTADO=0 (desfazíveis pelo rollback)
	Imported int64 // IMPORTADO=1 (já no livro — estorno fiscal, fora do escopo)
}

// OurRow é uma linha nossa no manifesto de auditoria (--audit --dump).
type OurRow struct {
	Chave         string    `json:"chave"`
	CodigoEmpresa int       `json:"codigo_empresa"`
	CodigoFilial  int       `json:"codigo_filial"`
	Importado     int       `json:"importado"`
	DataInclusao  time.Time `json:"data_inclusao"`
	URL           string    `json:"url"`
}

// CountOurRows conta TODAS as linhas com o nosso marcador, split por IMPORTADO.
// READ-ONLY (credencial de leitura basta): é a foto de "quanto o tracker já
// inseriu e quanto ainda dá para desfazer". markerPrefix vazio é recusado — sem
// ele a contagem varreria a tabela inteira (linhas de terceiros).
//
// since != zero restringe a DATAINCLUSAO >= since. IMPORTANTE: `OBSERVACOES
// STARTING WITH` não é indexado; sem `since` a query VARRE a TABLISTACHAVEACESSO
// inteira (milhões de linhas) — rode só em horário calmo, ou passe --since para
// aproveitar o índice de DATAINCLUSAO (nossas linhas só existem do F2 em diante).
func (r *Reader) CountOurRows(ctx context.Context, markerPrefix string, since time.Time) (OurRowsCount, error) {
	if markerPrefix == "" {
		return OurRowsCount{}, fmt.Errorf("CountOurRows: markerPrefix vazio contaria linhas de terceiros")
	}
	q := `SELECT COUNT(*),
	             SUM(CASE WHEN IMPORTADO = 0 THEN 1 ELSE 0 END),
	             SUM(CASE WHEN IMPORTADO = 1 THEN 1 ELSE 0 END)
	      FROM TABLISTACHAVEACESSO
	      WHERE OBSERVACOES STARTING WITH ?`
	args := []any{toLatin1(markerPrefix)}
	if !since.IsZero() {
		q += ` AND DATAINCLUSAO >= ?`
		args = append(args, dateOnlyUTC(since))
	}
	var c OurRowsCount
	err := r.retry(ctx, "contagem das nossas linhas", func(ctx context.Context) error {
		var total int64
		var pending, imported sql.NullInt64 // SUM de conjunto vazio é NULL
		e := r.db.QueryRowContext(ctx, q, args...).Scan(&total, &pending, &imported)
		if e != nil {
			return e
		}
		c = OurRowsCount{Total: total, Pending: pending.Int64, Imported: imported.Int64}
		return nil
	})
	return c, err
}

// HasImportedRow reporta se existe alguma linha NOSSA (marcador) já IMPORTADA
// (IMPORTADO=1) para a chave. É a guarda do rollback: se qualquer participação
// entrou no livro, os ARQUIVOS não podem ser mexidos (a linha importada referencia
// o XML no SINCRONIZADO, e reinjetar a origem arriscaria duplicata). READ-ONLY.
func (r *Reader) HasImportedRow(ctx context.Context, chave, markerPrefix string) (bool, error) {
	if len(chave) != 44 || markerPrefix == "" {
		return false, fmt.Errorf("HasImportedRow: chave/markerPrefix inválidos")
	}
	var has bool
	err := r.retry(ctx, "pre-check de importada", func(ctx context.Context) error {
		var one int
		e := r.db.QueryRowContext(ctx, `
			SELECT FIRST 1 1 FROM TABLISTACHAVEACESSO
			WHERE CHAVEACESSO = ? AND IMPORTADO = 1 AND OBSERVACOES STARTING WITH ?`,
			chave, toLatin1(markerPrefix)).Scan(&one)
		if e == sql.ErrNoRows {
			has = false
			return nil
		}
		has = e == nil
		return e
	})
	return has, err
}

// ListOurRows devolve as nossas linhas (para o manifesto e para enumerar chaves
// do rollback em massa). onlyPending=true restringe a IMPORTADO=0; since != zero
// restringe a DATAINCLUSAO >= since (índice — ver CountOurRows). READ-ONLY.
func (r *Reader) ListOurRows(ctx context.Context, markerPrefix string, onlyPending bool, since time.Time) ([]OurRow, error) {
	if markerPrefix == "" {
		return nil, fmt.Errorf("ListOurRows: markerPrefix vazio listaria linhas de terceiros")
	}
	q := `SELECT CHAVEACESSO, CODIGOEMPRESA, CODIGOFILIAL, IMPORTADO, DATAINCLUSAO, URL
	      FROM TABLISTACHAVEACESSO
	      WHERE OBSERVACOES STARTING WITH ?`
	args := []any{toLatin1(markerPrefix)}
	if onlyPending {
		q += ` AND IMPORTADO = 0`
	}
	if !since.IsZero() {
		q += ` AND DATAINCLUSAO >= ?`
		args = append(args, dateOnlyUTC(since))
	}
	q += ` ORDER BY DATAINCLUSAO, CHAVEACESSO`
	var out []OurRow
	err := r.retry(ctx, "lista das nossas linhas", func(ctx context.Context) error {
		out = nil // idempotente sob retry: nunca acumula de uma tentativa anterior
		rows, e := r.db.QueryContext(ctx, q, args...)
		if e != nil {
			return e
		}
		defer rows.Close()
		for rows.Next() {
			var chave, url sql.NullString
			var emp, fil, imp sql.NullInt64
			var di sql.NullTime
			if e := rows.Scan(&chave, &emp, &fil, &imp, &di, &url); e != nil {
				return e
			}
			out = append(out, OurRow{
				Chave: trimNull(chave), CodigoEmpresa: int(emp.Int64), CodigoFilial: int(fil.Int64),
				Importado: int(imp.Int64), DataInclusao: di.Time, URL: toUTF8(trimNull(url)),
			})
		}
		return rows.Err()
	})
	return out, err
}

// EmpresaNomes carrega o nome de cada empresa (TABEMPRESAS) — o 1º segmento do
// caminho derivado. Poucos milhares de linhas; carregado uma vez por ciclo.
// READ-ONLY (com retry de conexão). Nomes transcodificados para UTF-8 (o
// syncpath sanitiza depois).
func (r *Reader) EmpresaNomes(ctx context.Context) (map[int]string, error) {
	var out map[int]string
	err := r.retry(ctx, "nomes de empresa", func(ctx context.Context) error {
		var err error
		out, err = r.empresaNomes(ctx)
		return err
	})
	return out, err
}

func (r *Reader) empresaNomes(ctx context.Context) (map[int]string, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT CODIGO, NOME FROM TABEMPRESAS`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[int]string{}
	for rows.Next() {
		var cod sql.NullInt64
		var nome sql.NullString
		if err := rows.Scan(&cod, &nome); err != nil {
			return nil, err
		}
		if cod.Valid {
			out[int(cod.Int64)] = toUTF8(trimNull(nome))
		}
	}
	return out, rows.Err()
}

// toLatin1 transcodifica UTF-8 → Latin-1 (o inverso do toUTF8 da leitura): a
// conexão é charset=NONE e o banco fala Latin-1, então bytes crus > 0x7F devem
// ser o code point Latin-1. Runas fora do Latin-1 (ex.: emoji, aspas tipográficas)
// viram '?' — melhor um '?' visível que mojibake silencioso no Athenas.
func toLatin1(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		if r <= 0xFF {
			b.WriteByte(byte(r))
		} else {
			b.WriteByte('?')
		}
	}
	return b.String()
}

// dateOnlyUTC derruba um instante para a meia-noite UTC do mesmo dia-calendário
// (colunas DATE do Firebird; o driver manda o wall-clock).
func dateOnlyUTC(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}

func nullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func atoiOr0(s string) int {
	n := 0
	for _, r := range s {
		if r < '0' || r > '9' {
			return 0
		}
		n = n*10 + int(r-'0')
	}
	return n
}
