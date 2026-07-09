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

// HasRow reporta se já existe linha para (chave, empresa, filial) — o pre-check
// de idempotência do syncer (retomada de crash / corrida com o DownloadXML).
// READ-ONLY; fica no Reader para o syncer poder checar pela credencial RO.
func (r *Reader) HasRow(ctx context.Context, chave string, codigoEmpresa, codigoFilial int) (bool, error) {
	var one int
	err := r.db.QueryRowContext(ctx, `
		SELECT FIRST 1 1 FROM TABLISTACHAVEACESSO
		WHERE CHAVEACESSO = ? AND CODIGOEMPRESA = ? AND CODIGOFILIAL = ?`,
		chave, codigoEmpresa, codigoFilial).Scan(&one)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return err == nil, err
}

// EmpresaNomes carrega o nome de cada empresa (TABEMPRESAS) — o 1º segmento do
// caminho derivado. Poucos milhares de linhas; carregado uma vez por ciclo.
// READ-ONLY. Nomes transcodificados para UTF-8 (o syncpath sanitiza depois).
func (r *Reader) EmpresaNomes(ctx context.Context) (map[int]string, error) {
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
