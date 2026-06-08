// fbinspect — Phase-0 (F0.1) READ-ONLY Firebird investigation tool.
//
// Answers the critical open question: where does `imported_at` come from?
//   - Is DATADEENTRADA always populated when IMPORTADO = 1?
//   - Does its range/recency make it a trustworthy import timestamp?
//   - Or must we detect the IMPORTADO 0->1 transition by polling?
//
// It issues ONLY read-only SELECT statements against the Athenas Firebird DB
// (TABLISTACHAVEACESSO). It NEVER writes, updates, or alters anything.
//
// Connection (nakagami/firebirdsql DSN), via env or flag:
//   FIREBIRD_DSN="SYSDBA:masterkey@host:3050//var/lib/firebird/athenas.fdb"
// or build it from parts:
//   -host -port -user -pass -db  (and optional -charset, default NONE)
//
// Usage:
//   FIREBIRD_DSN=... go run ./fbinspect
//   go run ./fbinspect -host 10.0.0.5 -user SYSDBA -pass secret -db "C:\\Athenas\\BASE.FDB"
package main

import (
	"database/sql"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	_ "github.com/nakagami/firebirdsql"
)

const table = "TABLISTACHAVEACESSO"

func main() {
	dsn := flag.String("dsn", os.Getenv("FIREBIRD_DSN"), "full firebirdsql DSN (overrides parts); or set FIREBIRD_DSN")
	host := flag.String("host", "", "firebird host")
	port := flag.Int("port", 3050, "firebird port")
	user := flag.String("user", "SYSDBA", "firebird user (read-only recommended)")
	pass := flag.String("pass", "", "firebird password")
	db := flag.String("db", "", "firebird database path/alias")
	charset := flag.String("charset", "NONE", "connection charset")
	sampleN := flag.Int("sample", 15, "rows in the recent-imported sample")
	q := flag.String("q", "", "run a single read-only SELECT and exit (exploration mode)")
	flag.Parse()

	connStr := *dsn
	if connStr == "" {
		if *host == "" || *pass == "" || *db == "" {
			fmt.Fprintln(os.Stderr, "error: provide -dsn (or FIREBIRD_DSN), or -host -pass -db")
			flag.Usage()
			os.Exit(2)
		}
		connStr = fmt.Sprintf("%s:%s@%s:%d/%s?charset=%s", *user, *pass, *host, *port, *db, *charset)
	}

	conn, err := sql.Open("firebirdsql", connStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()
	conn.SetMaxOpenConns(1)
	if err := conn.Ping(); err != nil {
		fmt.Fprintf(os.Stderr, "ping (connectivity/credentials): %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("== fbinspect (READ-ONLY) — %s ==\n", table)
	fmt.Printf("Connected OK at %s\n", time.Now().Format(time.RFC3339))

	if *q != "" {
		runQuery(conn, *q)
		return
	}

	section("1. Column layout (RDB$RELATION_FIELDS)")
	dumpColumns(conn)

	section("2. Row volume & IMPORTADO distribution")
	scalar(conn, "Total rows", "SELECT COUNT(*) FROM "+table)
	groupBy(conn, "By IMPORTADO", "IMPORTADO", "SELECT IMPORTADO, COUNT(*) FROM "+table+" GROUP BY IMPORTADO")

	section("3. imported_at viability — DATADEENTRADA vs IMPORTADO")
	scalar(conn, "IMPORTADO=1 total",
		"SELECT COUNT(*) FROM "+table+" WHERE IMPORTADO = 1")
	scalar(conn, "IMPORTADO=1 AND DATADEENTRADA IS NULL  (<-- must be ~0 for DATADEENTRADA to work)",
		"SELECT COUNT(*) FROM "+table+" WHERE IMPORTADO = 1 AND DATADEENTRADA IS NULL")
	scalar(conn, "IMPORTADO=1 AND DATADEENTRADA IS NOT NULL",
		"SELECT COUNT(*) FROM "+table+" WHERE IMPORTADO = 1 AND DATADEENTRADA IS NOT NULL")
	scalar(conn, "DATADEENTRADA populated but IMPORTADO<>1 (entrada != importacao?)",
		"SELECT COUNT(*) FROM "+table+" WHERE DATADEENTRADA IS NOT NULL AND (IMPORTADO IS NULL OR IMPORTADO <> 1)")
	twoCol(conn, "DATADEENTRADA range (min / max)",
		"SELECT MIN(DATADEENTRADA), MAX(DATADEENTRADA) FROM "+table)

	section("4. Recency — can we poll incrementally by DATADEENTRADA?")
	scalar(conn, "Rows with DATADEENTRADA = today",
		"SELECT COUNT(*) FROM "+table+" WHERE CAST(DATADEENTRADA AS DATE) = CURRENT_DATE")
	scalar(conn, "Rows with DATADEENTRADA in last 7 days",
		"SELECT COUNT(*) FROM "+table+" WHERE DATADEENTRADA >= DATEADD(-7 DAY TO CURRENT_TIMESTAMP)")

	section("5. Terminal/ignored states")
	groupBy(conn, "By SITUACAO", "SITUACAO", "SELECT SITUACAO, COUNT(*) FROM "+table+" GROUP BY SITUACAO")
	groupBy(conn, "By IMPORTACAOIGNORADA", "IMPORTACAOIGNORADA",
		"SELECT IMPORTACAOIGNORADA, COUNT(*) FROM "+table+" GROUP BY IMPORTACAOIGNORADA")

	section("6. Recent imported sample (eyeball DATADEENTRADA vs the key)")
	recentSample(conn, *sampleN)

	fmt.Println("\n== done. All statements were read-only SELECTs. ==")
	fmt.Println("Interpretation hint:")
	fmt.Println("  - If '#3 IMPORTADO=1 AND DATADEENTRADA IS NULL' is ~0 and the range/recency look")
	fmt.Println("    sane, use DATADEENTRADA as imported_at.")
	fmt.Println("  - If many imported rows have NULL DATADEENTRADA, fall back to polling the")
	fmt.Println("    IMPORTADO 0->1 transition (imported_at ~= poll time, granularity = poll interval).")
}

// ---- query helpers: each is defensive so one failure doesn't abort the run ----

func section(t string) { fmt.Printf("\n--- %s ---\n", t) }

// runQuery executes one arbitrary read-only SELECT and prints a generic grid.
func runQuery(conn *sql.DB, query string) {
	fmt.Printf("\nQUERY: %s\n\n", query)
	rows, err := conn.Query(query)
	if err != nil {
		fmt.Printf("ERROR: %v\n", err)
		return
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	fmt.Println(strings.Join(cols, " | "))
	n := 0
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			fmt.Printf("scan error: %v\n", trimErr(err))
			continue
		}
		parts := make([]string, len(cols))
		for i, v := range vals {
			switch t := v.(type) {
			case nil:
				parts[i] = "<null>"
			case []byte:
				parts[i] = strings.TrimSpace(string(t))
			case time.Time:
				parts[i] = t.Format("2006-01-02 15:04:05")
			default:
				parts[i] = fmt.Sprintf("%v", t)
			}
		}
		fmt.Println(strings.Join(parts, " | "))
		n++
	}
	fmt.Printf("\n(%d rows)\n", n)
}

func scalar(conn *sql.DB, label, query string) {
	var v sql.NullString
	if err := conn.QueryRow(query).Scan(&v); err != nil {
		fmt.Printf("  %-60s ERROR: %v\n", label, trimErr(err))
		return
	}
	fmt.Printf("  %-60s %s\n", label, ns(v))
}

func twoCol(conn *sql.DB, label, query string) {
	var a, b sql.NullString
	if err := conn.QueryRow(query).Scan(&a, &b); err != nil {
		fmt.Printf("  %-40s ERROR: %v\n", label, trimErr(err))
		return
	}
	fmt.Printf("  %-40s %s  ..  %s\n", label, ns(a), ns(b))
}

func groupBy(conn *sql.DB, label, _ string, query string) {
	rows, err := conn.Query(query)
	if err != nil {
		fmt.Printf("  %s: ERROR: %v\n", label, trimErr(err))
		return
	}
	defer rows.Close()
	fmt.Printf("  %s:\n", label)
	for rows.Next() {
		var k, c sql.NullString
		if err := rows.Scan(&k, &c); err != nil {
			fmt.Printf("    scan error: %v\n", trimErr(err))
			continue
		}
		fmt.Printf("    %-12s %s\n", ns(k), ns(c))
	}
}

func dumpColumns(conn *sql.DB) {
	q := `SELECT TRIM(RF.RDB$FIELD_NAME), F.RDB$FIELD_TYPE, F.RDB$FIELD_LENGTH, RF.RDB$NULL_FLAG
	      FROM RDB$RELATION_FIELDS RF
	      JOIN RDB$FIELDS F ON F.RDB$FIELD_NAME = RF.RDB$FIELD_SOURCE
	      WHERE RF.RDB$RELATION_NAME = '` + table + `'
	      ORDER BY RF.RDB$FIELD_POSITION`
	rows, err := conn.Query(q)
	if err != nil {
		fmt.Printf("  ERROR: %v\n", trimErr(err))
		return
	}
	defer rows.Close()
	for rows.Next() {
		var name sql.NullString
		var ftype, flen, nflag sql.NullInt64
		if err := rows.Scan(&name, &ftype, &flen, &nflag); err != nil {
			continue
		}
		nullable := "NULL"
		if nflag.Int64 == 1 {
			nullable = "NOT NULL"
		}
		fmt.Printf("  %-26s type=%-3d len=%-4d %s\n", name.String, ftype.Int64, flen.Int64, nullable)
	}
}

func recentSample(conn *sql.DB, n int) {
	q := fmt.Sprintf(`SELECT FIRST %d CHAVEACESSO, IMPORTADO, DATADEENTRADA, SITUACAO, IMPORTACAOIGNORADA
	      FROM %s WHERE DATADEENTRADA IS NOT NULL ORDER BY DATADEENTRADA DESC`, n, table)
	rows, err := conn.Query(q)
	if err != nil {
		fmt.Printf("  ERROR: %v\n", trimErr(err))
		return
	}
	defer rows.Close()
	fmt.Printf("  %-46s %-4s %-22s %-4s %-4s\n", "CHAVEACESSO", "IMP", "DATADEENTRADA", "SIT", "IGN")
	for rows.Next() {
		var chave, dt sql.NullString
		var imp, sit, ign sql.NullInt64
		if err := rows.Scan(&chave, &imp, &dt, &sit, &ign); err != nil {
			fmt.Printf("  scan error: %v\n", trimErr(err))
			continue
		}
		fmt.Printf("  %-46s %-4d %-22s %-4d %-4d\n",
			chave.String, imp.Int64, dt.String, sit.Int64, ign.Int64)
	}
}

func ns(v sql.NullString) string {
	if !v.Valid {
		return "<null>"
	}
	return v.String
}

func trimErr(err error) string {
	s := err.Error()
	if i := strings.IndexByte(s, '\n'); i > 0 {
		s = s[:i]
	}
	return s
}
