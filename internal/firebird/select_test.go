package firebird

import "testing"

func codigo(n int) *int { return &n }

// the real case that triggered this: chave 29260510406651000130650000000809281001606071,
// a NFCe emitida pela CLW. The same chave has 4 rows in TABLISTACHAVEACESSO. The
// old "first non-empty wins" merge attributed it to ROSEMBERG (the row Firebird
// happened to return first), which is merely a third party that listed the nota
// "de terceiros" and ignores it via the Pré-Importação screen. The owner — the
// empresa that ACTUALLY imported it — is CLW, the one with IMPORTADO=1.
func TestSelectState_PicksImportadoOwnerNotArbitraryTerceiros(t *testing.T) {
	rows := []EmpresaImport{
		{CodigoEmpresa: codigo(120), NomeEmpresa: "ROSEMBERG PEREIRA DE SOUZA", ImportIgnorada: true,
			Motivo: "Empresa usa tela de Pre-Importacao de Entradas - nota e de terceiros", CnpjEmitente: "10406651000130"},
		{CodigoEmpresa: codigo(165), NomeEmpresa: "CLW CHURRASCARIA LTDA", Importado: true, CnpjEmitente: "10406651000130"},
		{CodigoEmpresa: codigo(996), NomeEmpresa: "EMPRESA TESTE", CnpjEmitente: "10406651000130"},
		{CodigoEmpresa: codigo(2165), NomeEmpresa: "CLW CHURRASCARIA LTDA.", CnpjEmitente: "10406651000130"},
	}
	st := selectState("CHAVE", rows)

	if !st.Importado || st.ImportIgnorada {
		t.Fatalf("want importado (não ignorada); got importado=%v ignorada=%v", st.Importado, st.ImportIgnorada)
	}
	if st.NomeEmpresa != "CLW CHURRASCARIA LTDA" {
		t.Errorf("empresa = %q, want CLW (a que importou), não a terceira que aparece primeiro", st.NomeEmpresa)
	}
	if st.CodigoEmpresa == nil || *st.CodigoEmpresa != 165 {
		t.Errorf("codigoEmpresa = %v, want 165", st.CodigoEmpresa)
	}
	if st.Motivo != "" {
		t.Errorf("motivo = %q, want vazio (a nota foi importada, não ignorada)", st.Motivo)
	}
	if len(st.Rows) != 4 {
		t.Errorf("Rows = %d, want 4 (todas as linhas preservadas)", len(st.Rows))
	}
}

// A terceiros' "importação ignorada" must NOT end a nota the owner still has
// pending: while any row is plain-pending, the nota stays em trânsito.
func TestSelectState_PendingBeatsTerceirosIgnored(t *testing.T) {
	rows := []EmpresaImport{
		{CodigoEmpresa: codigo(120), NomeEmpresa: "ROSEMBERG", ImportIgnorada: true, Motivo: "de terceiros"},
		{CodigoEmpresa: codigo(165), NomeEmpresa: "CLW"}, // 0/0 — dona ainda vai importar
	}
	st := selectState("CHAVE", rows)
	if st.Importado || st.ImportIgnorada {
		t.Fatalf("want em trânsito (sem flags terminais); got importado=%v ignorada=%v", st.Importado, st.ImportIgnorada)
	}
	if st.CodigoEmpresa == nil || *st.CodigoEmpresa != 165 {
		t.Errorf("codigoEmpresa = %v, want 165 (a pendente, não a terceira ignorada)", st.CodigoEmpresa)
	}
}

// When EVERY row is ignored, the nota is genuinely ignored; report the motivo of
// the deterministic (lowest-codigo) row.
func TestSelectState_AllIgnored(t *testing.T) {
	rows := []EmpresaImport{
		{CodigoEmpresa: codigo(300), NomeEmpresa: "B", ImportIgnorada: true, Motivo: "config B"},
		{CodigoEmpresa: codigo(200), NomeEmpresa: "A", ImportIgnorada: true, Motivo: "config A"},
	}
	st := selectState("CHAVE", rows)
	if !st.ImportIgnorada || st.Importado {
		t.Fatalf("want ignorada; got importado=%v ignorada=%v", st.Importado, st.ImportIgnorada)
	}
	if st.Motivo != "config A" || st.CodigoEmpresa == nil || *st.CodigoEmpresa != 200 {
		t.Errorf("got empresa=%v motivo=%q, want 200/config A (menor codigo)", st.CodigoEmpresa, st.Motivo)
	}
}

// Single row (the common case) passes straight through.
func TestSelectState_SingleRow(t *testing.T) {
	st := selectState("CHAVE", []EmpresaImport{
		{CodigoEmpresa: codigo(42), NomeEmpresa: "ACME", Importado: true, CnpjEmitente: "99"},
	})
	if !st.Importado || st.NomeEmpresa != "ACME" || st.CnpjEmitente != "99" {
		t.Fatalf("passthrough falhou: %+v", st)
	}
}

// Two imported rows: deterministic lowest-codigo wins (no flapping).
func TestSelectState_ImportedTieLowestCodigo(t *testing.T) {
	rows := []EmpresaImport{
		{CodigoEmpresa: codigo(900), NomeEmpresa: "HIGH", Importado: true},
		{CodigoEmpresa: codigo(100), NomeEmpresa: "LOW", Importado: true},
	}
	st := selectState("CHAVE", rows)
	if st.CodigoEmpresa == nil || *st.CodigoEmpresa != 100 {
		t.Errorf("codigoEmpresa = %v, want 100 (menor)", st.CodigoEmpresa)
	}
}
