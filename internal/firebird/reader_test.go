package firebird

import (
	"context"
	"os"
	"strconv"
	"testing"
)

// TestReaderLookup runs only when TRACKER_TEST_FB_DSN is set. It exercises the
// READ-ONLY lookup against a real Athenas Firebird. Pass one or more known
// chaves via TRACKER_TEST_FB_CHAVES (comma-separated). Skipped otherwise.
func TestReaderLookup(t *testing.T) {
	dsn := os.Getenv("TRACKER_TEST_FB_DSN")
	if dsn == "" {
		t.Skip("set TRACKER_TEST_FB_DSN to run the Firebird reader test")
	}
	ctx := context.Background()
	r, err := NewReader(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer r.Close()

	chaves := splitEnv(os.Getenv("TRACKER_TEST_FB_CHAVES"))
	if len(chaves) == 0 {
		t.Skip("set TRACKER_TEST_FB_CHAVES (comma-separated) to assert on real keys")
	}

	got, err := r.Lookup(ctx, chaves)
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	for _, c := range chaves {
		st, ok := got[c]
		t.Logf("chave=%s found=%v rows=%d -> importado=%v ignorada=%v empresa=%s/%q motivo=%q emit=%q data=%q",
			c, ok, len(st.Rows), st.Importado, st.ImportIgnorada, codigoStr(st.CodigoEmpresa),
			st.NomeEmpresa, st.Motivo, st.CnpjEmitente, st.DataEmissao)
		for _, e := range st.Rows {
			t.Logf("    linha empresa=%s/%q importado=%v ignorada=%v motivo=%q",
				codigoStr(e.CodigoEmpresa), e.NomeEmpresa, e.Importado, e.ImportIgnorada, e.Motivo)
		}
	}
}

func codigoStr(p *int) string {
	if p == nil {
		return "<nil>"
	}
	return strconv.Itoa(*p)
}

func splitEnv(s string) []string {
	var out []string
	for _, p := range split(s, ',') {
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func split(s string, sep rune) []string {
	var out []string
	cur := ""
	for _, r := range s {
		if r == sep {
			out = append(out, cur)
			cur = ""
			continue
		}
		cur += string(r)
	}
	out = append(out, cur)
	return out
}

func TestValidChave(t *testing.T) {
	ok44 := "44250610567560000110550010001234561000123456"
	cases := []struct {
		in   string
		want bool
	}{
		{ok44, true},
		{ok44 + "7", false},                  // 45 chars (o lixo real que estourou o varchar(44))
		{ok44[:43], false},                   // curta
		{"", false},                          // vazia
		{ok44[:43] + "X", false},             // não-dígito
		{ok44[:20] + " " + ok44[21:], false}, // espaço no meio
	}
	for _, c := range cases {
		if got := validChave(c.in); got != c.want {
			t.Errorf("validChave(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}
