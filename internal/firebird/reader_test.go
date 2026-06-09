package firebird

import (
	"context"
	"os"
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
		t.Logf("chave=%s found=%v importado=%v tipo=%s empresa=%v/%q emit=%q/%q dest=%q data=%q",
			c, ok, st.Importado, st.TipoDocumento, st.CodigoEmpresa, st.NomeEmpresa,
			st.CnpjEmitente, st.NomeEmitente, st.CnpjDestinatario, st.DataEmissao)
	}
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
