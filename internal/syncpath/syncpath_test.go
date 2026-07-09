package syncpath

import (
	"strings"
	"testing"

	"github.com/EnzzoHosaki/rps-xml-tracker/internal/model"
)

// A URL real observada na TABLISTACHAVEACESSO que originou a hipótese do padrão
// (NFCe de saída — empresa emitente é a própria dona da pasta).
func TestDeriveExemploReal(t *testing.T) {
	chave := "35250630853529000461650010000123451000123456"
	got, err := Derive(Input{
		NomeEmpresa: "GESTAO BEACH LTDA",
		CnpjFilial:  "30.853.529/0004-61",
		DocType:     model.DocNFCe,
		Direction:   model.DirSaida,
		DataEmissao: "2026-06-15",
		Chave:       chave,
	})
	if err != nil {
		t.Fatal(err)
	}
	want := `\GESTAO BEACH LTDA\30853529000461\NFCe\SAIDA\202606\` + chave + `.xml`
	if got != want {
		t.Errorf("Derive:\n got  %s\n want %s", got, want)
	}
}

func TestDeriveEntrada(t *testing.T) {
	chave := strings.Repeat("4", 44)
	got, err := Derive(Input{
		NomeEmpresa: "COMERCIO DE ALIMENTOS XYZ LTDA",
		CnpjFilial:  "11222333000181",
		DocType:     model.DocNFe,
		Direction:   model.DirEntrada,
		DataEmissao: "2026-01-02",
		Chave:       chave,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got, `\NFe\ENTRADA\202601\`) {
		t.Errorf("Derive = %s; esperado segmentos \\NFe\\ENTRADA\\202601\\", got)
	}
}

func TestDeriveErros(t *testing.T) {
	ok := Input{
		NomeEmpresa: "EMPRESA LTDA",
		CnpjFilial:  "11222333000181",
		DocType:     model.DocNFe,
		Direction:   model.DirSaida,
		DataEmissao: "2026-06-15",
		Chave:       strings.Repeat("1", 44),
	}
	cases := []struct {
		nome   string
		mutate func(*Input)
	}{
		{"nome vazio", func(in *Input) { in.NomeEmpresa = "" }},
		{"nome só inválidos", func(in *Input) { in.NomeEmpresa = `\\//::` }},
		{"cnpj curto", func(in *Input) { in.CnpjFilial = "1122233300018" }},
		{"evento fora do escopo", func(in *Input) { in.DocType = model.DocEvento }},
		{"doc desconhecido", func(in *Input) { in.DocType = model.DocUnknown }},
		{"direção indeterminada", func(in *Input) { in.Direction = "" }},
		{"data inválida", func(in *Input) { in.DataEmissao = "15/06/2026" }},
		{"chave curta", func(in *Input) { in.Chave = "123" }},
		{"chave com letra", func(in *Input) { in.Chave = strings.Repeat("1", 43) + "x" }},
	}
	for _, c := range cases {
		in := ok
		c.mutate(&in)
		if _, err := Derive(in); err == nil {
			t.Errorf("%s: esperado erro, veio nil", c.nome)
		}
	}
	if _, err := Derive(ok); err != nil {
		t.Errorf("input válido deu erro: %v", err)
	}
}

func TestSanitizeSegment(t *testing.T) {
	cases := []struct{ in, want string }{
		{"GESTAO BEACH LTDA", "GESTAO BEACH LTDA"},
		{"AUTO POSTO S/A", "AUTO POSTO SA"},
		{"EMPRESA LTDA.", "EMPRESA LTDA"},
		{"  EMPRESA  ", "EMPRESA"},
		{`A<B>C:D"E/F\G|H?I*J`, "ABCDEFGHIJ"},
		{"NOME\x00COM\x1fCONTROLE", "NOMECOMCONTROLE"},
		{"TRAILING. . ", "TRAILING"},
		{"ACENTUAÇÃO É PRESERVADA", "ACENTUAÇÃO É PRESERVADA"},
	}
	for _, c := range cases {
		if got := SanitizeSegment(c.in); got != c.want {
			t.Errorf("SanitizeSegment(%q) = %q; want %q", c.in, got, c.want)
		}
	}
}

func TestCompetencia(t *testing.T) {
	if got, err := Competencia("2026-06-15"); err != nil || got != "202606" {
		t.Errorf("Competencia = %q, %v; want 202606", got, err)
	}
	// date-only da linha do Firebird também serve
	if got, err := Competencia("2025-12-31"); err != nil || got != "202512" {
		t.Errorf("Competencia = %q, %v; want 202512", got, err)
	}
	for _, bad := range []string{"", "202606", "15/06/2026", "26-06-15"} {
		if _, err := Competencia(bad); err == nil {
			t.Errorf("Competencia(%q): esperado erro", bad)
		}
	}
}

func TestSegments(t *testing.T) {
	segs := Segments(`\GESTAO BEACH LTDA\30853529000461\NFCe\SAIDA\202606\chave.xml`)
	if len(segs) != len(SegmentNames) {
		t.Fatalf("Segments: %d segmentos, want %d (%v)", len(segs), len(SegmentNames), segs)
	}
	if segs[0] != "GESTAO BEACH LTDA" || segs[5] != "chave.xml" {
		t.Errorf("Segments errado: %v", segs)
	}
	if Segments("") != nil || Segments(`\`) != nil {
		t.Error("Segments de vazio deveria ser nil")
	}
}
