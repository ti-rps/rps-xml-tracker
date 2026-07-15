package xmlparse

import (
	"strings"
	"testing"

	"github.com/EnzzoHosaki/rps-xml-tracker/internal/model"
)

func TestParse_ExtractsParties(t *testing.T) {
	xml := `<nfeProc><NFe><infNFe Id="NFe35250712345678000190550010000001231000001234">
	  <ide><mod>55</mod><dhEmi>2026-06-08T10:30:00-03:00</dhEmi></ide>
	  <emit><CNPJ>12345678000190</CNPJ><xNome>FORNECEDOR EXEMPLO LTDA</xNome></emit>
	  <dest><CNPJ>99999999000191</CNPJ><xNome>CLIENTE RPS LTDA</xNome></dest>
	  <total><ICMSTot><vNF>1234.56</vNF></ICMSTot></total>
	</infNFe></NFe></nfeProc>`
	r, err := Parse(strings.NewReader(xml))
	if err != nil {
		t.Fatal(err)
	}
	if r.CnpjEmitente != "12345678000190" || r.NomeEmitente != "FORNECEDOR EXEMPLO LTDA" {
		t.Errorf("emitente: %q / %q", r.CnpjEmitente, r.NomeEmitente)
	}
	if r.CnpjDestinatario != "99999999000191" || r.NomeDestinatario != "CLIENTE RPS LTDA" {
		t.Errorf("destinatário: %q / %q", r.CnpjDestinatario, r.NomeDestinatario)
	}
	if r.DataEmissao != "2026-06-08" {
		t.Errorf("data_emissao = %q want 2026-06-08", r.DataEmissao)
	}
	if r.ValorTotal != "1234.56" {
		t.Errorf("valor = %q want 1234.56", r.ValorTotal)
	}
	if r.Chave != "35250712345678000190550010000001231000001234" {
		t.Errorf("chave = %q", r.Chave)
	}
}

func TestParse_TipoNF(t *testing.T) {
	cases := []struct {
		name string
		ide  string
		want string
	}{
		{"saída (tpNF=1)", `<mod>55</mod><tpNF>1</tpNF>`, "1"},
		{"entrada/devolução (tpNF=0)", `<mod>55</mod><tpNF>0</tpNF>`, "0"},
		{"ausente (CTe/NFSe)", `<mod>55</mod>`, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			xml := `<NFe><infNFe Id="NFe35250712345678000190550010000001231000001234"><ide>` + c.ide + `</ide></infNFe></NFe>`
			r, err := Parse(strings.NewReader(xml))
			if err != nil {
				t.Fatal(err)
			}
			if r.TipoNF != c.want {
				t.Errorf("TipoNF = %q, want %q", r.TipoNF, c.want)
			}
		})
	}
}

func TestParse(t *testing.T) {
	cases := []struct {
		name    string
		xml     string
		chave   string
		via     string
		docType model.DocType
	}{
		{
			name:    "nfeProc NFe mod 55 via infNFe@Id",
			xml:     `<nfeProc><NFe><infNFe Id="NFe35250712345678000190550010000001231000001234"><ide><mod>55</mod></ide></infNFe></NFe></nfeProc>`,
			chave:   "35250712345678000190550010000001231000001234",
			via:     "infNFe@Id",
			docType: model.DocNFe,
		},
		{
			name:    "standalone NFCe mod 65",
			xml:     `<NFe><infNFe Id="NFe35250799999999000191650010000005551000005550"><ide><mod>65</mod></ide></infNFe></NFe>`,
			chave:   "35250799999999000191650010000005551000005550",
			via:     "infNFe@Id",
			docType: model.DocNFCe,
		},
		{
			name:    "evento via chNFe",
			xml:     `<procEventoNFe><evento><infEvento><chNFe>35250722222222000191550010000009991000009990</chNFe></infEvento></evento></procEventoNFe>`,
			chave:   "35250722222222000191550010000009991000009990",
			via:     "chNFe",
			docType: model.DocEvento,
		},
		{
			name:    "CTe via chCTe",
			xml:     `<cteProc><CTe><infCte></infCte></CTe><protCTe><infProt><chCTe>35250733333333000191570010000001111000001110</chCTe></infProt></protCTe></cteProc>`,
			chave:   "35250733333333000191570010000001111000001110",
			via:     "chCTe",
			docType: model.DocCTe,
		},
		{
			name:    "NFSe CompNfse has no 44-digit chave",
			xml:     `<CompNfse><Nfse><InfNfse><Numero>123</Numero></InfNfse></Nfse></CompNfse>`,
			chave:   "",
			docType: model.DocUnknown,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r, err := Parse(strings.NewReader(c.xml))
			if err != nil {
				t.Fatal(err)
			}
			if r.Chave != c.chave {
				t.Errorf("chave = %q, want %q", r.Chave, c.chave)
			}
			if c.chave != "" && r.ChaveVia != c.via {
				t.Errorf("via = %q, want %q", r.ChaveVia, c.via)
			}
			if r.DocType != c.docType {
				t.Errorf("docType = %s, want %s", r.DocType, c.docType)
			}
		})
	}
}
