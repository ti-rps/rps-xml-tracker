package xmlparse

import (
	"strings"
	"testing"

	"github.com/EnzzoHosaki/rps-xml-tracker/internal/model"
)

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
