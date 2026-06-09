// Package xmlparse extracts the chave de acesso and key fields from a fiscal
// XML file, READ-ONLY. The filename does NOT contain the chave (Fase 0), so we
// must read the content. It streams the XML with a small state machine and also
// captures emitente/destinatário (CNPJ + nome), emission date and total value —
// used to identify the empresa/parties and to power the dashboard filters.
package xmlparse

import (
	"encoding/xml"
	"io"
	"os"
	"strings"

	"github.com/EnzzoHosaki/rps-xml-tracker/internal/model"
)

// Result is what we learn from one XML file.
type Result struct {
	Chave    string        // 44-digit access key, "" if none (e.g. NFSe)
	ChaveVia string        // "infNFe@Id" | "chNFe" | "chCTe" | ""
	DocType  model.DocType // NFE/NFCE/CTE/EVENTO/UNKNOWN
	RootElem string

	CnpjEmitente     string
	NomeEmitente     string
	CnpjDestinatario string
	NomeDestinatario string
	DataEmissao      string // yyyy-mm-dd
	ValorTotal       string // raw, ex.: "1234.56"
}

// ParseFile opens path read-only and extracts the fields.
func ParseFile(path string) (Result, error) {
	f, err := os.Open(path) // read-only; never modifies the file
	if err != nil {
		return Result{}, err
	}
	defer f.Close()
	return Parse(f)
}

// Parse reads from r (used by ParseFile and by tests).
func Parse(r io.Reader) (Result, error) {
	var res Result
	dec := xml.NewDecoder(r)
	dec.Strict = false
	dec.CharsetReader = func(_ string, in io.Reader) (io.Reader, error) { return in, nil }

	var mod string
	var inEmit, inDest bool

	for {
		tok, err := dec.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			if res.RootElem == "" {
				return res, err
			}
			break
		}
		switch t := tok.(type) {
		case xml.StartElement:
			local := t.Name.Local
			if res.RootElem == "" {
				res.RootElem = local
			}
			switch local {
			case "infNFe", "infNFCe":
				for _, a := range t.Attr {
					if a.Name.Local == "Id" && res.Chave == "" {
						if k := normalizeKey(a.Value); k != "" {
							res.Chave, res.ChaveVia = k, "infNFe@Id"
						}
					}
				}
			case "mod":
				mod = strings.TrimSpace(text(dec))
			case "chNFe":
				if res.Chave == "" {
					if k := normalizeKey(text(dec)); k != "" {
						res.Chave, res.ChaveVia = k, "chNFe"
					}
				}
			case "chCTe":
				if res.Chave == "" {
					if k := normalizeKey(text(dec)); k != "" {
						res.Chave, res.ChaveVia = k, "chCTe"
					}
				}
			case "emit":
				inEmit = true
			case "dest":
				inDest = true
			case "CNPJ", "CPF":
				v := strings.TrimSpace(text(dec))
				if inEmit && res.CnpjEmitente == "" {
					res.CnpjEmitente = v
				} else if inDest && res.CnpjDestinatario == "" {
					res.CnpjDestinatario = v
				}
			case "xNome":
				v := strings.TrimSpace(text(dec))
				if inEmit && res.NomeEmitente == "" {
					res.NomeEmitente = v
				} else if inDest && res.NomeDestinatario == "" {
					res.NomeDestinatario = v
				}
			case "dhEmi", "dEmi":
				if res.DataEmissao == "" {
					if d := dateOnly(text(dec)); d != "" {
						res.DataEmissao = d
					}
				}
			case "vNF":
				if res.ValorTotal == "" {
					res.ValorTotal = strings.TrimSpace(text(dec))
				}
			}
		case xml.EndElement:
			switch t.Name.Local {
			case "emit":
				inEmit = false
			case "dest":
				inDest = false
			}
		}
	}
	res.DocType = classify(res.RootElem, res.ChaveVia, mod)
	return res, nil
}

// text reads the immediate char data of the element just started.
func text(dec *xml.Decoder) string {
	tok, err := dec.Token()
	if err != nil {
		return ""
	}
	if cd, ok := tok.(xml.CharData); ok {
		return string(cd)
	}
	return ""
}

// dateOnly returns the yyyy-mm-dd prefix of a date/datetime string.
func dateOnly(s string) string {
	s = strings.TrimSpace(s)
	if len(s) >= 10 {
		return s[:10]
	}
	return ""
}

// normalizeKey strips an "NFe"/"CTe" prefix and non-digits, returns 44 digits or "".
func normalizeKey(s string) string {
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, "NFe")
	s = strings.TrimPrefix(s, "CTe")
	var b strings.Builder
	for _, r := range s {
		if r >= '0' && r <= '9' {
			b.WriteRune(r)
		}
	}
	if d := b.String(); len(d) == 44 {
		return d
	}
	return ""
}

// classify maps root element + model code to a DocType. mod 65 = NFCe, 55 = NFe.
func classify(root, via, mod string) model.DocType {
	switch root {
	case "nfeProc", "NFe", "enviNFe":
		if mod == "65" {
			return model.DocNFCe
		}
		return model.DocNFe
	case "procEventoNFe", "envEvento", "evento", "retEvento", "resEvento":
		return model.DocEvento
	case "cteProc", "CTe":
		return model.DocCTe
	}
	switch via {
	case "chCTe":
		return model.DocCTe
	case "infNFe@Id", "chNFe":
		if mod == "65" {
			return model.DocNFCe
		}
		return model.DocNFe
	}
	return model.DocUnknown
}
