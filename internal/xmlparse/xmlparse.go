// Package xmlparse extracts the chave de acesso and document type from a fiscal
// XML file, READ-ONLY. The filename does NOT contain the chave (Fase 0), so we
// must read the content. It streams the XML and stops as soon as it has the
// chave + type, and never loads the whole DOM.
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
}

// ParseFile opens path read-only and extracts the chave/type.
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
	var inChNFe, inChCTe bool
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
			if res.RootElem == "" {
				res.RootElem = t.Name.Local
			}
			switch t.Name.Local {
			case "infNFe", "infNFCe":
				for _, a := range t.Attr {
					if a.Name.Local == "Id" && res.Chave == "" {
						if k := normalizeKey(a.Value); k != "" {
							res.Chave, res.ChaveVia = k, "infNFe@Id"
						}
					}
				}
			case "mod":
				if v, ok := charData(dec); ok {
					mod = strings.TrimSpace(v)
				}
			case "chNFe":
				inChNFe = true
			case "chCTe":
				inChCTe = true
			}
		case xml.CharData:
			if inChNFe && res.Chave == "" {
				if k := normalizeKey(string(t)); k != "" {
					res.Chave, res.ChaveVia = k, "chNFe"
				}
			} else if inChCTe && res.Chave == "" {
				if k := normalizeKey(string(t)); k != "" {
					res.Chave, res.ChaveVia = k, "chCTe"
				}
			}
		case xml.EndElement:
			switch t.Name.Local {
			case "chNFe":
				inChNFe = false
			case "chCTe":
				inChCTe = false
			}
		}
		if res.Chave != "" && res.RootElem != "" && mod != "" {
			break
		}
	}
	res.DocType = classify(res.RootElem, res.ChaveVia, mod)
	return res, nil
}

func charData(dec *xml.Decoder) (string, bool) {
	tok, err := dec.Token()
	if err != nil {
		return "", false
	}
	if cd, ok := tok.(xml.CharData); ok {
		return string(cd), true
	}
	return "", false
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
	// fallback by how the chave was found
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
