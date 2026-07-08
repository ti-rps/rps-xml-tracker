// Package netpath traduz caminhos internos do SRVIMPORT (a visão do agente, ex.:
// F:\Xml_ASincronizar\...) para o caminho de rede equivalente que qualquer máquina
// do escritório enxerga (R:\XML_ASINCRONIZAR\..., mapeamento de \\srvdoc01\REDE).
// É tradução de EXIBIÇÃO: o caminho interno segue sendo o dado canônico gravado
// nas observations; a API anexa a versão de rede na resposta.
package netpath

import "strings"

// Rule mapeia um prefixo interno para o prefixo de rede correspondente.
type Rule struct {
	Interno string
	Rede    string
}

// Mapper aplica a primeira regra cujo prefixo interno casa (case-insensitive,
// como o filesystem do Windows).
type Mapper struct {
	rules []Rule
}

// Default é o mapeamento real do SRVIMPORT → share \\srvdoc01\REDE. Atenção aos
// nomes: o interno usa espaço ("XML SINCRONIZADO") e o de rede usa underscore.
func Default() *Mapper {
	return &Mapper{rules: []Rule{
		{Interno: `F:\Xml_ASincronizar\`, Rede: `R:\XML_ASINCRONIZAR\`},
		{Interno: `F:\XML SINCRONIZADO\`, Rede: `R:\XML_SINCRONIZADO\`},
	}}
}

// FromEnv monta um Mapper a partir de "interno=rede;interno=rede" (valor da env
// TRACKER_NETPATH_MAP). Vazio ou só pares inválidos caem no Default.
func FromEnv(v string) *Mapper {
	var rules []Rule
	for _, pair := range strings.Split(v, ";") {
		interno, rede, ok := strings.Cut(pair, "=")
		if !ok || strings.TrimSpace(interno) == "" || strings.TrimSpace(rede) == "" {
			continue
		}
		rules = append(rules, Rule{Interno: strings.TrimSpace(interno), Rede: strings.TrimSpace(rede)})
	}
	if len(rules) == 0 {
		return Default()
	}
	return &Mapper{rules: rules}
}

// Rede devolve o caminho na visão da rede, ou "" quando nenhum prefixo casa
// (não inventa caminho de rede para pastas fora do fluxo mapeado).
func (m *Mapper) Rede(path string) string {
	for _, r := range m.rules {
		if len(path) >= len(r.Interno) && strings.EqualFold(path[:len(r.Interno)], r.Interno) {
			return r.Rede + path[len(r.Interno):]
		}
	}
	return ""
}
