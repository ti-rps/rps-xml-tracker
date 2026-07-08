package netpath

import "testing"

func TestDefault_Rede(t *testing.T) {
	m := Default()
	cases := []struct{ in, want string }{
		{`F:\Xml_ASincronizar\ZZZ_XML_BOT\nota.xml`, `R:\XML_ASINCRONIZAR\ZZZ_XML_BOT\nota.xml`},
		{`F:\XML SINCRONIZADO\12345678\202606\NFe\nota.xml`, `R:\XML_SINCRONIZADO\12345678\202606\NFe\nota.xml`},
		// case-insensitive como o NTFS
		{`f:\xml_asincronizar\sub\a.xml`, `R:\XML_ASINCRONIZAR\sub\a.xml`},
		{`F:\xml sincronizado\sub\a.xml`, `R:\XML_SINCRONIZADO\sub\a.xml`},
		// fora do fluxo mapeado: sem tradução
		{`C:\temp\nota.xml`, ""},
		{`F:\Outra\nota.xml`, ""},
		{"", ""},
	}
	for _, c := range cases {
		if got := m.Rede(c.in); got != c.want {
			t.Errorf("Rede(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestFromEnv(t *testing.T) {
	m := FromEnv(`X:\a\=Y:\b\;X:\c\=Y:\d\`)
	if got := m.Rede(`X:\a\f.xml`); got != `Y:\b\f.xml` {
		t.Errorf("Rede = %q, want Y:\\b\\f.xml", got)
	}
	if got := m.Rede(`X:\c\f.xml`); got != `Y:\d\f.xml` {
		t.Errorf("Rede = %q, want Y:\\d\\f.xml", got)
	}

	// vazio ou inválido cai no Default
	for _, v := range []string{"", ";;", "semigual"} {
		if got := FromEnv(v).Rede(`F:\Xml_ASincronizar\a.xml`); got != `R:\XML_ASINCRONIZAR\a.xml` {
			t.Errorf("FromEnv(%q) não caiu no Default (got %q)", v, got)
		}
	}
}
