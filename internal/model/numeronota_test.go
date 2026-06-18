package model

import "testing"

func TestNumeroNota(t *testing.T) {
	cases := map[string]string{
		"29260619131398000123650100013687841016616987": "1368784", // nNF posições 26-34, sem zeros
		"":           "",
		"123":        "",
		"abc":        "", // 3 chars, não 44 -> ""
	}
	for chave, want := range cases {
		if got := NumeroNota(chave); got != want {
			t.Errorf("NumeroNota(%q)=%q, want %q", chave, got, want)
		}
	}
}
