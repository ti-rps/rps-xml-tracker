package model

import "testing"

func TestDirectionFromCNPJs(t *testing.T) {
	const emp = "12345678000190" // raiz 12345678
	cases := []struct {
		name, empCNPJ, emit, dest, want string
	}{
		{"saida: empresa é a emitente", emp, "12345678000190", "99888777000166", DirSaida},
		{"entrada: empresa é a destinatária", emp, "99888777000166", "12345678000190", DirEntrada},
		{"raiz casa mesmo com filial diferente", emp, "12345678000272", "99888777000166", DirSaida},
		{"intra-grupo (emit e dest) -> saida", emp, "12345678000190", "12345678000272", DirSaida},
		{"CNPJ formatado (pontuação) ainda casa", emp, "12.345.678/0001-90", "x", DirSaida},
		{"nenhum lado casa -> indeterminada", emp, "11111111000111", "22222222000122", ""},
		{"empresa sem CNPJ -> indeterminada", "", "12345678000190", "12345678000190", ""},
	}
	for _, c := range cases {
		if got := DirectionFromCNPJs(c.empCNPJ, c.emit, c.dest); got != c.want {
			t.Errorf("%s: got %q, want %q", c.name, got, c.want)
		}
	}
}
