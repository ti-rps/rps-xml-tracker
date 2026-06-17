package store

import (
	"strings"
	"testing"
)

func TestIsCompleteChave(t *testing.T) {
	full := strings.Repeat("1", 44)
	cases := []struct {
		name string
		in   string
		want bool
	}{
		{"full 44 digits", full, true},
		{"empty", "", false},
		{"short partial", "3520", false},
		{"43 digits", strings.Repeat("9", 43), false},
		{"45 digits", strings.Repeat("9", 45), false},
		{"44 with a letter", strings.Repeat("1", 43) + "A", false},
		{"44 with spaces", strings.Repeat("1", 42) + "  ", false},
	}
	for _, c := range cases {
		if got := isCompleteChave(c.in); got != c.want {
			t.Errorf("%s: isCompleteChave(%q)=%v, want %v", c.name, c.in, got, c.want)
		}
	}
}
