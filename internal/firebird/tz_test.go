package firebird

import (
	"testing"
	"time"
)

// TestFbLocalTime garante que um TIMESTAMP do Firebird devolvido como wall-clock UTC
// (bug do driver) é reinterpretado como horário de Brasília: mesmos componentes, fuso
// -03:00, instante deslocado +3h. Era a causa do imported_at "18/06 21:00" para uma
// importação datada 19/06 (date-only): 19/06 00:00 lido como UTC = 18/06 21:00 BRT.
func TestFbLocalTime(t *testing.T) {
	// date-only 19/06 que o driver entregava como meia-noite UTC
	in := time.Date(2026, 6, 19, 0, 0, 0, 0, time.UTC)
	got := fbLocalTime(in)

	// componentes do wall-clock preservados
	if got.Year() != 2026 || got.Month() != time.June || got.Day() != 19 ||
		got.Hour() != 0 || got.Minute() != 0 || got.Second() != 0 {
		t.Errorf("wall-clock alterado: %s", got.Format("2006-01-02 15:04:05"))
	}
	// offset -03:00 (Brasil sem DST)
	if _, off := got.Zone(); off != -3*3600 {
		t.Errorf("offset=%d want -10800", off)
	}
	// instante correto: 19/06 00:00 BRT = 19/06 03:00 UTC (era 19/06 00:00 UTC = bug)
	if want := time.Date(2026, 6, 19, 3, 0, 0, 0, time.UTC); !got.UTC().Equal(want) {
		t.Errorf("instante=%s want %s", got.UTC(), want)
	}

	// um valor com hora-do-dia real preserva a hora (só re-rotula o fuso)
	in2 := time.Date(2026, 6, 19, 9, 30, 15, 0, time.UTC)
	got2 := fbLocalTime(in2)
	if got2.Hour() != 9 || got2.Minute() != 30 || got2.Second() != 15 {
		t.Errorf("hora-do-dia alterada: %s", got2.Format("15:04:05"))
	}
	if want := time.Date(2026, 6, 19, 12, 30, 15, 0, time.UTC); !got2.UTC().Equal(want) {
		t.Errorf("instante2=%s want %s", got2.UTC(), want)
	}
}
