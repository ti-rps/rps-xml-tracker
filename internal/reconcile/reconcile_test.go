package reconcile

import (
	"reflect"
	"testing"
)

func TestDiff(t *testing.T) {
	athena := []string{"A", "B", "C", "C"} // C duplicado (colapsa)
	tracker := []string{"B", "C", "D", ""} // "" ignorado

	missing, extra := Diff(athena, tracker)
	if want := []string{"A"}; !reflect.DeepEqual(missing, want) {
		t.Errorf("missing = %v, want %v (A está no Athenas e não no tracker)", missing, want)
	}
	if want := []string{"D"}; !reflect.DeepEqual(extra, want) {
		t.Errorf("extra = %v, want %v (D está no tracker e não no Athenas)", extra, want)
	}
}

func TestDiff_Iguais(t *testing.T) {
	missing, extra := Diff([]string{"X", "Y"}, []string{"Y", "X"})
	if len(missing) != 0 || len(extra) != 0 {
		t.Errorf("conjuntos iguais deveriam dar diff vazio; got missing=%v extra=%v", missing, extra)
	}
}
