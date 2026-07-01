// Package reconcile contém a lógica pura de comparação de conjuntos de chaves entre o
// Athenas (fonte da verdade) e o tracker, para a auditoria de acurácia do import.
package reconcile

import "sort"

// Diff compara o conjunto do Athenas (autoritativo) com o do tracker e retorna, ordenados
// e sem duplicatas: as chaves que faltam no tracker (Athenas tem, tracker não) e as que
// sobram no tracker (tracker tem, Athenas não). Entradas duplicadas de qualquer lado são
// colapsadas — o que importa é a presença no conjunto.
func Diff(athena, tracker []string) (missing, extra []string) {
	sa := toSet(athena)
	st := toSet(tracker)
	for c := range sa {
		if _, ok := st[c]; !ok {
			missing = append(missing, c)
		}
	}
	for c := range st {
		if _, ok := sa[c]; !ok {
			extra = append(extra, c)
		}
	}
	sort.Strings(missing)
	sort.Strings(extra)
	return missing, extra
}

func toSet(s []string) map[string]struct{} {
	m := make(map[string]struct{}, len(s))
	for _, c := range s {
		if c != "" {
			m[c] = struct{}{}
		}
	}
	return m
}
