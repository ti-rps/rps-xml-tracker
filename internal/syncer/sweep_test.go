package syncer

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

// chaveN gera uma chave válida (44 dígitos, AAMM=2607 — dentro da janela do
// Now dos testes) distinta por i.
func chaveN(i int) string { return fmt.Sprintf("352607%038d", i) }

// newSyncerAt monta um syncer dry-run sobre um base FIXO (journal reutilizável
// entre instâncias — o teste de restart depende disso).
func newSyncerAt(t *testing.T, fb *fakeFB, base string, maxScan int) (*Syncer, string) {
	t.Helper()
	arrival := filepath.Join(base, "asincronizar")
	if err := os.MkdirAll(arrival, 0o755); err != nil {
		t.Fatal(err)
	}
	s, err := New(Config{
		Name: "TEST", ArrivalRoot: arrival, SyncRoot: filepath.Join(base, "sincronizado"),
		JournalPath: filepath.Join(base, "journal.db"),
		PlansPath:   filepath.Join(base, "plans.jsonl"),
		DryRun:      true, MaxPerCycle: 100, MaxScanPer: maxScan,
		Marker:   "sync rps-xml-tracker test",
		Empresas: map[int]bool{100: true, 200: true},
		Now:      func() time.Time { return time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC) },
	}, fb, fb, fb)
	if err != nil {
		t.Fatal(err)
	}
	return s, arrival
}

// planOrigens lê as origens planejadas do plans.jsonl (base name -> quantas vezes).
func planOrigens(t *testing.T, plansPath string) map[string]int {
	t.Helper()
	out := map[string]int{}
	b, err := os.ReadFile(plansPath)
	if os.IsNotExist(err) {
		return out
	}
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		if line == "" {
			continue
		}
		var p Plan
		if err := json.Unmarshal([]byte(line), &p); err != nil {
			t.Fatalf("plans.jsonl inválido: %v", err)
		}
		out[filepath.Base(p.Origem)]++
	}
	return out
}

// Rotação básica: com mais arquivos que MaxScanPer, ciclos consecutivos cobrem
// blocos DIFERENTES, todos os arquivos acabam planejados exatamente uma vez e o
// wrap-around acontece ao chegar no fim.
func TestSweepOnce_CursorRotativo(t *testing.T) {
	fb := newFake()
	s, arrival := newSyncerAt(t, fb, t.TempDir(), 3)
	t.Cleanup(func() { s.Close() })
	for i := 0; i < 8; i++ {
		writeXML(t, arrival, fmt.Sprintf("n%02d.xml", i), nfeXML(chaveN(i), cnpjA, cnpjB))
	}

	res1, err := s.SweepOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res1.Scanned != 3 || res1.Planned != 3 || res1.Wrapped {
		t.Fatalf("ciclo 1 = %+v; want 3 escaneados/planejados sem wrap", res1)
	}
	if res1.CursorStart != "" || res1.CursorEnd != "n02.xml" {
		t.Errorf("ciclo 1 cursor %q→%q; want \"\"→\"n02.xml\"", res1.CursorStart, res1.CursorEnd)
	}

	res2, err := s.SweepOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res2.CursorStart != "n02.xml" || res2.CursorEnd != "n05.xml" || res2.Planned != 3 {
		t.Errorf("ciclo 2 = %+v; deveria continuar de n02 a n05", res2)
	}

	// ciclo 3: n06, n07 e wrap-around de volta ao n00 (já planejado → ja_processada)
	res3, err := s.SweepOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !res3.Wrapped || res3.Scanned != 3 || res3.Planned != 2 || res3.Skips["ja_processada"] != 1 {
		t.Errorf("ciclo 3 = %+v; want wrap com n06+n07 planejados e n00 já processado", res3)
	}
	if res3.CursorEnd != "n00.xml" {
		t.Errorf("ciclo 3 cursor final = %q; want n00.xml", res3.CursorEnd)
	}

	origens := planOrigens(t, s.cfg.PlansPath)
	if len(origens) != 8 {
		t.Fatalf("origens planejadas = %d (%v); TODOS os 8 arquivos deveriam ter sido alcançados", len(origens), origens)
	}
	for nome, n := range origens {
		if n != 1 {
			t.Errorf("%s planejado %d vezes; want 1 (idempotência)", nome, n)
		}
	}
}

// O cursor sobrevive à recriação do processo: um syncer novo sobre o MESMO
// journal continua de onde o anterior parou.
func TestSweepOnce_CursorSobreviveRestart(t *testing.T) {
	base := t.TempDir()
	fb := newFake()
	s1, arrival := newSyncerAt(t, fb, base, 2)
	for i := 0; i < 4; i++ {
		writeXML(t, arrival, fmt.Sprintf("n%02d.xml", i), nfeXML(chaveN(i), cnpjA, cnpjB))
	}
	if _, err := s1.SweepOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := s1.Close(); err != nil {
		t.Fatal(err)
	}

	s2, _ := newSyncerAt(t, fb, base, 2)
	t.Cleanup(func() { s2.Close() })
	res, err := s2.SweepOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.CursorStart != "n01.xml" || res.Planned != 2 || res.Skips["ja_processada"] != 0 {
		t.Errorf("pós-restart = %+v; deveria retomar depois de n01 sem reexaminar o começo", res)
	}
	if got := planOrigens(t, s2.cfg.PlansPath); len(got) != 4 {
		t.Errorf("origens = %v; os 4 arquivos deveriam estar planejados", got)
	}
}

// Remover o arquivo apontado pelo cursor não bloqueia nem rebobina a varredura
// (a retomada compara por ORDEM, não por igualdade).
func TestSweepOnce_CursorRemovido(t *testing.T) {
	fb := newFake()
	s, arrival := newSyncerAt(t, fb, t.TempDir(), 2)
	t.Cleanup(func() { s.Close() })
	for i := 0; i < 4; i++ {
		writeXML(t, arrival, fmt.Sprintf("n%02d.xml", i), nfeXML(chaveN(i), cnpjA, cnpjB))
	}
	if _, err := s.SweepOnce(context.Background()); err != nil { // examina n00, n01
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(arrival, "n01.xml")); err != nil {
		t.Fatal(err)
	}
	res, err := s.SweepOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Planned != 2 || res.Skips["ja_processada"] != 0 || res.CursorEnd != "n03.xml" {
		t.Errorf("res = %+v; deveria seguir direto para n02/n03", res)
	}
}

// Arquivo NOVO que entra ANTES do cursor é alcançado na volta seguinte (wrap),
// não fica indefinidamente sem exame.
func TestSweepOnce_ArquivoNovoAntesDoCursor(t *testing.T) {
	fb := newFake()
	s, arrival := newSyncerAt(t, fb, t.TempDir(), 2)
	t.Cleanup(func() { s.Close() })
	for i := 1; i <= 4; i++ {
		writeXML(t, arrival, fmt.Sprintf("n%02d.xml", i), nfeXML(chaveN(i), cnpjA, cnpjB))
	}
	if _, err := s.SweepOnce(context.Background()); err != nil { // n01, n02
		t.Fatal(err)
	}
	writeXML(t, arrival, "n00.xml", nfeXML(chaveN(0), cnpjA, cnpjB)) // entra ANTES do cursor
	if _, err := s.SweepOnce(context.Background()); err != nil {     // n03, n04
		t.Fatal(err)
	}
	res, err := s.SweepOnce(context.Background()) // wrap → n00
	if err != nil {
		t.Fatal(err)
	}
	if !res.Wrapped {
		t.Errorf("res = %+v; want wrap", res)
	}
	if origens := planOrigens(t, s.cfg.PlansPath); origens["n00.xml"] != 1 {
		t.Errorf("origens = %v; n00.xml deveria ter sido planejado na volta", origens)
	}
}

// Arquivos que só geram SKIP (parse falhou etc.) também avançam o cursor — a
// starvation original vinha justamente de reexaminar os mesmos skips p/ sempre.
func TestSweepOnce_SkipsAvancamCursor(t *testing.T) {
	fb := newFake()
	s, arrival := newSyncerAt(t, fb, t.TempDir(), 2)
	t.Cleanup(func() { s.Close() })
	for i := 0; i < 4; i++ {
		writeXML(t, arrival, fmt.Sprintf("g%02d.xml", i), "isto não é um xml")
	}
	res1, err := s.SweepOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res1.Planned != 0 || res1.Skips["sem_chave"] != 2 || res1.CursorEnd != "g01.xml" {
		t.Fatalf("ciclo 1 = %+v; skips deveriam avançar o cursor até g01", res1)
	}
	res2, err := s.SweepOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res2.CursorStart != "g01.xml" || res2.CursorEnd != "g03.xml" {
		t.Errorf("ciclo 2 cursor %q→%q; want g01→g03 (sem reexaminar o prefixo)", res2.CursorStart, res2.CursorEnd)
	}
}

// Rotação com allowlist Dirs: o cursor atravessa os dirs na ordem, dá o wrap de
// volta ao primeiro, e uma mudança na allowlist invalida o cursor sem erro.
func TestSweepOnce_RotacaoEntreDirs(t *testing.T) {
	fb := newFake()
	s, arrival := newSyncerAt(t, fb, t.TempDir(), 1)
	t.Cleanup(func() { s.Close() })
	s.cfg.Dirs = []string{"d1", "d2"}
	for _, d := range s.cfg.Dirs {
		if err := os.MkdirAll(filepath.Join(arrival, d), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	writeXML(t, filepath.Join(arrival, "d1"), "a.xml", "lixo")
	writeXML(t, filepath.Join(arrival, "d2"), "b.xml", "lixo")

	res1, _ := s.SweepOnce(context.Background())
	if res1.CursorEnd != "d1/a.xml" {
		t.Fatalf("ciclo 1 cursor = %q; want d1/a.xml", res1.CursorEnd)
	}
	res2, _ := s.SweepOnce(context.Background())
	if res2.CursorEnd != "d2/b.xml" {
		t.Fatalf("ciclo 2 cursor = %q; want d2/b.xml", res2.CursorEnd)
	}
	res3, _ := s.SweepOnce(context.Background())
	if !res3.Wrapped || res3.CursorEnd != "d1/a.xml" {
		t.Fatalf("ciclo 3 = %+v; want wrap de volta a d1/a.xml", res3)
	}

	// allowlist mudou: o dir do cursor saiu — recomeça do zero, sem erro
	s.cfg.Dirs = []string{"d2"}
	res4, err := s.SweepOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res4.CursorStart != "" || res4.CursorEnd != "d2/b.xml" {
		t.Errorf("ciclo 4 = %+v; cursor de dir removido deveria ser descartado", res4)
	}
}

// Falha transitória do Firebird num ciclo NÃO exige reiniciar o processo nem
// mexe no cursor: o ciclo seguinte, com a mesma instância, funciona e retoma
// do mesmo ponto.
func TestSweepOnce_FirebirdCaiUmCiclo(t *testing.T) {
	fb := newFake()
	s, arrival := newSyncerAt(t, fb, t.TempDir(), 2)
	t.Cleanup(func() { s.Close() })
	for i := 0; i < 4; i++ {
		writeXML(t, arrival, fmt.Sprintf("n%02d.xml", i), nfeXML(chaveN(i), cnpjA, cnpjB))
	}
	if _, err := s.SweepOnce(context.Background()); err != nil { // n00, n01
		t.Fatal(err)
	}

	fb.failFiliais = 1
	if _, err := s.SweepOnce(context.Background()); err == nil {
		t.Fatal("com o Firebird fora, a varredura tem de reportar erro (nunca sucesso silencioso)")
	}

	res, err := s.SweepOnce(context.Background())
	if err != nil {
		t.Fatalf("o ciclo seguinte deveria recuperar sozinho: %v", err)
	}
	if res.CursorStart != "n01.xml" || res.Planned != 2 {
		t.Errorf("res = %+v; o ciclo falho não pode ter mexido no cursor", res)
	}
}

// pathLess segue a ordem do WalkDir (componente a componente), não a ordem das
// strings — nomes com caractere < '/' (espaço, '!', '-') divergem entre as duas.
func TestPathLess(t *testing.T) {
	for _, tc := range []struct {
		a, b string
		want bool
	}{
		{"a.xml", "b.xml", true},
		{"b.xml", "a.xml", false},
		{"a.xml", "a.xml", false},
		{"a", "a/x", true},    // prefixo (dir) vem antes do conteúdo
		{"a/x", "a!/y", true}, // WalkDir visita "a/" antes de "a!/" (string diria o oposto)
		{"a!/y", "a/x", false},
		{"d1/z.xml", "d2/a.xml", true},
		{"pasta/x", "pasta um/x", true}, // "pasta" < "pasta um" (prefixo primeiro)
	} {
		if got := pathLess(tc.a, tc.b); got != tc.want {
			t.Errorf("pathLess(%q, %q) = %v; want %v", tc.a, tc.b, got, tc.want)
		}
	}
}

// Journal ANTIGO (sem o bucket "sweep") continua legível: o bucket é criado no
// open e o cursor simplesmente começa vazio.
func TestJournal_CompatSemBucketSweep(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal.db")
	db, err := bolt.Open(path, 0o644, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = db.Update(func(tx *bolt.Tx) error {
		for _, b := range [][]byte{bucketParts, bucketDone, bucketDry} { // formato antigo
			if _, err := tx.CreateBucketIfNotExists(b); err != nil {
				return err
			}
		}
		return tx.Bucket(bucketDone).Put([]byte(chaveTeste), []byte("2026-07-01T00:00:00Z"))
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	jr, err := openJournal(path)
	if err != nil {
		t.Fatalf("journal antigo deveria abrir: %v", err)
	}
	t.Cleanup(func() { jr.Close() })
	if !jr.isDone(chaveTeste) {
		t.Error("dados antigos (done) deveriam continuar legíveis")
	}
	if _, ok := jr.getSweepCursor(); ok {
		t.Error("journal antigo não tem cursor — getSweepCursor deveria dizer não-encontrado")
	}
	if err := jr.setSweepCursor(sweepCursor{Dir: "", Path: "x.xml"}); err != nil {
		t.Fatal(err)
	}
	if c, ok := jr.getSweepCursor(); !ok || c.Path != "x.xml" {
		t.Errorf("cursor gravado deveria voltar na leitura: %+v %v", c, ok)
	}
}
