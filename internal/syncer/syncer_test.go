package syncer

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/EnzzoHosaki/rps-xml-tracker/internal/firebird"
	"github.com/EnzzoHosaki/rps-xml-tracker/internal/model"
)

const (
	cnpjA = "11222333000181" // filial da empresa 100 (emitente -> saída)
	cnpjB = "99888777000166" // filial da empresa 200 (destinatária -> entrada)
)

// chave válida de 44 dígitos com emissão 2026-07 (AAMM=2607).
var chaveTeste = "352607" + strings.Repeat("1", 38)

// nfeXML monta um NFe mínimo que o xmlparse entende.
func nfeXML(chave, emit, dest string) string {
	return `<?xml version="1.0"?><nfeProc><NFe><infNFe Id="NFe` + chave + `">` +
		`<ide><mod>55</mod><dhEmi>2026-07-01T10:00:00-03:00</dhEmi></ide>` +
		`<emit><CNPJ>` + emit + `</CNPJ><xNome>EMISSOR TESTE</xNome></emit>` +
		`<dest><CNPJ>` + dest + `</CNPJ><xNome>DESTINO TESTE</xNome></dest>` +
		`<total><ICMSTot><vNF>123.45</vNF></ICMSTot></total>` +
		`</infNFe></NFe></nfeProc>`
}

// fakeFB implementa resolver+inserter+submitter em memória.
type fakeFB struct {
	filiais     []firebird.Filial
	nomes       map[int]string
	rows        map[string]bool // "chave|emp/fil"
	imported    map[string]bool // subconjunto de rows com IMPORTADO=1 (Horse importou)
	inserts     []firebird.InsertRow
	nextID      int64
	failInsert  bool
	failFiliais int // nº de ListFiliais que ainda vão falhar (simula sessão morta)
	obs         []model.Observation
}

func rowKey(chave string, emp, fil int) string { return fmt.Sprintf("%s|%d/%d", chave, emp, fil) }

func (f *fakeFB) ListFiliais(context.Context) ([]firebird.Filial, error) {
	if f.failFiliais > 0 {
		f.failFiliais--
		return nil, fmt.Errorf("connection shutdown\nKilled by database administrator.")
	}
	return f.filiais, nil
}
func (f *fakeFB) EmpresaNomes(context.Context) (map[int]string, error) { return f.nomes, nil }
func (f *fakeFB) HasRow(_ context.Context, chave string, emp, fil int) (bool, error) {
	return f.rows[rowKey(chave, emp, fil)], nil
}
func (f *fakeFB) HasImportedRow(_ context.Context, chave, _ string) (bool, error) {
	for k := range f.imported {
		if strings.HasPrefix(k, chave+"|") && f.rows[k] {
			return true, nil
		}
	}
	return false, nil
}
func (f *fakeFB) NextChaveID(context.Context) (int64, error) { f.nextID++; return f.nextID, nil }
func (f *fakeFB) DeleteOurRows(_ context.Context, chave, _ string) (int64, error) {
	var n int64
	for k := range f.rows {
		if strings.HasPrefix(k, chave+"|") && !f.imported[k] { // espelha o filtro IMPORTADO=0
			delete(f.rows, k)
			n++
		}
	}
	return n, nil
}
func (f *fakeFB) CountOurRows(context.Context, string, time.Time) (firebird.OurRowsCount, error) {
	total := int64(len(f.rows))
	var imp int64
	for k := range f.rows {
		if f.imported[k] {
			imp++
		}
	}
	return firebird.OurRowsCount{Total: total, Pending: total - imp, Imported: imp}, nil
}
func (f *fakeFB) ListOurRows(_ context.Context, _ string, onlyPending bool, _ time.Time) ([]firebird.OurRow, error) {
	var out []firebird.OurRow
	for k := range f.rows {
		if onlyPending && f.imported[k] {
			continue
		}
		parts := strings.SplitN(k, "|", 2)
		var emp, fil int
		fmt.Sscanf(parts[1], "%d/%d", &emp, &fil)
		out = append(out, firebird.OurRow{Chave: parts[0], CodigoEmpresa: emp, CodigoFilial: fil})
	}
	return out, nil
}
func (f *fakeFB) InsertChaveAcesso(_ context.Context, _ int64, r firebird.InsertRow) error {
	if f.failInsert {
		return fmt.Errorf("firebird indisponível (simulado)")
	}
	f.inserts = append(f.inserts, r)
	f.rows[rowKey(r.Chave, r.CodigoEmpresa, r.CodigoFilial)] = true
	return nil
}
func (f *fakeFB) Submit(_ context.Context, batch []model.Observation) error {
	f.obs = append(f.obs, batch...)
	return nil
}

func newFake() *fakeFB {
	return &fakeFB{
		filiais: []firebird.Filial{
			{CodigoEmpresa: 100, CodigoFilial: 1, Cnpj: cnpjA},
			{CodigoEmpresa: 200, CodigoFilial: 1, Cnpj: cnpjB},
		},
		nomes:    map[int]string{100: "EMPRESA A LTDA", 200: "EMPRESA B & FILHOS"},
		rows:     map[string]bool{},
		imported: map[string]bool{},
	}
}

// newSyncer monta um syncer sobre diretórios temporários.
func newSyncer(t *testing.T, fb *fakeFB, dry bool) (*Syncer, string, string) {
	t.Helper()
	base := t.TempDir()
	arrival := filepath.Join(base, "asincronizar")
	syncRoot := filepath.Join(base, "sincronizado")
	if err := os.MkdirAll(arrival, 0o755); err != nil {
		t.Fatal(err)
	}
	s, err := New(Config{
		Name: "TEST", ArrivalRoot: arrival, SyncRoot: syncRoot,
		JournalPath: filepath.Join(base, "journal.db"),
		PlansPath:   filepath.Join(base, "plans.jsonl"),
		DryRun:      dry, MaxPerCycle: 10, Marker: "sync rps-xml-tracker test",
		Empresas: map[int]bool{100: true, 200: true},
		Now:      func() time.Time { return time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC) },
	}, fb, fb, fb)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s, arrival, syncRoot
}

func writeXML(t *testing.T, dir, name, content string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func countEvents(obs []model.Observation, event string) int {
	n := 0
	for _, o := range obs {
		if o.EventType == event {
			n++
		}
	}
	return n
}

// O caminho feliz completo com MULTI-PARTICIPAÇÃO: uma cópia + um INSERT por
// empresa, origem removida só no fim, observações por participação.
func TestExecute_MultiParticipacao(t *testing.T) {
	fb := newFake()
	s, arrival, syncRoot := newSyncer(t, fb, false)
	origem := writeXML(t, arrival, "nota.xml", nfeXML(chaveTeste, cnpjA, cnpjB))

	if err := s.refreshResolve(context.Background()); err != nil {
		t.Fatal(err)
	}
	plan := s.PlanFile(context.Background(), origem, true)
	if plan.Skip != "" {
		t.Fatalf("plan.Skip = %q", plan.Skip)
	}
	if len(plan.Participacoes) != 2 {
		t.Fatalf("participações = %+v; want 2", plan.Participacoes)
	}
	// direções: A emitente (saída), B destinatária (entrada)
	if plan.Participacoes[0].Direction != model.DirSaida || plan.Participacoes[1].Direction != model.DirEntrada {
		t.Errorf("direções erradas: %+v", plan.Participacoes)
	}

	if err := s.Execute(context.Background(), plan); err != nil {
		t.Fatal(err)
	}

	// as duas cópias existem, cada uma na pasta da sua empresa
	for _, part := range plan.Participacoes {
		if _, err := os.Stat(part.DestAbs); err != nil {
			t.Errorf("destino %s: %v", part.DestAbs, err)
		}
		if !strings.HasPrefix(part.DestAbs, syncRoot) {
			t.Errorf("destino fora do SyncRoot: %s", part.DestAbs)
		}
	}
	// origem removida SÓ no fim
	if _, err := os.Stat(origem); !os.IsNotExist(err) {
		t.Error("origem deveria ter sido removida")
	}
	// um INSERT por participação, com o marcador
	if len(fb.inserts) != 2 {
		t.Fatalf("inserts = %d; want 2", len(fb.inserts))
	}
	for _, r := range fb.inserts {
		if r.Observacoes != "sync rps-xml-tracker test" {
			t.Errorf("marcador ausente: %+v", r.Observacoes)
		}
		if r.URL == "" || !strings.HasPrefix(r.URL, `\`) {
			t.Errorf("URL relativa inválida: %q", r.URL)
		}
	}
	// observações: sync_moved + sync_db_inserted por participação, nenhum failed
	if n := countEvents(fb.obs, model.EventSyncMoved); n != 2 {
		t.Errorf("sync_moved = %d; want 2", n)
	}
	if n := countEvents(fb.obs, model.EventSyncDBInserted); n != 2 {
		t.Errorf("sync_db_inserted = %d; want 2", n)
	}
	if n := countEvents(fb.obs, model.EventSyncFailed); n != 0 {
		t.Errorf("sync_failed = %d; want 0", n)
	}
	// journal: done
	if !s.jr.isDone(chaveTeste) {
		t.Error("journal deveria marcar a chave como done")
	}
}

// Falha no INSERT: arquivo posicionado, origem INTACTA, sync_failed emitido.
// Retry (Firebird de volta) só refaz o INSERT — não re-copia.
func TestExecute_FalhaNoInsertERetomada(t *testing.T) {
	fb := newFake()
	fb.failInsert = true
	s, arrival, _ := newSyncer(t, fb, false)
	origem := writeXML(t, arrival, "nota.xml", nfeXML(chaveTeste, cnpjA, cnpjB))

	_ = s.refreshResolve(context.Background())
	plan := s.PlanFile(context.Background(), origem, true)
	if err := s.Execute(context.Background(), plan); err == nil {
		t.Fatal("Execute deveria falhar com o INSERT indisponível")
	}
	if _, err := os.Stat(origem); err != nil {
		t.Fatal("origem NUNCA pode sumir sem linha no banco")
	}
	if n := countEvents(fb.obs, model.EventSyncFailed); n != 1 {
		t.Errorf("sync_failed = %d; want 1", n)
	}

	// retry: Firebird voltou
	fb.failInsert = false
	if err := s.Execute(context.Background(), plan); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if _, err := os.Stat(origem); !os.IsNotExist(err) {
		t.Error("origem deveria ter sido removida após o retry completo")
	}
	if len(fb.inserts) != 2 {
		t.Errorf("inserts = %d; want 2", len(fb.inserts))
	}
	// exatamente UM sync_moved por participação no total: a que moveu antes da
	// falha NÃO re-copia/re-emite no retry (o pre-check viu o destino ok); a
	// outra faz seu primeiro (e único) move no retry.
	if n := countEvents(fb.obs, model.EventSyncMoved); n != 2 {
		t.Errorf("sync_moved total = %d; want 2 (um por participação, sem duplicata)", n)
	}
}

// Conflito: destino já existe com CONTEÚDO DIVERGENTE -> nunca sobrescrever.
func TestExecute_ConflitoDeConteudo(t *testing.T) {
	fb := newFake()
	s, arrival, _ := newSyncer(t, fb, false)
	origem := writeXML(t, arrival, "nota.xml", nfeXML(chaveTeste, cnpjA, cnpjB))

	_ = s.refreshResolve(context.Background())
	plan := s.PlanFile(context.Background(), origem, true)
	// planta um intruso com outra chave no destino da 1ª participação
	intruso := plan.Participacoes[0].DestAbs
	if err := os.MkdirAll(filepath.Dir(intruso), 0o755); err != nil {
		t.Fatal(err)
	}
	outraChave := "352607" + strings.Repeat("9", 38)
	if err := os.WriteFile(intruso, []byte(nfeXML(outraChave, cnpjA, cnpjB)), 0o644); err != nil {
		t.Fatal(err)
	}

	err := s.Execute(context.Background(), plan)
	if err == nil || !strings.Contains(err.Error(), "conflito") {
		t.Fatalf("esperado erro de conflito, veio %v", err)
	}
	// intruso intacto, origem intacta
	b, _ := os.ReadFile(intruso)
	if !strings.Contains(string(b), outraChave) {
		t.Error("o destino conflitante foi sobrescrito — NUNCA pode")
	}
	if _, err := os.Stat(origem); err != nil {
		t.Error("origem não pode sumir num conflito")
	}
}

// Dry-run: varredura planeja, grava o JSONL e NÃO escreve nada em lugar nenhum;
// o ciclo seguinte não re-planeja a mesma chave.
func TestSweepOnce_DryRun(t *testing.T) {
	fb := newFake()
	s, arrival, syncRoot := newSyncer(t, fb, true)
	writeXML(t, arrival, "nota.xml", nfeXML(chaveTeste, cnpjA, cnpjB))

	res, err := s.SweepOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Planned != 1 || res.Executed != 0 {
		t.Fatalf("res = %+v; want 1 planejado, 0 executados", res)
	}
	if len(fb.inserts) != 0 || len(fb.obs) != 0 {
		t.Error("dry-run não pode inserir nem emitir observações")
	}
	if _, err := os.Stat(syncRoot); !os.IsNotExist(err) {
		t.Error("dry-run não pode criar nada no SINCRONIZADO")
	}
	b, err := os.ReadFile(s.cfg.PlansPath)
	if err != nil || !strings.Contains(string(b), chaveTeste) {
		t.Errorf("plano deveria estar no JSONL: %v", err)
	}

	// segundo ciclo: mesma chave não é re-planejada
	res2, err := s.SweepOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res2.Planned != 0 || res2.Skips["ja_processada"] != 1 {
		t.Errorf("res2 = %+v; a chave já planejada deveria ser pulada", res2)
	}
}

// A janela do AthenasHorse: emissão velha não sincroniza (vira lixo IMPORTADO=0
// eterno); --allow-stale destrava.
func TestPlanFile_JanelaDeEmissao(t *testing.T) {
	fb := newFake()
	s, arrival, _ := newSyncer(t, fb, false)
	_ = s.refreshResolve(context.Background())

	velho := strings.Replace(nfeXML(chaveTeste, cnpjA, cnpjB),
		"2026-07-01T10:00:00-03:00", "2026-03-01T10:00:00-03:00", 1)
	origem := writeXML(t, arrival, "velha.xml", velho)

	plan := s.PlanFile(context.Background(), origem, true)
	if !strings.Contains(plan.Skip, "janela") {
		t.Errorf("plan.Skip = %q; esperado skip pela janela", plan.Skip)
	}

	s.cfg.AllowStale = true
	plan = s.PlanFile(context.Background(), origem, true)
	if plan.Skip != "" {
		t.Errorf("com AllowStale deveria planejar; skip = %q", plan.Skip)
	}
}

// Allowlist na varredura: participação fora da lista descarta o arquivo INTEIRO
// (multi-participação não sincroniza pela metade).
// Rollback (§10): desfaz um sync real — apaga NOSSAS linhas, restaura a origem,
// apaga as cópias do destino e emite sync_failed "rollback manual".
func TestRollback_DesfazSync(t *testing.T) {
	fb := newFake()
	s, arrival, _ := newSyncer(t, fb, false) // modo real
	origem := writeXML(t, arrival, "nota.xml", nfeXML(chaveTeste, cnpjA, cnpjB))

	if _, err := s.RunChave(context.Background(), chaveTeste, origem); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if len(fb.inserts) != 2 {
		t.Fatalf("pré-condição: esperava 2 inserts, teve %d", len(fb.inserts))
	}
	if _, err := os.Stat(origem); !os.IsNotExist(err) {
		t.Fatalf("pré-condição: origem deveria ter saído após o sync")
	}

	res, err := s.Rollback(context.Background(), chaveTeste)
	if err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if res.RowsDeleted != 2 {
		t.Errorf("RowsDeleted=%d; want 2", res.RowsDeleted)
	}
	for _, ef := range [][2]int{{100, 1}, {200, 1}} {
		if has, _ := fb.HasRow(context.Background(), chaveTeste, ef[0], ef[1]); has {
			t.Errorf("linha %d/%d deveria ter sido apagada", ef[0], ef[1])
		}
	}
	if _, err := os.Stat(origem); err != nil {
		t.Errorf("origem deveria ter sido restaurada: %v", err)
	}
	if res.FilesRestored != 1 || res.FilesDeleted != 2 {
		t.Errorf("arquivos: restaurados=%d apagados=%d; want 1 e 2", res.FilesRestored, res.FilesDeleted)
	}
	var rb int
	for _, o := range fb.obs {
		if o.EventType == model.EventSyncFailed {
			rb++
		}
	}
	if rb != 2 {
		t.Errorf("esperava 2 sync_failed de rollback, teve %d", rb)
	}
}

// RollbackAll enumera as chaves pendentes pelo banco e desfaz cada uma; cada
// DELETE segue chave-scoped e o arquivo é restaurado por chave.
func TestRollbackAll_DesfazTodasPendentes(t *testing.T) {
	fb := newFake()
	s, arrival, _ := newSyncer(t, fb, false) // modo real
	// duas notas de empresas diferentes, ambas sincronizadas.
	chave2 := "352607" + strings.Repeat("2", 38)
	o1 := writeXML(t, arrival, "n1.xml", nfeXML(chaveTeste, cnpjA, cnpjB))
	o2 := writeXML(t, arrival, "n2.xml", nfeXML(chave2, cnpjA, cnpjB))
	for _, c := range []struct{ ch, f string }{{chaveTeste, o1}, {chave2, o2}} {
		if _, err := s.RunChave(context.Background(), c.ch, c.f); err != nil {
			t.Fatalf("sync %s: %v", c.ch, err)
		}
	}
	if len(fb.rows) != 4 { // 2 chaves × 2 participações
		t.Fatalf("pré-condição: esperava 4 linhas, teve %d", len(fb.rows))
	}

	res, err := s.RollbackAll(context.Background())
	if err != nil {
		t.Fatalf("rollback-all: %v", err)
	}
	if res.Chaves != 2 || res.RowsDeleted != 4 {
		t.Errorf("chaves=%d linhas=%d; want 2 e 4", res.Chaves, res.RowsDeleted)
	}
	if res.Failures != 0 {
		t.Errorf("falhas=%d; want 0 (avisos: %v)", res.Failures, res.Warnings)
	}
	if len(fb.rows) != 0 {
		t.Errorf("todas as linhas deveriam ter sumido, restaram %d", len(fb.rows))
	}
	for _, o := range []string{o1, o2} {
		if _, err := os.Stat(o); err != nil {
			t.Errorf("origem %s deveria ter sido restaurada: %v", o, err)
		}
	}
}

// RunWorklist (real): sincroniza os itens da lista sem varrer o filesystem —
// move o arquivo e insere a linha, igual ao sweep, mas dirigido pela lista.
func TestRunWorklist_Real(t *testing.T) {
	fb := newFake()
	s, arrival, _ := newSyncer(t, fb, false)
	o := writeXML(t, arrival, "n.xml", nfeXML(chaveTeste, cnpjA, cnpjB))
	res, err := s.RunWorklist(context.Background(), []WorklistItem{{Chave: chaveTeste, FilePath: o}})
	if err != nil {
		t.Fatal(err)
	}
	if res.Executed != 1 || res.Errors != 0 {
		t.Errorf("executed=%d errors=%d; want 1,0", res.Executed, res.Errors)
	}
	if len(fb.rows) == 0 {
		t.Error("esperava linha(s) inserida(s)")
	}
	if _, err := os.Stat(o); !os.IsNotExist(err) {
		t.Error("origem deveria ter sido removida após o sync")
	}
}

// RunWorklist (dry-run): planeja, não escreve.
func TestRunWorklist_DryRun(t *testing.T) {
	fb := newFake()
	s, arrival, _ := newSyncer(t, fb, true)
	o := writeXML(t, arrival, "n.xml", nfeXML(chaveTeste, cnpjA, cnpjB))
	res, err := s.RunWorklist(context.Background(), []WorklistItem{{Chave: chaveTeste, FilePath: o}})
	if err != nil {
		t.Fatal(err)
	}
	if res.Planned != 1 || res.Executed != 0 {
		t.Errorf("planned=%d executed=%d; want 1,0", res.Planned, res.Executed)
	}
	if len(fb.rows) != 0 {
		t.Error("dry-run não deveria inserir")
	}
}

// RunWorklist protege contra file_path stale: se o arquivo tem chave diferente da
// esperada (movido/substituído), o item é pulado, não sincronizado errado.
func TestRunWorklist_ChaveDivergente(t *testing.T) {
	fb := newFake()
	s, arrival, _ := newSyncer(t, fb, false)
	o := writeXML(t, arrival, "n.xml", nfeXML(chaveTeste, cnpjA, cnpjB))
	outra := "35" + strings.Repeat("9", 42) // 44 díg., mas não é a chave do arquivo
	res, err := s.RunWorklist(context.Background(), []WorklistItem{{Chave: outra, FilePath: o}})
	if err != nil {
		t.Fatal(err)
	}
	if res.Mismatch != 1 || res.Executed != 0 {
		t.Errorf("mismatch=%d executed=%d; want 1,0", res.Mismatch, res.Executed)
	}
	if _, err := os.Stat(o); err != nil {
		t.Error("origem NÃO deveria ter sido tocada num mismatch")
	}
}

// Guarda de importada: numa nota multi-participação com UMA participação já
// importada (IMPORTADO=1), o rollback apaga só a pendente e NÃO toca em arquivo
// (o XML importado segue referenciado pelo livro). Achado do code-review.
func TestRollback_ParticipacaoImportada_PreservaArquivos(t *testing.T) {
	fb := newFake()
	s, arrival, _ := newSyncer(t, fb, false) // modo real
	o := writeXML(t, arrival, "n.xml", nfeXML(chaveTeste, cnpjA, cnpjB))
	plan, err := s.RunChave(context.Background(), chaveTeste, o)
	if err != nil {
		t.Fatalf("sync: %v", err)
	}
	if len(plan.Participacoes) != 2 || len(fb.rows) != 2 {
		t.Fatalf("pré-condição: 2 participações/linhas; teve %d/%d", len(plan.Participacoes), len(fb.rows))
	}
	// o Horse importou a 1ª participação.
	imp := plan.Participacoes[0]
	fb.imported[rowKey(chaveTeste, imp.CodigoEmpresa, imp.CodigoFilial)] = true

	res, err := s.Rollback(context.Background(), chaveTeste)
	if err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if !res.Skipped || res.SkipReason == "" {
		t.Errorf("esperava Skipped com motivo (participação importada); res=%+v", res)
	}
	if res.RowsDeleted != 1 {
		t.Errorf("RowsDeleted=%d; want 1 (só a pendente)", res.RowsDeleted)
	}
	if res.FilesRestored != 0 || res.FilesDeleted != 0 {
		t.Errorf("arquivos NÃO deviam ser tocados; restaurados=%d apagados=%d", res.FilesRestored, res.FilesDeleted)
	}
	if !fb.rows[rowKey(chaveTeste, imp.CodigoEmpresa, imp.CodigoFilial)] {
		t.Error("a linha IMPORTADA foi apagada — não deveria")
	}
	for _, p := range plan.Participacoes {
		if _, err := os.Stat(p.DestAbs); err != nil {
			t.Errorf("destino %s deveria estar preservado: %v", p.DestAbs, err)
		}
	}
	// nenhum sync_failed de rollback (não desfizemos de verdade).
	for _, ob := range fb.obs {
		if ob.EventType == model.EventSyncFailed {
			t.Error("não deveria emitir sync_failed de rollback numa chave importada")
		}
	}
}

// RollbackAll pula (sem apagar) chaves sem registro no journal local — apagar a
// linha deixaria o XML órfão no SINCRONIZADO. Achado do code-review.
func TestRollbackAll_SemJournal_Pula(t *testing.T) {
	fb := newFake()
	s, _, _ := newSyncer(t, fb, false)
	// linha pendente no banco SEM entrada no journal (ex.: outra máquina/instância).
	fb.rows[rowKey(chaveTeste, 100, 1)] = true

	res, err := s.RollbackAll(context.Background())
	if err != nil {
		t.Fatalf("rollback-all: %v", err)
	}
	if res.Skipped != 1 || res.Chaves != 0 || res.RowsDeleted != 0 {
		t.Errorf("esperava pular sem apagar: puladas=%d desfeitas=%d apagadas=%d", res.Skipped, res.Chaves, res.RowsDeleted)
	}
	if !fb.rows[rowKey(chaveTeste, 100, 1)] {
		t.Error("a linha foi apagada sem journal — deixaria o XML órfão")
	}
}

// SelfTestRollback insere uma isca sintética e a desfaz, verificando que a
// contagem geral não mudou (o filtro só pegou a isca).
func TestSelfTestRollback_OK(t *testing.T) {
	fb := newFake()
	s, _, _ := newSyncer(t, fb, false) // modo real

	res, err := s.SelfTestRollback(context.Background())
	if err != nil {
		t.Fatalf("selftest: %v", err)
	}
	if !res.OK {
		t.Errorf("OK=false; inserida=%v apagadas=%d total_antes=%d total_depois=%d",
			res.Inserted, res.RowsDeleted, res.TotalBefore, res.TotalAfter)
	}
	if res.RowsDeleted != 1 {
		t.Errorf("RowsDeleted=%d; want 1", res.RowsDeleted)
	}
	if len(fb.rows) != 0 {
		t.Errorf("a isca deveria ter sumido, restaram %d linhas", len(fb.rows))
	}
	if res.Chave != selfTestChave {
		t.Errorf("chave=%q; want a isca sintética", res.Chave)
	}
}

// Devolução (tpNF=0): a empresa EMITE a nota mas é ENTRADA de mercadoria — o
// DownloadXML arquiva em ENTRADA. Sem ler o tpNF, o syncer classificava como
// saída (bug achado no check-plans: 385 divergências SAIDA×ENTRADA).
func TestPlanFile_DevolucaoTpNF(t *testing.T) {
	fb := newFake()
	s, arrival, _ := newSyncer(t, fb, true)
	xml := `<?xml version="1.0"?><nfeProc><NFe><infNFe Id="NFe` + chaveTeste + `">` +
		`<ide><mod>55</mod><tpNF>0</tpNF><dhEmi>2026-07-01T10:00:00-03:00</dhEmi></ide>` +
		`<emit><CNPJ>` + cnpjA + `</CNPJ><xNome>EMISSOR TESTE</xNome></emit>` +
		`<dest><CNPJ>` + cnpjB + `</CNPJ><xNome>DESTINO TESTE</xNome></dest>` +
		`<total><ICMSTot><vNF>123.45</vNF></ICMSTot></total>` +
		`</infNFe></NFe></nfeProc>`
	origem := writeXML(t, arrival, "devol.xml", xml)
	if err := s.refreshResolve(context.Background()); err != nil {
		t.Fatal(err)
	}
	plan := s.PlanFile(context.Background(), origem, true)
	if plan.Skip != "" {
		t.Fatalf("plan.Skip = %q", plan.Skip)
	}
	var emitPart *Participacao
	for i := range plan.Participacoes {
		if plan.Participacoes[i].CodigoEmpresa == 100 { // empresa A = EMITENTE
			emitPart = &plan.Participacoes[i]
		}
	}
	if emitPart == nil {
		t.Fatalf("participação do emitente (emp 100) ausente: %+v", plan.Participacoes)
	}
	if emitPart.Direction != model.DirEntrada {
		t.Errorf("devolução: emitente Direction = %q; want entrada", emitPart.Direction)
	}
	if !strings.Contains(emitPart.DestRel, `\ENTRADA\`) {
		t.Errorf("devolução: DestRel deveria conter \\ENTRADA\\: %s", emitPart.DestRel)
	}
}

func TestPlanFile_Allowlist(t *testing.T) {
	fb := newFake()
	s, arrival, _ := newSyncer(t, fb, false)
	s.cfg.Empresas = map[int]bool{100: true} // 200 fora
	_ = s.refreshResolve(context.Background())
	origem := writeXML(t, arrival, "nota.xml", nfeXML(chaveTeste, cnpjA, cnpjB))

	plan := s.PlanFile(context.Background(), origem, true)
	if !strings.Contains(plan.Skip, "allowlist") {
		t.Errorf("plan.Skip = %q; esperado skip por allowlist", plan.Skip)
	}
	// single-key (enforce=false) ignora a allowlist
	plan = s.PlanFile(context.Background(), origem, false)
	if plan.Skip != "" {
		t.Errorf("single-key não passa pela allowlist; skip = %q", plan.Skip)
	}
}

// O 1º segmento vem do NOME DA FILIAL (TABFILIAL.NOME), não da TABEMPRESAS —
// filiais da mesma empresa têm pastas próprias (confirmado no diff
// plano×realidade, caso JOAO BATISTA emp 369). TABEMPRESAS é só fallback.
func TestPlanFile_NomeDaFilial(t *testing.T) {
	fb := newFake()
	fb.filiais[0].Nome = "FAZENDA CONJUNTO LINDOIA" // nome PRÓPRIO da filial da emp 100
	// filial da emp 200 sem Nome -> fallback p/ TABEMPRESAS ("EMPRESA B & FILHOS")
	s, arrival, _ := newSyncer(t, fb, false)
	_ = s.refreshResolve(context.Background())
	origem := writeXML(t, arrival, "nota.xml", nfeXML(chaveTeste, cnpjA, cnpjB))

	plan := s.PlanFile(context.Background(), origem, true)
	if plan.Skip != "" {
		t.Fatalf("plan.Skip = %q", plan.Skip)
	}
	if !strings.HasPrefix(plan.Participacoes[0].DestRel, `\FAZENDA CONJUNTO LINDOIA\`) {
		t.Errorf("part[0] deveria usar o nome da FILIAL: %s", plan.Participacoes[0].DestRel)
	}
	if !strings.HasPrefix(plan.Participacoes[1].DestRel, `\EMPRESA B & FILHOS\`) {
		t.Errorf("part[1] deveria cair no fallback TABEMPRESAS (& mantido verbatim): %s", plan.Participacoes[1].DestRel)
	}
}

func TestDentroDaJanela(t *testing.T) {
	now := time.Date(2026, 7, 9, 0, 0, 0, 0, time.UTC)
	for emissao, want := range map[string]bool{
		"2026-07-01": true, "2026-06-30": true, "2026-05-31": false, "2026-08-01": false, "": false,
	} {
		if got := dentroDaJanela(emissao, now); got != want {
			t.Errorf("dentroDaJanela(%q) = %v; want %v", emissao, got, want)
		}
	}
}

func TestRelToAbs(t *testing.T) {
	got := relToAbs("/root", `\EMPRESA A\11222333000181\NFe\SAIDA\202607\x.xml`)
	want := filepath.Join("/root", "EMPRESA A", "11222333000181", "NFe", "SAIDA", "202607", "x.xml")
	if got != want {
		t.Errorf("relToAbs = %q; want %q", got, want)
	}
}
