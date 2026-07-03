package agent

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/EnzzoHosaki/rps-xml-tracker/internal/model"
)

// newSyncAgent monta um agente com um root de SYNC (backfill=true p/ não seedar).
func newSyncAgent(t *testing.T, root string, sink *fakeSink, fullEvery time.Duration) *Agent {
	t.Helper()
	a, err := New(Config{
		Name:          "TEST",
		Roots:         []Root{{Path: root, Stage: model.StageSync, Event: model.EventFileMoved}},
		StatePath:     filepath.Join(t.TempDir(), "state.db"),
		StableAge:     time.Nanosecond,
		Backfill:      true,
		SyncFullEvery: fullEvery,
	}, sink)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { a.Close() })
	return a
}

func TestIsPartitionDir(t *testing.T) {
	cases := map[string]bool{
		"202606": true, "202212": true, "202601": true,
		"202613": false, "202600": false, // mês inválido
		"2026":   false, "20260601": false, // tamanho errado
		"ENTRADA": false, "NFe": false, "abc123": false,
	}
	for name, want := range cases {
		if got := isPartitionDir(name); got != want {
			t.Errorf("isPartitionDir(%q) = %v, want %v", name, got, want)
		}
	}
}

// TestScanRoots_PruneSkipsUntouchedPartitions: partição AAAAMM com mtime anterior ao
// cutoff é pulada inteira; partição tocada é varrida — mesmo sendo de mês ANTIGO
// (o caso "nota emitida em 2025 sincronizada hoje": o mtime do diretório denuncia).
func TestScanRoots_PruneSkipsUntouchedPartitions(t *testing.T) {
	root := t.TempDir()
	sink := &fakeSink{}
	a := newSyncAgent(t, root, sink, 24*time.Hour)

	old := time.Now().Add(-48 * time.Hour)
	// duas partições de meses antigos: uma intocada (mtime velho), outra que acabou
	// de receber arquivo (mtime recente — os.Create bumpa o mtime do diretório).
	write(t, root, filepath.Join("1203-1 EMP", "NFe", "ENTRADA", "202501", "untouched.xml"), nfeXML)
	backdate(t, root, filepath.Join("1203-1 EMP", "NFe", "ENTRADA", "202501"), old)
	write(t, root, filepath.Join("1203-1 EMP", "NFe", "ENTRADA", "202502", "fresh.xml"), nfeXML)

	cutoff := time.Now().Add(-1 * time.Hour)
	res, err := a.scanRoots(context.Background(), "sync", a.cfg.Roots, &cutoff)
	if err != nil {
		t.Fatal(err)
	}
	if res.PrunedDirs != 1 {
		t.Errorf("PrunedDirs = %d, want 1 (só a 202501 intocada)", res.PrunedDirs)
	}
	if res.Scanned != 1 || res.Emitted != 1 {
		t.Errorf("Scanned=%d Emitted=%d, want 1/1 (só fresh.xml da 202502)", res.Scanned, res.Emitted)
	}
	if res.FullScan {
		t.Error("FullScan deveria ser false com cutoff ativo")
	}

	// varredura completa (cutoff nil) alcança a partição podada.
	res, err = a.scanRoots(context.Background(), "sync", a.cfg.Roots, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !res.FullScan || res.PrunedDirs != 0 {
		t.Errorf("completa: FullScan=%v PrunedDirs=%d, want true/0", res.FullScan, res.PrunedDirs)
	}
	if res.Emitted != 1 { // untouched.xml agora emite (fresh.xml já é seen)
		t.Errorf("completa: Emitted=%d, want 1 (untouched.xml)", res.Emitted)
	}
}

// TestScanSync_FullEveryBookkeeping: 1ª varredura é completa (sem meta); as seguintes
// podam; passado SyncFullEvery, volta a ser completa.
func TestScanSync_FullEveryBookkeeping(t *testing.T) {
	root := t.TempDir()
	sink := &fakeSink{}
	a := newSyncAgent(t, root, sink, time.Hour)

	// relógio injetável p/ avançar o tempo sem dormir.
	now := time.Now()
	a.now = func() time.Time { return now }

	write(t, root, filepath.Join("1203-1 EMP", "NFe", "ENTRADA", "202506", "a.xml"), nfeXML)

	res, err := a.scanSync(context.Background(), a.cfg.Roots)
	if err != nil {
		t.Fatal(err)
	}
	if !res.FullScan {
		t.Fatal("1ª varredura deveria ser completa (nenhuma completa registrada)")
	}

	now = now.Add(10 * time.Minute)
	res, err = a.scanSync(context.Background(), a.cfg.Roots)
	if err != nil {
		t.Fatal(err)
	}
	if res.FullScan {
		t.Fatal("2ª varredura (10min depois) deveria ser PODADA")
	}

	now = now.Add(2 * time.Hour) // passa do SyncFullEvery=1h
	res, err = a.scanSync(context.Background(), a.cfg.Roots)
	if err != nil {
		t.Fatal(err)
	}
	if !res.FullScan {
		t.Fatal("após SyncFullEvery, a varredura deveria voltar a ser completa")
	}
}

// TestScanSync_PruneDisabled: SyncFullEvery=0 -> toda varredura completa (comportamento antigo).
func TestScanSync_PruneDisabled(t *testing.T) {
	root := t.TempDir()
	a := newSyncAgent(t, root, &fakeSink{}, 0)
	write(t, root, filepath.Join("1203-1 EMP", "NFe", "ENTRADA", "202506", "a.xml"), nfeXML)

	for i := 0; i < 2; i++ {
		res, err := a.scanSync(context.Background(), a.cfg.Roots)
		if err != nil {
			t.Fatal(err)
		}
		if !res.FullScan {
			t.Fatalf("varredura %d: com poda desligada toda varredura deve ser completa", i+1)
		}
	}
}

// TestScanSync_CatchesLateFileInOldPartition: fluxo fim-a-fim do caso crítico — depois
// de varreduras podadas, um arquivo cai numa partição ANTIGA (mtime do diretório bumpa)
// e a próxima varredura podada o encontra.
func TestScanSync_CatchesLateFileInOldPartition(t *testing.T) {
	root := t.TempDir()
	sink := &fakeSink{}
	a := newSyncAgent(t, root, sink, 24*time.Hour)

	now := time.Now()
	a.now = func() time.Time { return now }

	oldPart := filepath.Join("1203-1 EMP", "NFe", "ENTRADA", "202401")
	write(t, root, filepath.Join(oldPart, "antiga.xml"), nfeXML)

	// 1ª: completa — vê antiga.xml.
	if _, err := a.scanSync(context.Background(), a.cfg.Roots); err != nil {
		t.Fatal(err)
	}
	// envelhece a partição p/ trás do próximo cutoff.
	backdate(t, root, oldPart, now.Add(-72*time.Hour))

	// 2ª (podada, +10min): partição intocada -> podada, nada novo.
	now = now.Add(10 * time.Minute)
	res, err := a.scanSync(context.Background(), a.cfg.Roots)
	if err != nil {
		t.Fatal(err)
	}
	if res.PrunedDirs != 1 || res.Emitted != 0 {
		t.Fatalf("2ª: PrunedDirs=%d Emitted=%d, want 1/0", res.PrunedDirs, res.Emitted)
	}

	// nota emitida em 2024 sincroniza AGORA: cai na partição antiga e o mtime do
	// diretório bumpa. (No teste o relógio é fake, então alinhamos o mtime do
	// diretório ao "agora" fake — em produção relógio e filesystem são a mesma
	// máquina e o bump é automático.)
	now = now.Add(10 * time.Minute)
	write(t, root, filepath.Join(oldPart, "tardia.xml"), nfeXML)
	backdate(t, root, oldPart, now)

	// 3ª (podada): o mtime novo do diretório fura a poda e a tardia é emitida.
	res, err = a.scanSync(context.Background(), a.cfg.Roots)
	if err != nil {
		t.Fatal(err)
	}
	if res.FullScan {
		t.Fatal("3ª deveria ser podada (dentro do SyncFullEvery)")
	}
	if res.Emitted != 1 {
		t.Fatalf("3ª: Emitted=%d, want 1 (tardia.xml na partição antiga tocada)", res.Emitted)
	}
	if res.PrunedDirs != 0 {
		t.Fatalf("3ª: PrunedDirs=%d, want 0 (a partição foi tocada)", res.PrunedDirs)
	}
}
