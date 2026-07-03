// Package agent watches the XML folders on SRVIMPORT (READ-ONLY), parses new
// files to extract the chave, and submits observation batches to the tracker.
//
// Discovery is by incremental scan (robust against the third-party mover and
// antivirus; fsnotify can augment later). A bbolt state file records which files
// were already processed (path -> size+modtime), so each scan parses only new or
// changed files — and parsing (the expensive, AV-bound step) never touches the
// backlog twice. With Backfill=false the first run records a cutoff timestamp
// (persisted up front) and skips everything older as backlog without emitting, so
// the agent ignores the ~23M-file history. Because the cutoff is durable, an
// interrupted first scan never re-seeds: the next run already has the cutoff and
// emits new files normally.
package agent

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/EnzzoHosaki/rps-xml-tracker/internal/model"
	"github.com/EnzzoHosaki/rps-xml-tracker/internal/xmlparse"
)

// Root is a watched folder and the stage/event its files represent.
type Root struct {
	Path  string
	Stage model.Stage
	Event string
}

// Config configures the agent.
type Config struct {
	Name      string        // agent name (e.g. SRVIMPORT) -> observation source
	Roots     []Root        // arrival + sync folders
	StatePath string        // bbolt file path
	BatchSize int           // observations per submit (default 500)
	StableAge time.Duration // a file must be older than this before parsing (default 5s)
	Backfill  bool          // false: first scan seeds backlog without emitting
	// SyncFullEvery ativa a PODA POR RECÊNCIA na varredura do SINCRONIZADO: as
	// varreduras entre completas PULAM as partições AAAAMM cujo mtime de diretório é
	// anterior ao início da última varredura bem-sucedida (no NTFS, criar/mover um
	// arquivo dentro de uma pasta atualiza o mtime DELA — partição intocada = nada
	// novo dentro). Isso derruba a passada de O(21M arquivos) para O(diretórios +
	// arquivos das partições quentes), sem perder notas antigas sincronizadas hoje
	// (a partição AAAAMM é por mês de EMISSÃO; quando uma nota velha sincroniza, o
	// mtime da partição antiga é atualizado e ela é visitada). Uma varredura COMPLETA
	// roda a cada SyncFullEvery como rede de segurança. 0 = poda desligada (toda
	// varredura é completa — comportamento antigo).
	SyncFullEvery time.Duration
}

// submitter is the ingest capability (interface for tests).
type submitter interface {
	Submit(ctx context.Context, batch []model.Observation) error
	FlushSpool(ctx context.Context) (int, error)
}

// Agent holds the scan state.
type Agent struct {
	cfg    Config
	sink   submitter
	db     *bolt.DB
	now    func() time.Time
	sinkMu sync.Mutex // serializa Submit/FlushSpool entre os loops concorrentes (chegada/sync)
}

// submitSafe e flushSpoolSafe serializam o acesso ao sink entre os dois loops de
// scan. O WalkDir (parte lenta) fica FORA do lock; só a chamada de rede é serializada.
func (a *Agent) submitSafe(ctx context.Context, batch []model.Observation) error {
	a.sinkMu.Lock()
	defer a.sinkMu.Unlock()
	return a.sink.Submit(ctx, batch)
}

func (a *Agent) flushSpoolSafe(ctx context.Context) {
	a.sinkMu.Lock()
	defer a.sinkMu.Unlock()
	_, _ = a.sink.FlushSpool(ctx)
}

var seenBucket = []byte("seen")
var metaBucket = []byte("meta")

type fileState struct {
	Size    int64  `json:"s"`
	ModUnix int64  `json:"m"`
	Chave   string `json:"k,omitempty"`
}

func New(cfg Config, sink submitter) (*Agent, error) {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 500
	}
	if cfg.StableAge <= 0 {
		cfg.StableAge = 5 * time.Second
	}
	db, err := bolt.Open(cfg.StatePath, 0o644, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		return nil, err
	}
	err = db.Update(func(tx *bolt.Tx) error {
		if _, e := tx.CreateBucketIfNotExists(seenBucket); e != nil {
			return e
		}
		_, e := tx.CreateBucketIfNotExists(metaBucket)
		return e
	})
	if err != nil {
		db.Close()
		return nil, err
	}
	return &Agent{cfg: cfg, sink: sink, db: db, now: time.Now}, nil
}

func (a *Agent) Close() error { return a.db.Close() }

// Result summarizes one scan cycle.
type Result struct {
	Scanned        int
	New            int
	Emitted        int
	SkippedNoChave int
	Seeded         bool
	PrunedDirs     int  // partições AAAAMM puladas por estarem intocadas (poda por recência)
	FullScan       bool // true quando a varredura foi completa (sem poda)
}

const (
	flushSeenEvery     = 2000             // grava o estado bbolt em lotes (não 1 transação/arquivo)
	progressEvery      = 5000             // loga progresso a cada N arquivos escaneados
	spoolFlushInterval = 90 * time.Second // reenvia o spool no próprio ticker (não só no início da varredura)
	// pruneSafetyMargin é subtraído do cutoff da poda p/ absorver jitter de relógio
	// entre o processo e os timestamps do filesystem (mesma máquina — folga generosa).
	pruneSafetyMargin = 2 * time.Minute
)

// ScanOnce flushes any spooled batches, then scans ALL roots once (arrival+sync
// sequentially). Mantido para testes e para o caminho simples; em produção o agente
// usa RunSplit, que escaneia chegada e sync em loops independentes (a varredura
// gigante do sync não pode atrasar a detecção da chegada).
func (a *Agent) ScanOnce(ctx context.Context) (Result, error) {
	return a.scanRoots(ctx, "all", a.cfg.Roots, nil)
}

// scanRoots flushes any spooled batches, then walks the given roots once.
// pruneCutoff != nil ativa a poda por recência: partições AAAAMM com mtime de
// diretório anterior ao cutoff são puladas inteiras (nada foi criado/movido nelas
// desde a última varredura — ver Config.SyncFullEvery).
func (a *Agent) scanRoots(ctx context.Context, label string, roots []Root, pruneCutoff *time.Time) (Result, error) {
	var res Result
	res.FullScan = pruneCutoff == nil
	a.flushSpoolSafe(ctx) // non-fatal: spool retries next cycle

	// Seeding by cutoff timestamp: on the very first run (no cutoff stored yet,
	// and not a backfill) record "now" as the backlog cutoff and persist it
	// immediately — before walking. Files older than the cutoff are backlog and
	// skipped without emitting; anything at or after it is a genuine arrival.
	// Persisting up front means an interrupted scan (Ctrl-C, reboot, AV stall)
	// never re-seeds: the next run already has the cutoff and emits normally.
	cutoff, hasCutoff := a.seedCutoff()
	firstSeed := !hasCutoff && !a.cfg.Backfill
	if firstSeed {
		cutoff = a.now()
		if err := a.setSeedCutoff(cutoff); err != nil {
			return res, err
		}
	}
	res.Seeded = firstSeed

	batch := make([]model.Observation, 0, a.cfg.BatchSize)
	pending := make(map[string]fileState, flushSeenEvery) // estado a gravar em lote

	flushObs := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := a.submitSafe(ctx, batch); err != nil {
			return err
		}
		batch = batch[:0]
		return nil
	}
	flushState := func() error {
		if len(pending) == 0 {
			return nil
		}
		err := a.db.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket(seenBucket)
			for p, st := range pending {
				v, _ := json.Marshal(st)
				if e := b.Put([]byte(p), v); e != nil {
					return e
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
		for k := range pending {
			delete(pending, k)
		}
		return nil
	}
	record := func(path string, info fs.FileInfo, chave string) error {
		pending[path] = fileState{Size: info.Size(), ModUnix: info.ModTime().Unix(), Chave: chave}
		if len(pending) >= flushSeenEvery {
			return flushState()
		}
		return nil
	}

	for _, root := range roots {
		err := filepath.WalkDir(root.Path, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return nil // skip unreadable entries
			}
			if d.IsDir() {
				// Poda por recência: partição AAAAMM intocada desde o cutoff -> pula a
				// subárvore. Só em diretórios-partição (nome AAAAMM), nunca na raiz;
				// qualquer outro nível é sempre descido (a poda é conservadora).
				if pruneCutoff != nil && path != root.Path && isPartitionDir(d.Name()) {
					if info, ierr := d.Info(); ierr == nil && info.ModTime().Before(*pruneCutoff) {
						res.PrunedDirs++
						return filepath.SkipDir
					}
				}
				return nil
			}
			if !strings.EqualFold(filepath.Ext(path), ".xml") {
				return nil
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			res.Scanned++
			if res.Scanned%progressEvery == 0 {
				log.Printf("scan[%s] em progresso: escaneados=%d novos=%d emitidos=%d sem_chave=%d",
					label, res.Scanned, res.New, res.Emitted, res.SkippedNoChave)
			}

			info, ierr := d.Info()
			if ierr != nil {
				return nil
			}
			// stable check: don't read a file that may still be being written
			if a.now().Sub(info.ModTime()) < a.cfg.StableAge {
				return nil
			}
			// backlog: older than the seed cutoff -> skip without emitting.
			if !a.cfg.Backfill && info.ModTime().Before(cutoff) {
				return nil
			}
			if a.alreadySeen(path, info) {
				return nil
			}
			res.New++

			obs, chave, ok := a.parseToObservation(root, path, info)
			if err := record(path, info, chave); err != nil {
				return err
			}
			if !ok {
				res.SkippedNoChave++
				return nil
			}
			batch = append(batch, obs)
			res.Emitted++
			if len(batch) >= a.cfg.BatchSize {
				return flushObs()
			}
			return nil
		})
		if err != nil {
			return res, err
		}
	}
	if err := flushObs(); err != nil {
		return res, err
	}
	if err := flushState(); err != nil {
		return res, err
	}
	return res, nil
}

// parseToObservation reads the file (one open), hashes it, parses the chave, and
// builds an arrival/sync observation. ok=false when there is no 44-digit chave
// (e.g. NFSe), in which case the file is recorded as seen and skipped.
func (a *Agent) parseToObservation(root Root, path string, info fs.FileInfo) (model.Observation, string, bool) {
	data, err := os.ReadFile(path) // read-only
	if err != nil {
		return model.Observation{}, "", false
	}
	sum := md5.Sum(data)
	hash := hex.EncodeToString(sum[:])
	pr, err := xmlparse.Parse(strings.NewReader(string(data)))
	if err != nil || pr.Chave == "" {
		return model.Observation{}, "", false
	}
	emp, fil := empresaFromPath(root.Path, path)
	// Arrival time = file mtime (≈ quando o XML foi escrito na chegada). Na etapa
	// sync o mover preserva o mtime original ao mover para SINCRONIZADO, então o
	// mtime NÃO diz quando sincronizou — usamos a hora de detecção do agente
	// (now), dando um instante de sync real (com precisão de ±intervalo de scan).
	observedAt := info.ModTime()
	if root.Stage == model.StageSync {
		observedAt = a.now()
	}
	obs := model.Observation{
		ChaveAcesso:      pr.Chave,
		Stage:            root.Stage,
		EventType:        root.Event,
		ObservedAt:       observedAt,
		Source:           "agent:" + a.cfg.Name,
		DocType:          pr.DocType,
		FilePath:         path,
		FileHash:         hash,
		CodigoEmpresa:    emp,
		CodigoFilial:     fil,
		CnpjEmitente:     pr.CnpjEmitente,
		NomeEmitente:     pr.NomeEmitente,
		CnpjDestinatario: pr.CnpjDestinatario,
		NomeDestinatario: pr.NomeDestinatario,
		DataEmissao:      pr.DataEmissao,
		ValorTotal:       parseValor(pr.ValorTotal),
	}
	return obs, pr.Chave, true
}

// parseValor converte o vNF do XML ("1234.56") em *float64; nil se vazio/inválido.
func parseValor(s string) *float64 {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return nil
	}
	return &v
}

// empresaFromPath reads the top folder under root: "1203-1 NOME" -> (1203, 1).
func empresaFromPath(root, full string) (*int, *int) {
	rel, err := filepath.Rel(root, full)
	if err != nil {
		return nil, nil
	}
	parts := strings.Split(filepath.ToSlash(rel), "/")
	if len(parts) == 0 {
		return nil, nil
	}
	top := parts[0]
	// leading token like "1203-1" or "1203-1 NOME"
	field := strings.Fields(top)
	if len(field) == 0 {
		return nil, nil
	}
	code := field[0]
	emp, fil := code, "1"
	if i := strings.IndexByte(code, '-'); i > 0 {
		emp, fil = code[:i], code[i+1:]
	}
	e, err1 := strconv.Atoi(emp)
	if err1 != nil {
		return nil, nil
	}
	f, err2 := strconv.Atoi(fil)
	if err2 != nil {
		f = 1
	}
	return &e, &f
}

// ---- bbolt state ----

func (a *Agent) alreadySeen(path string, info fs.FileInfo) bool {
	var seen bool
	_ = a.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(seenBucket).Get([]byte(path))
		if v == nil {
			return nil
		}
		var st fileState
		if json.Unmarshal(v, &st) == nil {
			seen = st.Size == info.Size() && st.ModUnix == info.ModTime().Unix()
		}
		return nil
	})
	return seen
}

var (
	seedCutoffKey   = []byte("seed_cutoff_unixnano")
	syncLastScanKey = []byte("sync_last_scan_start_unixnano") // início da última varredura de sync BEM-SUCEDIDA (podada ou completa)
	syncLastFullKey = []byte("sync_last_full_start_unixnano") // início da última varredura de sync COMPLETA bem-sucedida
)

// getMetaTime lê um timestamp persistido do bucket meta.
func (a *Agent) getMetaTime(key []byte) (time.Time, bool) {
	var t time.Time
	var ok bool
	_ = a.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(metaBucket).Get(key)
		if v == nil {
			return nil
		}
		if n, err := strconv.ParseInt(string(v), 10, 64); err == nil {
			t, ok = time.Unix(0, n), true
		}
		return nil
	})
	return t, ok
}

// setMetaTime persiste um timestamp no bucket meta.
func (a *Agent) setMetaTime(key []byte, t time.Time) error {
	return a.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(metaBucket).Put(key, []byte(strconv.FormatInt(t.UnixNano(), 10)))
	})
}

// seedCutoff returns the persisted backlog cutoff and whether one was stored.
func (a *Agent) seedCutoff() (time.Time, bool) { return a.getMetaTime(seedCutoffKey) }

// setSeedCutoff persists the backlog cutoff so the seed survives a restart.
func (a *Agent) setSeedCutoff(t time.Time) error { return a.setMetaTime(seedCutoffKey, t) }

// isPartitionDir reports whether name looks like an AAAAMM partition folder
// (ex.: 202606) — o nível folha do SINCRONIZADO que contém os XMLs diretamente.
// Exige mês 01..12; qualquer outro nome é descido normalmente (poda conservadora).
func isPartitionDir(name string) bool {
	if len(name) != 6 {
		return false
	}
	for i := 0; i < 6; i++ {
		if name[i] < '0' || name[i] > '9' {
			return false
		}
	}
	mm := int(name[4]-'0')*10 + int(name[5]-'0')
	return mm >= 1 && mm <= 12
}

// syncPruneCutoff decide o modo da PRÓXIMA varredura do sync: retorna o cutoff da
// poda, ou nil quando a varredura deve ser COMPLETA (poda desligada, nenhuma completa
// registrada ainda, ou a última completa mais velha que SyncFullEvery). A corretude é
// indutiva: uma partição só é podada se intocada desde o início da última varredura
// bem-sucedida — que por sua vez cobriu tudo que suas antecessoras não podaram,
// ancorado numa varredura completa.
func (a *Agent) syncPruneCutoff() *time.Time {
	if a.cfg.SyncFullEvery <= 0 {
		return nil
	}
	lastFull, ok := a.getMetaTime(syncLastFullKey)
	if !ok || a.now().Sub(lastFull) >= a.cfg.SyncFullEvery {
		return nil
	}
	lastScan, ok := a.getMetaTime(syncLastScanKey)
	if !ok {
		return nil
	}
	c := lastScan.Add(-pruneSafetyMargin)
	return &c
}

// scanSync roda uma varredura do grupo sync (podada ou completa conforme
// syncPruneCutoff) e, SÓ em caso de sucesso, registra o início desta varredura como
// referência da próxima poda (arquivos criados durante a varredura têm mtime >= start
// e serão revisitados; varredura interrompida não registra nada — a próxima recobre).
func (a *Agent) scanSync(ctx context.Context, roots []Root) (Result, error) {
	start := a.now()
	cutoff := a.syncPruneCutoff()
	res, err := a.scanRoots(ctx, "sync", roots, cutoff)
	if err != nil {
		return res, err
	}
	if e := a.setMetaTime(syncLastScanKey, start); e != nil {
		return res, e
	}
	if cutoff == nil {
		if e := a.setMetaTime(syncLastFullKey, start); e != nil {
			return res, e
		}
	}
	return res, nil
}

// Run loops ScanOnce every interval until ctx is cancelled.
func (a *Agent) Run(ctx context.Context, interval time.Duration, onResult func(Result, error)) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		res, err := a.ScanOnce(ctx)
		if onResult != nil {
			onResult(res, err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// RunSplit escaneia chegada e sync em LOOPS INDEPENDENTES, cada um no seu intervalo.
// Motivo: a pasta de chegada esvazia (Download XML recorta p/ SINCRONIZADO), então é
// pequena e deve ser varrida com frequência (detecção em ~1 ciclo); já o SINCRONIZADO
// acumula milhões de arquivos e uma passada leva dias. Num scan único e sequencial, a
// nota recém-chegada ficava refém da varredura gigante do sync. Separados, a chegada
// é detectada em minutos. Os dois loops compartilham bbolt (single-writer safe) e o
// sink (serializado por sinkMu); cada scanRoots tem batch/estado locais.
func (a *Agent) RunSplit(ctx context.Context, arrivalInterval, syncInterval time.Duration, onResult func(group string, res Result, err error)) {
	var arrivalRoots, syncRoots []Root
	for _, r := range a.cfg.Roots {
		if r.Stage == model.StageArrival {
			arrivalRoots = append(arrivalRoots, r)
		} else {
			syncRoots = append(syncRoots, r)
		}
	}

	loop := func(wg *sync.WaitGroup, label string, scan func(context.Context) (Result, error), interval time.Duration) {
		defer wg.Done()
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			res, err := scan(ctx)
			if onResult != nil {
				onResult(label, res, err)
			}
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
		}
	}

	var wg sync.WaitGroup
	if len(arrivalRoots) > 0 {
		wg.Add(1)
		go loop(&wg, "chegada", func(c context.Context) (Result, error) {
			return a.scanRoots(c, "chegada", arrivalRoots, nil) // chegada é fila pequena: sempre completa
		}, arrivalInterval)
	}
	if len(syncRoots) > 0 {
		wg.Add(1)
		go loop(&wg, "sync", func(c context.Context) (Result, error) {
			return a.scanSync(c, syncRoots) // podada por recência; completa a cada SyncFullEvery
		}, syncInterval)
	}

	// Flush do spool no PRÓPRIO ticker (e já na partida): com as varreduras longas, os
	// lotes spoolados não podem esperar o fim do ciclo p/ serem reenviados. Drena assim
	// que o agente sobe e a cada spoolFlushInterval.
	wg.Add(1)
	go func() {
		defer wg.Done()
		t := time.NewTicker(spoolFlushInterval)
		defer t.Stop()
		for {
			a.flushSpoolSafe(ctx)
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
		}
	}()

	wg.Wait()
}
