// Package agent watches the XML folders on SRVIMPORT (READ-ONLY), parses new
// files to extract the chave, and submits observation batches to the tracker.
//
// Discovery is by incremental scan (robust against the third-party mover and
// antivirus; fsnotify can augment later). A bbolt state file records which files
// were already processed (path -> size+modtime), so each scan parses only new or
// changed files — and parsing (the expensive, AV-bound step) never touches the
// backlog twice. With Backfill=false the first scan SEEDS the existing backlog as
// "seen" without emitting, so the agent does not hammer the ~23M-file history.
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
}

// submitter is the ingest capability (interface for tests).
type submitter interface {
	Submit(ctx context.Context, batch []model.Observation) error
	FlushSpool(ctx context.Context) (int, error)
}

// Agent holds the scan state.
type Agent struct {
	cfg  Config
	sink submitter
	db   *bolt.DB
	now  func() time.Time
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
}

const (
	flushSeenEvery = 2000 // grava o estado bbolt em lotes (não 1 transação/arquivo)
	progressEvery  = 5000 // loga progresso a cada N arquivos escaneados
)

// ScanOnce flushes any spooled batches, then scans all roots once.
func (a *Agent) ScanOnce(ctx context.Context) (Result, error) {
	var res Result
	if _, err := a.sink.FlushSpool(ctx); err != nil {
		// non-fatal: keep scanning; spool will retry next cycle
	}

	seeding := !a.initialized() && !a.cfg.Backfill
	res.Seeded = seeding

	batch := make([]model.Observation, 0, a.cfg.BatchSize)
	pending := make(map[string]fileState, flushSeenEvery) // estado a gravar em lote

	flushObs := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := a.sink.Submit(ctx, batch); err != nil {
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

	for _, root := range a.cfg.Roots {
		err := filepath.WalkDir(root.Path, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return nil // skip unreadable entries
			}
			if d.IsDir() || !strings.EqualFold(filepath.Ext(path), ".xml") {
				return nil
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			res.Scanned++
			if res.Scanned%progressEvery == 0 {
				log.Printf("scan em progresso: escaneados=%d novos=%d emitidos=%d sem_chave=%d",
					res.Scanned, res.New, res.Emitted, res.SkippedNoChave)
			}

			info, ierr := d.Info()
			if ierr != nil {
				return nil
			}
			// stable check: don't read a file that may still be being written
			if a.now().Sub(info.ModTime()) < a.cfg.StableAge {
				return nil
			}
			if a.alreadySeen(path, info) {
				return nil
			}
			res.New++

			if seeding {
				return record(path, info, "")
			}

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
	if seeding {
		a.setInitialized()
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
	obs := model.Observation{
		ChaveAcesso:   pr.Chave,
		Stage:         root.Stage,
		EventType:     root.Event,
		ObservedAt:    info.ModTime(),
		Source:        "agent:" + a.cfg.Name,
		DocType:       pr.DocType,
		FilePath:      path,
		FileHash:      hash,
		CodigoEmpresa: emp,
		CodigoFilial:  fil,
	}
	return obs, pr.Chave, true
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

func (a *Agent) initialized() bool {
	var ok bool
	_ = a.db.View(func(tx *bolt.Tx) error {
		ok = tx.Bucket(metaBucket).Get([]byte("initialized")) != nil
		return nil
	})
	return ok
}

func (a *Agent) setInitialized() {
	_ = a.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(metaBucket).Put([]byte("initialized"), []byte("1"))
	})
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
