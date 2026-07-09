package syncer

import (
	"encoding/json"
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"
)

// Estados de uma participação no journal. A máquina só anda para frente:
// planned → moved → inserted; o "done" é da CHAVE (origem removida), não da
// participação. No restart, o Execute re-deriva o ponto de retomada pelos
// pre-checks (destino existe? linha existe?) — o journal é o registro de
// intenção/auditoria que dirige e confirma essa retomada.
const (
	statePlanned  = "planned"
	stateMoved    = "moved"
	stateInserted = "inserted"
)

var (
	bucketParts = []byte("participacoes") // chave|emp/fil -> partState JSON
	bucketDone  = []byte("done")          // chave -> RFC3339 (origem removida)
	bucketDry   = []byte("dry_planned")   // chave -> RFC3339 (já planejada no dry-run; não re-planejar)
)

// partState é o registro journalizado de uma participação.
type partState struct {
	State     string    `json:"state"`
	Origem    string    `json:"origem"`
	DestRel   string    `json:"dest_rel"`
	DestAbs   string    `json:"dest_abs"`
	InsertID  int64     `json:"insert_id,omitempty"`
	UpdatedAt time.Time `json:"updated_at"`
}

// journal é o estado local durável do syncer (bbolt, como o agent-state).
type journal struct {
	db *bolt.DB
}

func openJournal(path string) (*journal, error) {
	db, err := bolt.Open(path, 0o644, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		return nil, err
	}
	err = db.Update(func(tx *bolt.Tx) error {
		for _, b := range [][]byte{bucketParts, bucketDone, bucketDry} {
			if _, err := tx.CreateBucketIfNotExists(b); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		db.Close()
		return nil, err
	}
	return &journal{db: db}, nil
}

func (j *journal) Close() error { return j.db.Close() }

func partKey(chave string, emp, fil int) []byte {
	return []byte(fmt.Sprintf("%s|%d/%d", chave, emp, fil))
}

// setPart grava a transição de estado da participação.
func (j *journal) setPart(chave string, emp, fil int, st partState) error {
	st.UpdatedAt = time.Now()
	b, err := json.Marshal(st)
	if err != nil {
		return err
	}
	return j.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketParts).Put(partKey(chave, emp, fil), b)
	})
}

func (j *journal) getPart(chave string, emp, fil int) (partState, bool) {
	var st partState
	found := false
	_ = j.db.View(func(tx *bolt.Tx) error {
		if v := tx.Bucket(bucketParts).Get(partKey(chave, emp, fil)); v != nil {
			found = json.Unmarshal(v, &st) == nil
		}
		return nil
	})
	return st, found
}

// markDone registra que a chave completou (origem removida).
func (j *journal) markDone(chave string) error {
	return j.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketDone).Put([]byte(chave), []byte(time.Now().Format(time.RFC3339)))
	})
}

func (j *journal) isDone(chave string) bool {
	done := false
	_ = j.db.View(func(tx *bolt.Tx) error {
		done = tx.Bucket(bucketDone).Get([]byte(chave)) != nil
		return nil
	})
	return done
}

// markDryPlanned/isDryPlanned: no dry-run os arquivos não saem do lugar — sem
// esta marca, cada ciclo re-planejaria os MESMOS primeiros N arquivos para
// sempre e o dry-run nunca cobriria o resto da pasta.
func (j *journal) markDryPlanned(chave string) error {
	return j.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketDry).Put([]byte(chave), []byte(time.Now().Format(time.RFC3339)))
	})
}

func (j *journal) isDryPlanned(chave string) bool {
	planned := false
	_ = j.db.View(func(tx *bolt.Tx) error {
		planned = tx.Bucket(bucketDry).Get([]byte(chave)) != nil
		return nil
	})
	return planned
}
