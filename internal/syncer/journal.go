package syncer

import (
	"bytes"
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
	bucketSweep = []byte("sweep")         // "cursor" -> último path processado (rotação da varredura)
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
		for _, b := range [][]byte{bucketParts, bucketDone, bucketDry, bucketSweep} {
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

// journaledPart é uma participação recuperada do journal para o rollback.
type journaledPart struct {
	Emp, Fil int
	State    partState
}

// partsForChave devolve todas as participações journalizadas de uma chave
// (varre o bucket por prefixo "<chave>|").
func (j *journal) partsForChave(chave string) []journaledPart {
	prefix := []byte(chave + "|")
	var out []journaledPart
	_ = j.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(bucketParts).Cursor()
		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			var st partState
			if json.Unmarshal(v, &st) != nil {
				continue
			}
			var emp, fil int
			if _, err := fmt.Sscanf(string(k[len(prefix):]), "%d/%d", &emp, &fil); err != nil {
				continue
			}
			out = append(out, journaledPart{Emp: emp, Fil: fil, State: st})
		}
		return nil
	})
	return out
}

// clearChave remove os registros locais (participações + done) de uma chave após
// um rollback, para que um re-sync futuro parta do zero.
func (j *journal) clearChave(chave string) error {
	prefix := []byte(chave + "|")
	return j.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketParts)
		var del [][]byte
		c := b.Cursor()
		for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
			del = append(del, append([]byte(nil), k...)) // copiar: k é reusado pelo cursor
		}
		for _, k := range del {
			if err := b.Delete(k); err != nil {
				return err
			}
		}
		return tx.Bucket(bucketDone).Delete([]byte(chave))
	})
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

// sweepCursor é a posição durável da varredura ROTATIVA: o ÚLTIMO arquivo
// examinado (dir da allowlist + caminho relativo à raiz dele, separador '/').
// Vive num bucket próprio ("sweep"), separado dos registros de planos — journals
// antigos não têm o bucket (criado vazio no open) e continuam legíveis.
type sweepCursor struct {
	Dir  string `json:"dir"`  // entrada de cfg.Dirs ("" = raiz da ASINCRONIZAR)
	Path string `json:"path"` // relativo à raiz do Dir, normalizado com '/'
}

// human formata o cursor para logs/heartbeat ("" = início da rotação).
func (c sweepCursor) human() string {
	if c.Path == "" {
		return ""
	}
	if c.Dir == "" {
		return c.Path
	}
	return c.Dir + "/" + c.Path
}

var keySweepCursor = []byte("cursor")

// getSweepCursor lê o cursor; ok=false quando nunca foi gravado (journal novo
// ou anterior ao cursor) — a varredura começa do início.
func (j *journal) getSweepCursor() (sweepCursor, bool) {
	var c sweepCursor
	ok := false
	_ = j.db.View(func(tx *bolt.Tx) error {
		if v := tx.Bucket(bucketSweep).Get(keySweepCursor); v != nil {
			ok = json.Unmarshal(v, &c) == nil && c.Path != ""
		}
		return nil
	})
	return c, ok
}

func (j *journal) setSweepCursor(c sweepCursor) error {
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}
	return j.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketSweep).Put(keySweepCursor, b)
	})
}
