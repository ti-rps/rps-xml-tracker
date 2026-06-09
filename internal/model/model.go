// Package model defines the core domain types shared across the tracker.
package model

import "time"

// DocType is the authoritative document type, derived from the agent's XML parse
// (Firebird's TIPODOCUMENTO is unreliable — see Fase 0 findings).
type DocType string

const (
	DocNFe     DocType = "NFE"
	DocNFCe    DocType = "NFCE"
	DocCTe     DocType = "CTE"
	DocNFS     DocType = "NFS"
	DocEvento  DocType = "EVENTO"
	DocUnknown DocType = "UNKNOWN"
)

// Stage is one of the three pipeline steps a nota flows through.
type Stage string

const (
	StageArrival Stage = "arrival" // chegada em XML_ASINCRONIZAR
	StageSync    Stage = "sync"    // movido para XML_SINCRONIZADO
	StageImport  Stage = "import"  // importado no Athenas (Firebird)
)

// NotaStatus is the derived lifecycle state of a nota.
type NotaStatus string

const (
	StatusArrived       NotaStatus = "arrived"
	StatusSynced        NotaStatus = "synced"
	StatusImported      NotaStatus = "imported"        // terminal (sucesso)
	StatusImportIgnored NotaStatus = "import_ignored"  // terminal (esperado por config)
	StatusPendingImport NotaStatus = "pending_import"  // conhecido no Firebird, aguardando
	StatusStuck         NotaStatus = "stuck"           // passou do SLA (fase de alertas)
	StatusLost          NotaStatus = "lost"            // sumiu antes de importar (fase de alertas)
)

// Common event_type values carried by an Observation.
const (
	EventFileSeen      = "file_seen"      // arquivo apareceu na chegada
	EventFileMoved     = "file_moved"     // apareceu em SINCRONIZADO
	EventImported      = "imported"       // IMPORTADO 0->1 detectado
	EventImportIgnored = "import_ignored" // IMPORTACAOIGNORADA=1
)

// Observation is one immutable, append-only signal about a nota, from any source.
// It is the source of truth; Nota state is derived from a chave's observations.
type Observation struct {
	ID            int64          `json:"id,omitempty"`
	ChaveAcesso   string         `json:"chave_acesso"`
	Stage         Stage          `json:"stage"`
	EventType     string         `json:"event_type"`
	ObservedAt    time.Time      `json:"observed_at"`
	IngestedAt    time.Time      `json:"ingested_at,omitempty"`
	Source        string         `json:"source"`
	DocType       DocType        `json:"doc_type"`
	FilePath      string         `json:"file_path,omitempty"`
	FileHash      string         `json:"file_hash,omitempty"`
	CodigoEmpresa *int           `json:"codigo_empresa,omitempty"`
	CodigoFilial  *int           `json:"codigo_filial,omitempty"`
	Payload       map[string]any `json:"payload,omitempty"`
}

// Nota is the derived state for a single chave (NFe/NFCe/CTe).
type Nota struct {
	ChaveAcesso    string     `json:"chave_acesso"`
	DocType        DocType    `json:"doc_type"`
	Status         NotaStatus `json:"status"`
	CodigoEmpresa  *int       `json:"codigo_empresa,omitempty"`
	CodigoFilial   *int       `json:"codigo_filial,omitempty"`
	ArrivedAt      *time.Time `json:"arrived_at,omitempty"`
	SyncedAt       *time.Time `json:"synced_at,omitempty"`
	ImportedAt     *time.Time `json:"imported_at,omitempty"`
	ImportIgnored  bool       `json:"import_ignored"`
	MotivoIgnorado string     `json:"motivo_ignorado,omitempty"`
	FirstSeenAt    time.Time  `json:"first_seen_at"`
	LastUpdateAt   time.Time  `json:"last_update_at"`
	// latências derivadas (segundos), nil quando os spans não existem
	LatArrivalSyncS *int64 `json:"lat_arrival_sync_s,omitempty"`
	LatSyncImportS  *int64 `json:"lat_sync_import_s,omitempty"`
}

// NotaDetail adds the full span timeline to a Nota.
type NotaDetail struct {
	Nota
	Spans []Observation `json:"spans"`
}

// StatusCounts holds per-status totals (shared by overview and per-empresa).
type StatusCounts struct {
	Arrived       int `json:"arrived"`
	Synced        int `json:"synced"`
	Imported      int `json:"imported"`
	ImportIgnored int `json:"import_ignored"`
	PendingImport int `json:"pending_import"`
	Stuck         int `json:"stuck"`
	Lost          int `json:"lost"`
}

// Overview is the dashboard's summary cards.
type Overview struct {
	StatusCounts
	InTransit     int `json:"in_transit"`      // arrived + synced
	ImportedToday int `json:"imported_today"`
	// latências (segundos); nil quando não há amostra
	LatArrivalSyncP50S *int64 `json:"lat_arrival_sync_p50_s,omitempty"`
	LatArrivalSyncP95S *int64 `json:"lat_arrival_sync_p95_s,omitempty"`
	LatSyncImportP50S  *int64 `json:"lat_sync_import_p50_s,omitempty"`
	LatSyncImportP95S  *int64 `json:"lat_sync_import_p95_s,omitempty"`
}

// EmpresaAgg is the per-empresa status breakdown (quem está pendente).
type EmpresaAgg struct {
	CodigoEmpresa *int `json:"codigo_empresa,omitempty"`
	CodigoFilial  *int `json:"codigo_filial,omitempty"`
	StatusCounts
}

// NfseImport is one NFSe import-side record (lado Firebird; sem etapa de chegada).
type NfseImport struct {
	AthenasChave   string     `json:"athenas_chave"`
	CodigoEmpresa  *int       `json:"codigo_empresa,omitempty"`
	Status         NotaStatus `json:"status"`
	MotivoIgnorado string     `json:"motivo_ignorado,omitempty"`
	DataEmissao    *string    `json:"data_emissao,omitempty"`
}
