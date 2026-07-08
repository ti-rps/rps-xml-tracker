// Package model defines the core domain types shared across the tracker.
package model

import (
	"strings"
	"time"
)

// NumeroNota extrai o número da nota (tag <nNF> do XML) da chave de acesso: nas 44
// posições da chave NFe/NFCe/CTe, o nNF são os 9 dígitos nas posições 26–34, sem
// zeros à esquerda. Retorna "" se a chave não tiver 44 dígitos (ex.: NFSe).
func NumeroNota(chave string) string {
	if len(chave) != 44 {
		return ""
	}
	return strings.TrimLeft(chave[25:34], "0")
}

// Direção da nota relativa à empresa monitorada: SAÍDA = a empresa (codigo_empresa/
// codigo_filial) é a EMITENTE; ENTRADA = é a DESTINATÁRIA. "" quando indeterminada
// (sem empresa, ou CNPJ da empresa não casa nenhum dos lados).
const (
	DirEntrada = "entrada"
	DirSaida   = "saida"
)

// cnpjRoot8 retorna a raiz (8 primeiros dígitos) de um CNPJ, ignorando não-dígitos. A
// raiz identifica o grupo econômico (compartilhada entre filiais), então casa a empresa
// mesmo quando o documento traz o CNPJ de outra filial do mesmo grupo. "" se < 8 dígitos.
func cnpjRoot8(cnpj string) string {
	var b strings.Builder
	for _, r := range cnpj {
		if r >= '0' && r <= '9' {
			b.WriteRune(r)
			if b.Len() == 8 {
				return b.String()
			}
		}
	}
	return ""
}

// DirectionFromCNPJs decide a direção comparando a raiz do CNPJ da empresa (filial) com
// a do emitente/destinatário. Testa emitente primeiro: intra-grupo (a empresa é emitente
// E destinatária) classifica como "saida". "" se a raiz da empresa não casa nenhum lado.
func DirectionFromCNPJs(empresaCNPJ, cnpjEmitente, cnpjDestinatario string) string {
	r := cnpjRoot8(empresaCNPJ)
	if r == "" {
		return ""
	}
	switch r {
	case cnpjRoot8(cnpjEmitente):
		return DirSaida
	case cnpjRoot8(cnpjDestinatario):
		return DirEntrada
	}
	return ""
}

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
	StatusImported      NotaStatus = "imported"       // terminal (sucesso)
	StatusImportIgnored NotaStatus = "import_ignored" // terminal (esperado por config)
	StatusPendingImport NotaStatus = "pending_import" // conhecido no Firebird, aguardando
	StatusStuck         NotaStatus = "stuck"          // passou do SLA (fase de alertas)
	StatusLost          NotaStatus = "lost"           // sumiu antes de importar (fase de alertas)
)

// Common event_type values carried by an Observation.
const (
	EventFileSeen      = "file_seen"      // arquivo apareceu na chegada
	EventFileMoved     = "file_moved"     // apareceu em SINCRONIZADO
	EventSeenPending   = "seen_pending"   // visto no Athenas, IMPORTADO=0 (aguardando importação)
	EventImported      = "imported"       // IMPORTADO 0->1 detectado
	EventImportIgnored = "import_ignored" // IMPORTACAOIGNORADA=1
)

// Observation is one immutable, append-only signal about a nota, from any source.
// It is the source of truth; Nota state is derived from a chave's observations.
type Observation struct {
	ID            int64     `json:"id,omitempty"`
	ChaveAcesso   string    `json:"chave_acesso"`
	Stage         Stage     `json:"stage"`
	EventType     string    `json:"event_type"`
	ObservedAt    time.Time `json:"observed_at"`
	IngestedAt    time.Time `json:"ingested_at,omitempty"`
	Source        string    `json:"source"`
	DocType       DocType   `json:"doc_type"`
	FilePath      string    `json:"file_path,omitempty"`
	// FilePathRede é o FilePath traduzido para a visão da REDE (R:\..., share
	// \\srvdoc01\REDE) — calculado pela API ao servir, nunca persistido.
	FilePathRede string `json:"file_path_rede,omitempty"`
	FileHash     string `json:"file_hash,omitempty"`
	CodigoEmpresa *int      `json:"codigo_empresa,omitempty"`
	CodigoFilial  *int      `json:"codigo_filial,omitempty"`
	// metadados da nota (do parse do XML ou da linha do Firebird)
	NomeEmpresa      string         `json:"nome_empresa,omitempty"`
	CnpjEmitente     string         `json:"cnpj_emitente,omitempty"`
	NomeEmitente     string         `json:"nome_emitente,omitempty"`
	CnpjDestinatario string         `json:"cnpj_destinatario,omitempty"`
	NomeDestinatario string         `json:"nome_destinatario,omitempty"`
	DataEmissao      string         `json:"data_emissao,omitempty"` // yyyy-mm-dd
	ValorTotal       *float64       `json:"valor_total,omitempty"`
	Direction        string         `json:"direction,omitempty"` // entrada|saida (lado da empresa); "" = indeterminada
	Payload          map[string]any `json:"payload,omitempty"`
}

// Nota is the derived state for a single chave (NFe/NFCe/CTe).
type Nota struct {
	ChaveAcesso      string     `json:"chave_acesso"`
	NumeroNota       string     `json:"numero_nota,omitempty"` // nNF, derivado da chave
	DocType          DocType    `json:"doc_type"`
	Status           NotaStatus `json:"status"`
	CodigoEmpresa    *int       `json:"codigo_empresa,omitempty"`
	CodigoFilial     *int       `json:"codigo_filial,omitempty"`
	NomeEmpresa      string     `json:"nome_empresa,omitempty"`
	CnpjEmitente     string     `json:"cnpj_emitente,omitempty"`
	NomeEmitente     string     `json:"nome_emitente,omitempty"`
	CnpjDestinatario string     `json:"cnpj_destinatario,omitempty"`
	NomeDestinatario string     `json:"nome_destinatario,omitempty"`
	DataEmissao      string     `json:"data_emissao,omitempty"`
	ValorTotal       *float64   `json:"valor_total,omitempty"`
	Direction        string     `json:"direction,omitempty"` // entrada|saida; omitido quando indeterminada
	ArrivedAt        *time.Time `json:"arrived_at,omitempty"`
	SyncedAt         *time.Time `json:"synced_at,omitempty"`
	PendingAt        *time.Time `json:"pending_at,omitempty"` // visto no Athenas aguardando importação
	ImportedAt       *time.Time `json:"imported_at,omitempty"`
	ImportIgnored    bool       `json:"import_ignored"`
	MotivoIgnorado   string     `json:"motivo_ignorado,omitempty"`
	FirstSeenAt      time.Time  `json:"first_seen_at"`
	LastUpdateAt     time.Time  `json:"last_update_at"`
	// latências derivadas (segundos), nil quando os spans não existem
	LatArrivalSyncS *int64 `json:"lat_arrival_sync_s,omitempty"`
	LatSyncImportS  *int64 `json:"lat_sync_import_s,omitempty"`
}

// NotaDetail adds the full span timeline to a Nota.
type NotaDetail struct {
	Nota
	Spans []Observation `json:"spans"`
}

// NotaSummary é o agregado do conjunto que casa um filtro (mesmos filtros do GET /notas):
// quantas notas e a soma de valor_total. Para apuração rápida no painel (ex.: NFC-e do mês).
type NotaSummary struct {
	Count      int     `json:"count"`
	ValorTotal float64 `json:"valor_total"`
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
	// Mode = "flow" quando as contagens foram recomputadas dentro de uma janela de data
	// (notas cujo date_field caiu em [from,to], agrupadas por status atual), em vez do
	// estoque atual global. Omitido (snapshot) sem janela. ImportedToday e as latências
	// seguem globais/30d mesmo no modo flow.
	Mode          string `json:"mode,omitempty"`
	InTransit     int    `json:"in_transit"` // arrived + synced
	ImportedToday int    `json:"imported_today"`
	// latências (segundos); nil quando não há amostra
	LatArrivalSyncP50S *int64 `json:"lat_arrival_sync_p50_s,omitempty"`
	LatArrivalSyncP95S *int64 `json:"lat_arrival_sync_p95_s,omitempty"`
	LatSyncImportP50S  *int64 `json:"lat_sync_import_p50_s,omitempty"`
	LatSyncImportP95S  *int64 `json:"lat_sync_import_p95_s,omitempty"`
}

// TimeseriesBucket is one time bucket (day or week) of pipeline evolution for the
// Painel v2 line charts. Counts são fluxo-por-evento: arrived/synced/imported = notas
// cujo arrived_at/synced_at/imported_at caiu no bucket; import_ignored = notas com
// status atual import_ignored, datadas pelo observed_at do evento de ignore. Latências
// são percentis (segundos) por coorte de evento (chegada->sync chaveada por quem chegou
// no bucket; sync->import por quem sincronizou), nil quando não há amostra.
type TimeseriesBucket struct {
	Date               string `json:"date"` // YYYY-MM-DD (America/Sao_Paulo); semana = segunda-feira
	Arrived            int    `json:"arrived"`
	Synced             int    `json:"synced"`
	Imported           int    `json:"imported"`
	ImportIgnored      int    `json:"import_ignored"`
	LatArrivalSyncP50S *int64 `json:"lat_arrival_sync_p50_s"` // sempre presente (null sem amostra) p/ gap na linha
	LatArrivalSyncP95S *int64 `json:"lat_arrival_sync_p95_s"`
	LatSyncImportP50S  *int64 `json:"lat_sync_import_p50_s"`
	LatSyncImportP95S  *int64 `json:"lat_sync_import_p95_s"`
}

// Timeseries is the time-bucketed pipeline evolution (Painel v2). A série é contínua:
// todo bucket do range aparece, com contagens zeradas e latências null quando vazio.
type Timeseries struct {
	Range   string             `json:"range"`  // 7d|30d|90d
	Bucket  string             `json:"bucket"` // day|week
	TZ      string             `json:"tz"`     // America/Sao_Paulo
	Buckets []TimeseriesBucket `json:"buckets"`
}

// DocTypeCount é a contagem de notas por tipo de documento (gráfico de distribuição).
type DocTypeCount struct {
	DocType DocType `json:"doc_type"`
	Count   int     `json:"count"`
}

// BacklogBucket é quantas notas pendentes (não-terminais) estão esperando há quanto
// tempo (faixa de idade desde a chegada). Para visualizar notas presas na fila.
type BacklogBucket struct {
	Label string `json:"label"` // <1h | 1-6h | 6-24h | 1-3d | 3-7d | >7d
	Count int    `json:"count"`
}

// BacklogBuckets é a ordem canônica das faixas de idade do backlog.
var BacklogBuckets = []string{"<1h", "1-6h", "6-24h", "1-3d", "3-7d", ">7d"}

// BacklogBucketOf mapeia uma idade (desde a chegada) para a faixa do backlog.
func BacklogBucketOf(age time.Duration) string {
	switch {
	case age < time.Hour:
		return "<1h"
	case age < 6*time.Hour:
		return "1-6h"
	case age < 24*time.Hour:
		return "6-24h"
	case age < 72*time.Hour:
		return "1-3d"
	case age < 168*time.Hour:
		return "3-7d"
	default:
		return ">7d"
	}
}

// AgingBucket é uma faixa de idade do backlog pendente. MaxDays é o limite superior
// (exclusivo) da faixa em dias; nil na faixa aberta (">30d").
type AgingBucket struct {
	Label   string `json:"label"`
	MaxDays *int   `json:"max_days,omitempty"`
	Count   int    `json:"count"`
}

// Aging é o backlog pendente por faixa de idade, separado pelas duas esperas:
// to_sync (status arrived; idade desde arrived_at) e to_import (status synced/
// pending_import; idade desde synced_at). Os anchors são ecoados p/ o front rotular.
type Aging struct {
	AnchorToSync   string        `json:"anchor_to_sync"`   // "arrived_at"
	AnchorToImport string        `json:"anchor_to_import"` // "synced_at"
	ToSync         []AgingBucket `json:"to_sync"`
	ToImport       []AgingBucket `json:"to_import"`
}

// LatencyDaily é o p50/p95 de UM dia da latência chegada→sync (GET /metrics/latency).
type LatencyDaily struct {
	Date  string  `json:"date"` // yyyy-mm-dd (dia BRT do synced_at)
	Count int     `json:"count"`
	P50S  float64 `json:"p50_s"`
	P95S  float64 `json:"p95_s"`
}

// LatencyArrivalSync agrega a latência chegada→sync da janela. Os dois timestamps vêm
// do agente (resolução real), então percentis em segundos são honestos aqui.
type LatencyArrivalSync struct {
	Count int            `json:"count"`
	P50S  *float64       `json:"p50_s"` // nil quando count=0
	P95S  *float64       `json:"p95_s"`
	Daily []LatencyDaily `json:"daily"`
}

// LatencySyncImport distribui a espera sync→import em DIAS (mesmo dia / D+1 / D+2+).
// O imported_at tem granularidade de DATA (o Athenas grava DATAROBO/DATAINCLUSAO à
// meia-noite), então percentis em segundos seriam lixo — mesmo dia daria negativo
// (meia-noite < synced_at); dias corridos é a resolução máxima honesta do dado.
type LatencySyncImport struct {
	Count      int     `json:"count"`
	SameDay    int     `json:"same_day"` // inclui diff negativo (sync observado após o import — artefato de backfill)
	D1         int     `json:"d1"`
	D2Plus     int     `json:"d2_plus"`
	SameDayPct float64 `json:"same_day_pct"`
	D1Pct      float64 `json:"d1_pct"`
	D2PlusPct  float64 `json:"d2_plus_pct"`
}

// Latency é a resposta do GET /metrics/latency (janela deslizante de N dias).
type Latency struct {
	Days          int                `json:"days"`
	TZ            string             `json:"tz"`
	ArrivalToSync LatencyArrivalSync `json:"arrival_to_sync"`
	SyncToImport  LatencySyncImport  `json:"sync_to_import"`
}

// AgingBuckets é a ordem canônica das faixas do aging (com o limite superior em dias;
// 0 = faixa aberta ">30d"). Compartilhado por Postgres e memória.
var AgingBuckets = []struct {
	Label   string
	MaxDays int // 0 = aberta (>30d)
}{
	{"<1d", 1}, {"1-3d", 3}, {"3-7d", 7}, {"7-30d", 30}, {">30d", 0},
}

// AgingBucketOf mapeia uma idade para o label da faixa do aging.
func AgingBucketOf(age time.Duration) string {
	days := age.Hours() / 24
	switch {
	case days < 1:
		return "<1d"
	case days < 3:
		return "1-3d"
	case days < 7:
		return "3-7d"
	case days < 30:
		return "7-30d"
	default:
		return ">30d"
	}
}

// EmpresaAgg is the per-empresa status breakdown (quem está pendente).
type EmpresaAgg struct {
	CodigoEmpresa *int   `json:"codigo_empresa,omitempty"`
	CodigoFilial  *int   `json:"codigo_filial,omitempty"`
	NomeEmpresa   string `json:"nome_empresa,omitempty"`
	InTransit     int    `json:"in_transit"` // arrived + synced
	StatusCounts
}

// ServiceStatus é o último heartbeat de um serviço (poller, agent, api).
type ServiceStatus struct {
	Service    string         `json:"service"`
	LastBeat   time.Time      `json:"last_beat"`
	SecondsAgo int64          `json:"seconds_ago"`
	Online     bool           `json:"online"` // last_beat nos últimos 5 min
	Payload    map[string]any `json:"payload"`
}

// NfseImport is one NFSe import-side record (lado Firebird; sem etapa de chegada).
type NfseImport struct {
	AthenasChave   string     `json:"athenas_chave"`
	CodigoEmpresa  *int       `json:"codigo_empresa,omitempty"`
	Status         NotaStatus `json:"status"`
	MotivoIgnorado string     `json:"motivo_ignorado,omitempty"`
	DataEmissao    *string    `json:"data_emissao,omitempty"`
}
