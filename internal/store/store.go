// Package store abstracts persistence behind an interface so the API/worker can
// run against an in-memory store (tests, local smoke runs) or Postgres (prod)
// without code changes. The Postgres (pgx) implementation lands behind this same
// interface in the next slice.
package store

import (
	"context"
	"strconv"
	"time"

	"github.com/EnzzoHosaki/rps-xml-tracker/internal/model"
)

// Store persists observations (append-only, idempotent) and serves derived notas.
type Store interface {
	// AppendObservations stores a batch idempotently (dedup by DedupKey).
	// Returns how many were newly accepted vs. skipped as duplicates.
	AppendObservations(ctx context.Context, obs []model.Observation) (accepted, rejected int, err error)

	// GetNota returns the derived nota + its span timeline, or ok=false if unknown.
	GetNota(ctx context.Context, chave string) (model.NotaDetail, bool, error)

	// ListNotas returns derived notas matching the filter (limit/offset paging).
	ListNotas(ctx context.Context, f NotaFilter) (items []model.Nota, total int, err error)

	// SummaryNotas returns count + sum(valor_total) for the SAME filter as ListNotas
	// (apuração — ex.: total das NFC-e do período), sem paginar.
	SummaryNotas(ctx context.Context, f NotaFilter) (model.NotaSummary, error)

	// ListInflightChaves returns chaves still in flight (status arrived/synced —
	// not yet imported/import_ignored), for the chave-driven Firebird poller.
	ListInflightChaves(ctx context.Context, limit int) ([]string, error)

	// ListChavesByStatus returns chaves whose derived status equals the given one
	// (limit<=0 = todas). Para o re-poll one-off de notas terminais.
	ListChavesByStatus(ctx context.Context, status model.NotaStatus, limit, offset int) ([]string, error)

	// DeleteImportIgnoredObs removes a chave's import_ignored observations and
	// recomputes the nota. DESTRUTIVO: usado só na correção retroativa de notas
	// erradamente terminais (terceiro ignorou antes de a dona importar). Retorna
	// quantas observações removeu.
	DeleteImportIgnoredObs(ctx context.Context, chave string) (int, error)

	// ListChavesImportedSince retorna as chaves com status imported cujo imported_at
	// é >= since. Para a correção retroativa do imported_at (janela recente).
	ListChavesImportedSince(ctx context.Context, since time.Time) ([]string, error)

	// StatusForChaves retorna o status derivado atual de cada chave dada (as ausentes
	// não aparecem no mapa). Usado pelo reconcile contínuo: a acurácia do import é
	// medida perguntando o STATUS das chaves que o Athenas importou — nunca recortando
	// o tracker por imported_at, cuja granularidade é de DATA (DATAROBO/DATAINCLUSAO
	// vêm com hora 00:00) e não casa com janela rolante de relógio.
	StatusForChaves(ctx context.Context, chaves []string) (map[string]model.NotaStatus, error)

	// KnownImported reporta quais das chaves dadas já têm ALGUMA importação registrada
	// (imported_at não-nulo). É o teste correto do reconcile no modelo M0: o STATUS
	// agregado não serve, porque "importada 1/2" (uma participação importou, outra
	// pende) fica pending_import até todas terminarem — pelo status, essas chaves
	// contariam como "faltando" para sempre. Ausentes não aparecem no mapa.
	KnownImported(ctx context.Context, chaves []string) (map[string]bool, error)

	// UpdateImportedObservedAt reescreve o observed_at da observação 'imported' de uma
	// chave (e re-deriva a nota) quando difere do valor dado; retorna se mudou.
	// DESTRUTIVO: usado só na correção retroativa do fuso do imported_at.
	UpdateImportedObservedAt(ctx context.Context, chave string, observedAt time.Time) (bool, error)

	// Overview returns the dashboard summary cards. Com OverviewFilter vazio = estoque
	// atual global (snapshot). Com janela de data e/ou filtros = recompute ao vivo das
	// contagens por status (mode="flow"); imported_today e latências seguem globais/30d.
	Overview(ctx context.Context, f OverviewFilter) (model.Overview, error)

	// Timeseries returns time-bucketed pipeline flow + latency percentiles for the
	// Painel v2 charts (série contínua, zero-fill nas contagens, nil nas latências).
	Timeseries(ctx context.Context, f TimeseriesFilter) (model.Timeseries, error)

	// DocTypes returns nota counts grouped by doc_type (gráfico de distribuição).
	DocTypes(ctx context.Context) ([]model.DocTypeCount, error)

	// BacklogAge returns how many pending (non-terminal) notas fall in each age
	// bucket since arrival (notas presas na fila). Faixas em model.BacklogBuckets.
	BacklogAge(ctx context.Context) ([]model.BacklogBucket, error)

	// Latency returns the pipeline latency metrics over the last N days (janela
	// deslizante): chegada→sync em percentis de segundos (timestamps reais do agente)
	// e sync→import em DIAS (imported_at é date-only — percentil em segundos seria
	// lixo). Barato: janelas indexadas de ~N×30-100k linhas.
	Latency(ctx context.Context, days int) (model.Latency, error)

	// Aging returns the pending backlog bucketed by age, split into the two waits:
	// to_sync (status arrived, idade desde arrived_at) e to_import (status synced/
	// pending_import, idade desde synced_at). Filtrável por empresa/filial/doc_type.
	Aging(ctx context.Context, f AgingFilter) (model.Aging, error)

	// Empresas returns the per-empresa status breakdown. total is the number of
	// empresas matching the filter (before limit/offset), for pagination.
	Empresas(ctx context.Context, f EmpresaFilter) (items []model.EmpresaAgg, total int, err error)

	// ListNfseImport returns NFSe import-side records (lado Firebird).
	ListNfseImport(ctx context.Context, f NfseFilter) (items []model.NfseImport, total int, err error)

	// UpsertHeartbeat atualiza (ou insere) o heartbeat de um serviço com o payload fornecido.
	UpsertHeartbeat(ctx context.Context, service string, payload map[string]any) error
	// GetStatus retorna o último heartbeat de cada serviço registrado.
	GetStatus(ctx context.Context) ([]model.ServiceStatus, error)
}

// EmpresaFilter holds the supported per-empresa aggregation filters.
type EmpresaFilter struct {
	PendentesOnly bool          // só empresas com itens não-terminais (arrived/synced/pending_import/stuck)
	Query         string        // busca por nome da empresa (ILIKE); vazio = todas
	Sort          string        // "pendentes" = mais pendentes primeiro; vazio/"codigo" = por código
	DocType       model.DocType // filtra por tipo de documento (dimensão do contador desde a 00014)
	Direction     string        // entrada|saida (dimensão do contador desde a 00014)
	// faixa de data sobre o campo escolhido (mesmos nomes do GET /notas):
	// emissao|arrived|synced|imported. Quando preenchida, os agregados são
	// recomputados ao vivo da notas (o contador empresa_counts não tem dimensão
	// temporal); sem faixa, lê do contador (instantâneo).
	DateField string
	From      string // yyyy-mm-dd (inclusive)
	To        string // yyyy-mm-dd (inclusive)
	Limit     int    // <=0 retorna todas (sem paginação)
	Offset    int
}

// AgingFilter restringe o aging do backlog (GET /metrics/aging). Todos opcionais.
type AgingFilter struct {
	CodigoEmpresa *int
	CodigoFilial  *int
	DocType       model.DocType
	Direction     string // entrada|saida
}

// OverviewFilter restringe o overview (GET /metrics/overview). Tudo opcional; vazio =
// snapshot global. Com janela (date_field+from/to) e/ou empresa/filial/doc_type, as
// contagens por status são recomputadas ao vivo dentro do recorte.
type OverviewFilter struct {
	DateField     string // emissao|arrived|synced|imported
	From          string // yyyy-mm-dd (inclusive)
	To            string // yyyy-mm-dd (inclusive)
	CodigoEmpresa *int
	CodigoFilial  *int
	DocType       model.DocType
}

// windowed reporta se há uma janela de data válida (date_field reconhecido + from/to).
func (f OverviewFilter) windowed() bool {
	return dateColumn(f.DateField) != "" && (f.From != "" || f.To != "")
}

// live reporta se o overview precisa recomputar ao vivo (janela de data e/ou
// empresa/filial) em vez de ler o contador notas_counts. DocType sozinho NÃO força
// mais o recompute: o contador tem a dimensão (migração 00014).
func (f OverviewFilter) live() bool {
	return f.windowed() || f.CodigoEmpresa != nil || f.CodigoFilial != nil
}

// TimeseriesFilter holds the timeseries query params (já validados no handler).
type TimeseriesFilter struct {
	RangeDays int    // 7 | 30 | 90 (echo no response como "%dd")
	Bucket    string // "day" | "week"
}

// NfseFilter holds the supported NFSe list filters.
type NfseFilter struct {
	CodigoEmpresa *int
	Status        model.NotaStatus
	Limit         int
	Offset        int
}

// NotaFilter holds the supported list filters.
type NotaFilter struct {
	Status        model.NotaStatus
	DocType       model.DocType
	CodigoEmpresa *int
	CodigoFilial  *int   // filtra a filial exata (combina com CodigoEmpresa via AND)
	SemEmpresa    bool   // só notas sem empresa identificada (codigo_empresa IS NULL)
	EmpresaQuery  string // LIKE em empresa_nome
	Cnpj          string // LIKE em cnpj_emitente OU cnpj_destinatario
	ChaveQuery    string // partial/full chave
	Numero        string // prefixo do número da nota (nNF derivado da chave); distinto de ChaveQuery
	Direction     string // entrada|saida (lado da empresa)
	// faixa de data sobre o campo escolhido: emissao|arrived|synced|imported
	DateField string
	From      string // yyyy-mm-dd (inclusive)
	To        string // yyyy-mm-dd (inclusive)
	Limit     int
	Offset    int
}

// dateColumn maps a DateField to the notas column (empty = unsupported/ignored).
func dateColumn(field string) string {
	switch field {
	case "emissao":
		return "data_emissao"
	case "arrived":
		return "arrived_at"
	case "synced":
		return "synced_at"
	case "imported":
		return "imported_at"
	default:
		return ""
	}
}

// DedupKey is the idempotency key for an observation: same source+stage+event+
// chave+file_hash never stored twice. M0: observações que carregam empresa
// (poller/syncer) ganham o sufixo empresa/filial — cada PARTICIPAÇÃO tem seu
// próprio ciclo, então "imported da empresa A" e "seen_pending da empresa B"
// coexistem para a mesma chave. Efeito colateral único e aceito: a primeira
// reemissão pós-deploy de um seen_pending antigo é aceita de novo (chave nova);
// o derive colapsa por setIfEarlier, então nada muda no estado.
func DedupKey(o model.Observation) string {
	k := o.Source + "|" + string(o.Stage) + "|" + o.EventType + "|" + o.ChaveAcesso + "|" + o.FileHash
	if o.CodigoEmpresa != nil {
		fil := 0
		if o.CodigoFilial != nil {
			fil = *o.CodigoFilial
		}
		k += "|" + strconv.Itoa(*o.CodigoEmpresa) + "/" + strconv.Itoa(fil)
	}
	return k
}
