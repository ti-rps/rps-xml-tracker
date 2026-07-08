// Package api wires the tracker's HTTP surface (Gin). Conventions mirror the
// maestro backend: base /api/v1, JWT Bearer for reads, {"error":...} on failure,
// {items,total,limit,offset} for lists. Ingest is authenticated by the agent HMAC.
package api

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/EnzzoHosaki/rps-xml-tracker/internal/model"
	"github.com/EnzzoHosaki/rps-xml-tracker/internal/netpath"
	"github.com/EnzzoHosaki/rps-xml-tracker/internal/store"
	"github.com/EnzzoHosaki/rps-xml-tracker/internal/version"
)

// Config holds the secrets/wiring the server needs.
type Config struct {
	JWTSecret   string          // MAESTRO_JWT_SECRET (validated non-empty at boot)
	AgentSecret string          // shared HMAC secret with the SRVIMPORT agent
	CORSOrigins []string        // allowed browser origins for the maestro_web UI ("" = none)
	NetPath     *netpath.Mapper // tradução F:\ (interno) → R:\ (rede) na exibição; nil = Default
}

// Server is the HTTP server.
type Server struct {
	st  store.Store
	cfg Config
	r   *gin.Engine
}

func New(st store.Store, cfg Config) *Server {
	if cfg.NetPath == nil {
		cfg.NetPath = netpath.Default()
	}
	s := &Server{st: st, cfg: cfg, r: gin.New()}
	s.r.Use(gin.Recovery())
	if len(cfg.CORSOrigins) > 0 {
		s.r.Use(cors(cfg.CORSOrigins))
	}
	s.routes()
	return s
}

// cors mirrors the maestro CORS conventions so the maestro_web UI can call this
// API cross-origin (browser). Server-to-server callers (the agent) don't need it.
func cors(allowed []string) gin.HandlerFunc {
	set := make(map[string]bool, len(allowed))
	for _, o := range allowed {
		set[strings.TrimSpace(o)] = true
	}
	return func(c *gin.Context) {
		origin := c.GetHeader("Origin")
		if origin != "" && set[origin] {
			c.Header("Access-Control-Allow-Origin", origin)
			c.Header("Vary", "Origin")
			c.Header("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
			c.Header("Access-Control-Allow-Headers", "Origin, Content-Type, Authorization")
			c.Header("Access-Control-Max-Age", "43200")
		}
		if c.Request.Method == http.MethodOptions {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}
		c.Next()
	}
}

// Handler exposes the router (for httptest and for http.Server).
func (s *Server) Handler() http.Handler { return s.r }

func (s *Server) routes() {
	v1 := s.r.Group("/api/v1")
	v1.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok", "version": version.Commit, "built_at": version.BuiltAt})
	})

	// ingest — agent HMAC
	ingest := v1.Group("/ingest", agentHMAC(s.cfg.AgentSecret))
	ingest.POST("/observations", s.handleIngest)
	ingest.POST("/agent/heartbeat", s.handleAgentHeartbeat)

	// reads — maestro JWT
	read := v1.Group("", jwtAuth(s.cfg.JWTSecret))
	read.GET("/notas", s.handleListNotas)
	read.GET("/notas/summary", s.handleNotasSummary) // estático antes do :chave
	read.GET("/notas/:chave", s.handleGetNota)
	read.GET("/metrics/overview", s.handleOverview)
	read.GET("/metrics/timeseries", s.handleTimeseries)
	read.GET("/metrics/doctypes", s.handleDocTypes)
	read.GET("/metrics/backlog-age", s.handleBacklogAge)
	read.GET("/metrics/aging", s.handleAging)
	read.GET("/metrics/latency", s.handleLatency)
	read.GET("/empresas", s.handleEmpresas)
	read.GET("/nfse/import", s.handleNfseImport)
	read.GET("/status", s.handleStatus)
}

func (s *Server) handleOverview(c *gin.Context) {
	f := store.OverviewFilter{
		DateField: c.Query("date_field"), // emissao|arrived|synced|imported
		From:      c.Query("from"),        // yyyy-mm-dd (inclusive)
		To:        c.Query("to"),          // yyyy-mm-dd (inclusive)
		DocType:   model.DocType(c.Query("doc_type")),
	}
	if v := c.Query("codigo_empresa"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			f.CodigoEmpresa = &n
		}
	}
	if v := c.Query("codigo_filial"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			f.CodigoFilial = &n
		}
	}
	ov, err := s.st.Overview(c.Request.Context(), f)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "falha ao calcular overview"})
		return
	}
	c.JSON(http.StatusOK, ov)
}

func (s *Server) handleStatus(c *gin.Context) {
	services, err := s.st.GetStatus(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"services": services})
}

func (s *Server) handleAgentHeartbeat(c *gin.Context) {
	body, err := io.ReadAll(io.LimitReader(c.Request.Body, 64*1024))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "leitura do body falhou"})
		return
	}
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "json inválido"})
		return
	}
	if err := s.st.UpsertHeartbeat(c.Request.Context(), "agent", payload); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func (s *Server) handleTimeseries(c *gin.Context) {
	days, ok := map[string]int{"7d": 7, "30d": 30, "90d": 90}[c.DefaultQuery("range", "30d")]
	if !ok {
		c.JSON(http.StatusBadRequest, gin.H{"error": "range inválido (use 7d|30d|90d)"})
		return
	}
	bucket := c.DefaultQuery("bucket", "day")
	if bucket != "day" && bucket != "week" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "bucket inválido (use day|week)"})
		return
	}
	ts, err := s.st.Timeseries(c.Request.Context(), store.TimeseriesFilter{RangeDays: days, Bucket: bucket})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "falha ao calcular timeseries"})
		return
	}
	c.JSON(http.StatusOK, ts)
}

func (s *Server) handleDocTypes(c *gin.Context) {
	items, err := s.st.DocTypes(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "falha ao calcular distribuição por tipo"})
		return
	}
	if items == nil {
		items = []model.DocTypeCount{}
	}
	c.JSON(http.StatusOK, gin.H{"items": items})
}

func (s *Server) handleAging(c *gin.Context) {
	f := store.AgingFilter{DocType: model.DocType(c.Query("doc_type")), Direction: c.Query("direction")}
	if v := c.Query("codigo_empresa"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			f.CodigoEmpresa = &n
		}
	}
	if v := c.Query("codigo_filial"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			f.CodigoFilial = &n
		}
	}
	ag, err := s.st.Aging(c.Request.Context(), f)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "falha ao calcular aging"})
		return
	}
	c.JSON(http.StatusOK, ag)
}

// handleLatency serve o GET /metrics/latency?days=7 (1..90). Chegada→sync em
// percentis de segundos; sync→import em distribuição de dias (imported_at é
// date-only — ver model.LatencySyncImport).
func (s *Server) handleLatency(c *gin.Context) {
	days := 7
	if v := c.Query("days"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 1 && n <= 90 {
			days = n
		}
	}
	lat, err := s.st.Latency(c.Request.Context(), days)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "falha ao calcular latência"})
		return
	}
	c.JSON(http.StatusOK, lat)
}

func (s *Server) handleBacklogAge(c *gin.Context) {
	items, err := s.st.BacklogAge(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "falha ao calcular idade do backlog"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": items})
}

func (s *Server) handleEmpresas(c *gin.Context) {
	f := store.EmpresaFilter{
		PendentesOnly: c.Query("pendentes") == "true",
		Query:         c.Query("q"),
		Sort:          c.Query("sort"),
		DocType:       model.DocType(c.Query("doc_type")), // NFE|NFCE|CTE|NFS|EVENTO|UNKNOWN
		Direction:     c.Query("direction"),               // entrada|saida
		DateField:     c.Query("date_field"),              // emissao|arrived|synced|imported
		From:          c.Query("from"),                    // yyyy-mm-dd (inclusive)
		To:            c.Query("to"),                      // yyyy-mm-dd (inclusive)
		Limit:         atoiDefault(c.Query("limit"), 0),   // 0 = todas (sem paginação)
		Offset:        atoiDefault(c.Query("offset"), 0),
	}
	items, total, err := s.st.Empresas(c.Request.Context(), f)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "falha ao listar empresas"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": items, "total": total, "limit": f.Limit, "offset": f.Offset})
}

func (s *Server) handleNfseImport(c *gin.Context) {
	f := store.NfseFilter{
		Status: model.NotaStatus(c.Query("status")),
		Limit:  atoiDefault(c.Query("limit"), 50),
		Offset: atoiDefault(c.Query("offset"), 0),
	}
	items, total, err := s.st.ListNfseImport(c.Request.Context(), f)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "falha ao listar nfse"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": items, "total": total, "limit": f.Limit, "offset": f.Offset})
}

type ingestRequest struct {
	Agent string              `json:"agent"`
	Batch []model.Observation `json:"batch"`
}

func (s *Server) handleIngest(c *gin.Context) {
	var req ingestRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "json inválido: " + err.Error()})
		return
	}
	now := time.Now()
	for i := range req.Batch {
		if req.Batch[i].Source == "" {
			req.Batch[i].Source = "agent:" + req.Agent
		}
		if req.Batch[i].IngestedAt.IsZero() {
			req.Batch[i].IngestedAt = now
		}
	}
	accepted, rejected, err := s.st.AppendObservations(c.Request.Context(), req.Batch)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "falha ao gravar observações"})
		return
	}
	c.JSON(http.StatusAccepted, gin.H{"accepted": accepted, "rejected": rejected})
}

func (s *Server) handleGetNota(c *gin.Context) {
	chave := c.Param("chave")
	detail, ok, err := s.st.GetNota(c.Request.Context(), chave)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "falha ao consultar nota"})
		return
	}
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "nota não encontrada"})
		return
	}
	// anexa o caminho na visão da rede (R:\...) — o file_path gravado é a visão
	// interna do SRVIMPORT (F:\...), que ninguém fora dele consegue abrir
	for i := range detail.Spans {
		detail.Spans[i].FilePathRede = s.cfg.NetPath.Rede(detail.Spans[i].FilePath)
	}
	c.JSON(http.StatusOK, detail)
}

// notaFilterFromQuery lê os filtros de /notas da querystring (compartilhado por
// handleListNotas e handleNotasSummary p/ os filtros ficarem idênticos).
func notaFilterFromQuery(c *gin.Context) store.NotaFilter {
	f := store.NotaFilter{
		Status:       model.NotaStatus(c.Query("status")),
		DocType:      model.DocType(c.Query("doc_type")),
		SemEmpresa:   c.Query("sem_empresa") == "true",
		EmpresaQuery: c.Query("empresa"),
		Cnpj:         c.Query("cnpj"),
		ChaveQuery:   c.Query("q"),
		Numero:       onlyDigits(c.Query("numero")),
		Direction:    c.Query("direction"), // entrada|saida
		DateField:    c.Query("date_field"),
		From:         c.Query("from"),
		To:           c.Query("to"),
		Limit:        atoiDefault(c.Query("limit"), 50),
		Offset:       atoiDefault(c.Query("offset"), 0),
	}
	if v := c.Query("codigo_empresa"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			f.CodigoEmpresa = &n
		}
	}
	if v := c.Query("codigo_filial"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			f.CodigoFilial = &n
		}
	}
	return f
}

func (s *Server) handleNotasSummary(c *gin.Context) {
	sum, err := s.st.SummaryNotas(c.Request.Context(), notaFilterFromQuery(c))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "falha ao somar notas"})
		return
	}
	c.JSON(http.StatusOK, sum)
}

func (s *Server) handleListNotas(c *gin.Context) {
	f := notaFilterFromQuery(c)
	items, total, err := s.st.ListNotas(c.Request.Context(), f)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "falha ao listar notas"})
		return
	}
	if items == nil {
		items = []model.Nota{}
	}
	c.JSON(http.StatusOK, gin.H{"items": items, "total": total, "limit": f.Limit, "offset": f.Offset})
}

// onlyDigits mantém apenas os dígitos de s (o número da nota é numérico). Descarta
// curingas de LIKE (%, _) e espaços, garantindo que o param `numero` case o índice
// de prefixo sem injeção de wildcard.
func onlyDigits(s string) string {
	var b strings.Builder
	for _, r := range s {
		if r >= '0' && r <= '9' {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func atoiDefault(s string, def int) int {
	if n, err := strconv.Atoi(s); err == nil {
		return n
	}
	return def
}

// readAndRestoreBody reads the full body and restores it so downstream handlers
// can bind it again (needed because HMAC verification consumes the body).
func readAndRestoreBody(c *gin.Context) ([]byte, error) {
	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		return nil, err
	}
	c.Request.Body = io.NopCloser(bytes.NewReader(body))
	return body, nil
}
