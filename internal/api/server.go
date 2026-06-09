// Package api wires the tracker's HTTP surface (Gin). Conventions mirror the
// maestro backend: base /api/v1, JWT Bearer for reads, {"error":...} on failure,
// {items,total,limit,offset} for lists. Ingest is authenticated by the agent HMAC.
package api

import (
	"bytes"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/EnzzoHosaki/rps-xml-tracker/internal/model"
	"github.com/EnzzoHosaki/rps-xml-tracker/internal/store"
)

// Config holds the secrets/wiring the server needs.
type Config struct {
	JWTSecret   string   // MAESTRO_JWT_SECRET (validated non-empty at boot)
	AgentSecret string   // shared HMAC secret with the SRVIMPORT agent
	CORSOrigins []string // allowed browser origins for the maestro_web UI ("" = none)
}

// Server is the HTTP server.
type Server struct {
	st  store.Store
	cfg Config
	r   *gin.Engine
}

func New(st store.Store, cfg Config) *Server {
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
	v1.GET("/health", func(c *gin.Context) { c.JSON(http.StatusOK, gin.H{"status": "ok"}) })

	// ingest — agent HMAC
	ingest := v1.Group("/ingest", agentHMAC(s.cfg.AgentSecret))
	ingest.POST("/observations", s.handleIngest)

	// reads — maestro JWT
	read := v1.Group("", jwtAuth(s.cfg.JWTSecret))
	read.GET("/notas", s.handleListNotas)
	read.GET("/notas/:chave", s.handleGetNota)
	read.GET("/metrics/overview", s.handleOverview)
	read.GET("/empresas", s.handleEmpresas)
	read.GET("/nfse/import", s.handleNfseImport)
}

func (s *Server) handleOverview(c *gin.Context) {
	ov, err := s.st.Overview(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "falha ao calcular overview"})
		return
	}
	c.JSON(http.StatusOK, ov)
}

func (s *Server) handleEmpresas(c *gin.Context) {
	items, err := s.st.Empresas(c.Request.Context(), c.Query("pendentes") == "true")
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "falha ao listar empresas"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": items, "total": len(items)})
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
	c.JSON(http.StatusOK, detail)
}

func (s *Server) handleListNotas(c *gin.Context) {
	f := store.NotaFilter{
		Status:     model.NotaStatus(c.Query("status")),
		DocType:    model.DocType(c.Query("doc_type")),
		ChaveQuery: c.Query("q"),
		Limit:      atoiDefault(c.Query("limit"), 50),
		Offset:     atoiDefault(c.Query("offset"), 0),
	}
	if v := c.Query("codigo_empresa"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			f.CodigoEmpresa = &n
		}
	}
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
