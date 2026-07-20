package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"

	"github.com/EnzzoHosaki/rps-xml-tracker/internal/model"
	"github.com/EnzzoHosaki/rps-xml-tracker/internal/signing"
	"github.com/EnzzoHosaki/rps-xml-tracker/internal/store"
)

const (
	testJWT   = "test-jwt-secret-which-is-long-enough"
	testAgent = "test-agent-secret"
)

func newTestServer() http.Handler {
	gin.SetMode(gin.TestMode)
	return New(store.NewMemory(), Config{JWTSecret: testJWT, AgentSecret: testAgent}).Handler()
}

func makeJWT(t *testing.T, role string) string {
	t.Helper()
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, claims{
		UserID: 1, Email: "x@rps", Role: role,
		RegisteredClaims: jwt.RegisteredClaims{ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour))},
	})
	s, err := tok.SignedString([]byte(testJWT))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func ingestBody() []byte {
	req := ingestRequest{
		Agent: "SRVIMPORT",
		Batch: []model.Observation{
			{ChaveAcesso: "K1", Stage: model.StageArrival, EventType: model.EventFileSeen,
				ObservedAt: time.Date(2026, 6, 8, 9, 0, 0, 0, time.UTC), DocType: model.DocNFe,
				FilePath: `F:\Xml_ASincronizar\ZZZ_XML_BOT\k1.xml`},
			{ChaveAcesso: "K1", Stage: model.StageSync, EventType: model.EventFileMoved,
				ObservedAt: time.Date(2026, 6, 8, 9, 30, 0, 0, time.UTC), DocType: model.DocNFe,
				FilePath: `F:\XML SINCRONIZADO\04771171\202606\NFe\k1.xml`},
		},
	}
	b, _ := json.Marshal(req)
	return b
}

func TestIngestAndGetNota_EndToEnd(t *testing.T) {
	h := newTestServer()
	body := ingestBody()

	// ingest with valid HMAC
	req := httptest.NewRequest(http.MethodPost, "/api/v1/ingest/observations", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Agent-Signature", signing.Sign(testAgent, body))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusAccepted {
		t.Fatalf("ingest code = %d, body=%s", w.Code, w.Body.String())
	}
	var ing struct{ Accepted, Rejected int }
	json.Unmarshal(w.Body.Bytes(), &ing)
	if ing.Accepted != 2 || ing.Rejected != 0 {
		t.Fatalf("ingest accepted=%d rejected=%d, want 2/0", ing.Accepted, ing.Rejected)
	}

	// idempotency: re-ingest same batch -> all rejected as duplicates
	req2 := httptest.NewRequest(http.MethodPost, "/api/v1/ingest/observations", bytes.NewReader(body))
	req2.Header.Set("X-Agent-Signature", signing.Sign(testAgent, body))
	w2 := httptest.NewRecorder()
	h.ServeHTTP(w2, req2)
	json.Unmarshal(w2.Body.Bytes(), &ing)
	if ing.Accepted != 0 || ing.Rejected != 2 {
		t.Fatalf("re-ingest accepted=%d rejected=%d, want 0/2", ing.Accepted, ing.Rejected)
	}

	// get nota with JWT -> derived state + spans
	gr := httptest.NewRequest(http.MethodGet, "/api/v1/notas/K1", nil)
	gr.Header.Set("Authorization", "Bearer "+makeJWT(t, "viewer"))
	gw := httptest.NewRecorder()
	h.ServeHTTP(gw, gr)
	if gw.Code != http.StatusOK {
		t.Fatalf("get nota code = %d, body=%s", gw.Code, gw.Body.String())
	}
	var detail model.NotaDetail
	if err := json.Unmarshal(gw.Body.Bytes(), &detail); err != nil {
		t.Fatal(err)
	}
	if detail.Status != model.StatusSynced {
		t.Errorf("status = %s, want synced", detail.Status)
	}
	if len(detail.Spans) != 2 {
		t.Errorf("spans = %d, want 2", len(detail.Spans))
	}
	if detail.LatArrivalSyncS == nil || *detail.LatArrivalSyncS != 1800 {
		t.Errorf("lat = %v, want 1800", detail.LatArrivalSyncS)
	}
	// spans ganham o caminho na visão da rede (F:\ interno -> R:\ do share)
	wantRede := map[model.Stage]string{
		model.StageArrival: `R:\XML_ASINCRONIZAR\ZZZ_XML_BOT\k1.xml`,
		model.StageSync:    `R:\XML_SINCRONIZADO\04771171\202606\NFe\k1.xml`,
	}
	for _, sp := range detail.Spans {
		if sp.FilePathRede != wantRede[sp.Stage] {
			t.Errorf("span %s file_path_rede = %q, want %q", sp.Stage, sp.FilePathRede, wantRede[sp.Stage])
		}
	}
}

func TestIngest_BadHMAC_401(t *testing.T) {
	h := newTestServer()
	body := ingestBody()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/ingest/observations", bytes.NewReader(body))
	req.Header.Set("X-Agent-Signature", "deadbeef")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("code = %d, want 401", w.Code)
	}
}

// worklist: autenticado por HMAC (não JWT), valida roots+since, e devolve a forma
// {items,total}. O store em memória devolve lista vazia — aqui exercitamos auth,
// validação e o contrato de resposta, não a query (essa é testada com PG real).
func TestWorklist_HMAC_and_validation(t *testing.T) {
	h := newTestServer()
	post := func(sig string, payload map[string]any) *httptest.ResponseRecorder {
		body, _ := json.Marshal(payload)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/ingest/worklist", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		if sig != "" {
			req.Header.Set("X-Agent-Signature", sig)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		return w
	}
	valid := map[string]any{"roots": []string{"11222333"}, "since": "2026-06-01", "limit": 10}

	// HMAC válido -> 200 + {items,total}
	vb, _ := json.Marshal(valid)
	w := post(signing.Sign(testAgent, vb), valid)
	if w.Code != http.StatusOK {
		t.Fatalf("code = %d, want 200 (body=%s)", w.Code, w.Body.String())
	}
	var resp struct {
		Items []model.WorklistItem `json:"items"`
		Total int                  `json:"total"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("resposta não casa o contrato {items,total}: %v", err)
	}

	// assinatura inválida -> 401 (mesma guarda do ingest)
	if got := post("deadbeef", valid).Code; got != http.StatusUnauthorized {
		t.Errorf("bad HMAC code = %d, want 401", got)
	}
	// sem assinatura -> 401
	if got := post("", valid).Code; got != http.StatusUnauthorized {
		t.Errorf("no HMAC code = %d, want 401", got)
	}
	// roots vazio -> 200 (= TODAS as empresas, paridade com o sweep). Assina o body.
	empty := map[string]any{"roots": []string{}, "since": "2026-06-01"}
	eb, _ := json.Marshal(empty)
	if got := post(signing.Sign(testAgent, eb), empty).Code; got != http.StatusOK {
		t.Errorf("empty roots code = %d, want 200 (todas)", got)
	}
	// since inválido -> 400
	bad := map[string]any{"roots": []string{"11222333"}, "since": "ontem"}
	bb, _ := json.Marshal(bad)
	if got := post(signing.Sign(testAgent, bb), bad).Code; got != http.StatusBadRequest {
		t.Errorf("bad since code = %d, want 400", got)
	}
}

func TestGetNota_NoJWT_401(t *testing.T) {
	h := newTestServer()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/notas/K1", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("code = %d, want 401", w.Code)
	}
}

func TestGetNota_NotFound_404(t *testing.T) {
	h := newTestServer()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/notas/UNKNOWN", nil)
	req.Header.Set("Authorization", "Bearer "+makeJWT(t, "viewer"))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("code = %d, want 404", w.Code)
	}
}
