package ingest

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/EnzzoHosaki/rps-xml-tracker/internal/model"
	"github.com/EnzzoHosaki/rps-xml-tracker/internal/signing"
)

const secret = "agent-secret"

func obs() []model.Observation {
	return []model.Observation{{ChaveAcesso: "K", Stage: model.StageArrival, EventType: model.EventFileSeen}}
}

func TestSubmit_VerifiesSignatureServerSide(t *testing.T) {
	var gotSig, gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b := make([]byte, r.ContentLength)
		_, _ = r.Body.Read(b)
		gotBody = string(b)
		gotSig = r.Header.Get("X-Agent-Signature")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"accepted":1,"rejected":0}`))
	}))
	defer srv.Close()

	c, _ := New(srv.URL, "SRVIMPORT", secret, t.TempDir())
	if err := c.Submit(context.Background(), obs()); err != nil {
		t.Fatalf("submit: %v", err)
	}
	if gotSig == "" || gotSig != signing.Sign(secret, []byte(gotBody)) {
		t.Errorf("server signature mismatch: header=%s", gotSig)
	}
}

func TestSubmit_SpoolsOnFailureAndFlushes(t *testing.T) {
	spool := t.TempDir()
	ctx := context.Background()

	// 1) server down -> Submit must spool, not error
	down, _ := New("http://127.0.0.1:1/nope", "A", secret, spool)
	down.retries = 0
	if err := down.Submit(ctx, obs()); err != nil {
		t.Fatalf("submit while down should spool, got err: %v", err)
	}
	files, _ := os.ReadDir(spool)
	if len(files) != 1 {
		t.Fatalf("expected 1 spooled batch, got %d", len(files))
	}

	// 2) server up -> FlushSpool resends and clears the spool
	var hits int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits++
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"accepted":1,"rejected":0}`))
	}))
	defer srv.Close()

	up, _ := New(srv.URL, "A", secret, spool)
	resent, err := up.FlushSpool(ctx)
	if err != nil || resent != 1 || hits != 1 {
		t.Fatalf("flush: resent=%d hits=%d err=%v (want 1/1/nil)", resent, hits, err)
	}
	if files, _ := os.ReadDir(spool); len(files) != 0 {
		t.Fatalf("spool not cleared: %d files", len(files))
	}
}
