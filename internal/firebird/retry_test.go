package firebird

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

// errKilled é o erro REAL observado em produção quando a manutenção do Firebird
// mata a sessão ociosa do syncer.
var errKilled = errors.New("connection shutdown\nKilled by database administrator.")

func shortBackoff(t *testing.T) {
	t.Helper()
	old := connBackoffBase
	connBackoffBase = time.Millisecond
	t.Cleanup(func() { connBackoffBase = old })
}

// Primeira tentativa cai com erro transitório, a seguinte funciona: a conexão
// quebrada é descartada (flush) e a operação recupera sem reiniciar nada.
func TestRetryConn_TransienteDepoisOK(t *testing.T) {
	shortBackoff(t)
	calls, flushes := 0, 0
	var logs []string
	logf := func(f string, a ...any) { logs = append(logs, fmt.Sprintf(f, a...)) }

	err := retryConn(context.Background(), logf, func() { flushes++ }, "listar filiais",
		func(context.Context) error {
			calls++
			if calls == 1 {
				return errKilled
			}
			return nil
		})
	if err != nil {
		t.Fatalf("retryConn = %v; want nil", err)
	}
	if calls != 2 || flushes != 1 {
		t.Errorf("calls=%d flushes=%d; want 2/1 (a conexão morta tem de ser descartada UMA vez)", calls, flushes)
	}
	joined := strings.Join(logs, "\n")
	if !strings.Contains(joined, "tentativa 1/3") || !strings.Contains(joined, "recuperado") {
		t.Errorf("logs deveriam registrar a tentativa e a recuperação:\n%s", joined)
	}
}

// Erro que NÃO é de conexão (SQL/dados) retorna imediatamente, sem retry nem flush.
func TestRetryConn_ErroPermanenteSemRetry(t *testing.T) {
	shortBackoff(t)
	permanente := errors.New("Dynamic SQL Error\nSQL error code = -204\nTable unknown")
	calls, flushes := 0, 0
	err := retryConn(context.Background(), func(string, ...any) {}, func() { flushes++ }, "x",
		func(context.Context) error { calls++; return permanente })
	if !errors.Is(err, permanente) {
		t.Fatalf("err = %v; want o erro original", err)
	}
	if calls != 1 || flushes != 0 {
		t.Errorf("calls=%d flushes=%d; erro permanente não pode gastar retry", calls, flushes)
	}
}

// Erro transitório persistente esgota as tentativas e RETORNA erro — nunca
// sucesso silencioso, nunca loop infinito.
func TestRetryConn_EsgotaTentativas(t *testing.T) {
	shortBackoff(t)
	calls := 0
	err := retryConn(context.Background(), func(string, ...any) {}, func() {}, "listar filiais",
		func(context.Context) error { calls++; return errKilled })
	if err == nil {
		t.Fatal("indisponibilidade prolongada tem de retornar erro")
	}
	if calls != connAttempts {
		t.Errorf("calls = %d; want %d", calls, connAttempts)
	}
	if !errors.Is(err, errKilled) || !strings.Contains(err.Error(), "esgotadas") {
		t.Errorf("err = %v; deveria embrulhar o último erro e dizer que esgotou", err)
	}
}

// Cancelamento do contexto interrompe o backoff na hora.
func TestRetryConn_CancelamentoNoBackoff(t *testing.T) {
	old := connBackoffBase
	connBackoffBase = 30 * time.Second
	t.Cleanup(func() { connBackoffBase = old })

	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(20 * time.Millisecond); cancel() }()

	start := time.Now()
	err := retryConn(ctx, func(string, ...any) {}, func() {}, "x",
		func(context.Context) error { return errKilled })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v; want context.Canceled", err)
	}
	if d := time.Since(start); d > time.Second {
		t.Errorf("cancelamento demorou %v; o backoff não respeitou o contexto", d)
	}
}

// Contexto já expirado quando a operação falha: não tenta de novo.
func TestRetryConn_ContextoExpiradoNaoRetenta(t *testing.T) {
	shortBackoff(t)
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	err := retryConn(ctx, func(string, ...any) {}, func() {},
		"x", func(context.Context) error {
			calls++
			cancel() // a operação foi interrompida pelo cancelamento
			return errKilled
		})
	if err == nil || calls != 1 {
		t.Errorf("err=%v calls=%d; contexto cancelado não pode gerar nova tentativa", err, calls)
	}
}

func TestIsConnErr(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want bool
	}{
		{nil, false},
		{errKilled, true}, // a mensagem real de produção
		{errors.New("Error op_response:connection shutdown"), true},
		{driver.ErrBadConn, true},
		{fmt.Errorf("listar filiais: %w", driver.ErrBadConn), true},
		{errors.New("read tcp 10.0.0.1:5050: connection reset by peer"), true},
		{errors.New("Dynamic SQL Error SQL error code = -206"), false},
		{errors.New("arithmetic exception, numeric overflow"), false},
	} {
		if got := isConnErr(tc.err); got != tc.want {
			t.Errorf("isConnErr(%v) = %v; want %v", tc.err, got, tc.want)
		}
	}
}
