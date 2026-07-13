package firebird

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"time"
)

// connAttempts/connBackoffBase dimensionam o retry de conexão: 3 tentativas com
// backoff linear curto (2s, 4s) cabem folgadas num ciclo do syncer e nunca viram
// loop infinito. connBackoffBase é var para os testes encurtarem a espera.
const connAttempts = 3

var connBackoffBase = 2 * time.Second

// isConnErr classifica erros TRANSITÓRIOS de conexão. O caso que motivou isto:
// a manutenção do Firebird mata sessões ociosas ("connection shutdown / Killed
// by database administrator") e o aviso chega como resposta NORMAL de erro — o
// driver não devolve driver.ErrBadConn (só o faz em falha de escrita no socket),
// então o database/sql repõe a conexão morta no pool e o erro sobe ao chamador.
// Erros de SQL/dados não são transitórios e devem retornar imediatamente.
func isConnErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, driver.ErrBadConn) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var ne net.Error
	if errors.As(err, &ne) {
		return true
	}
	msg := strings.ToLower(err.Error())
	for _, probe := range []string{
		"connection shutdown",              // sessão encerrada pelo servidor Firebird
		"killed by database administrator", // idem (gfix/delete em MON$ATTACHMENTS)
		"connection reset",
		"broken pipe",
		"connection was closed",
		"bad connection",
	} {
		if strings.Contains(msg, probe) {
			return true
		}
	}
	return false
}

// retryConn executa op tolerando conexões derrubadas pelo servidor: em erro de
// conexão (isConnErr), descarta as conexões do pool via flush — é assim que a
// conexão morta é invalidada, já que o driver não a marca como ruim nesse
// cenário —, espera um backoff curto respeitando o contexto e tenta de novo,
// até connAttempts vezes. Nunca transforma falha real em sucesso: erro
// não-transitório retorna na hora e o esgotamento devolve o último erro.
func retryConn(ctx context.Context, logf func(string, ...any), flush func(), name string, op func(context.Context) error) error {
	var err error
	for attempt := 1; ; attempt++ {
		if err = op(ctx); err == nil {
			if attempt > 1 {
				logf("firebird: %s recuperado (tentativa %d/%d)", name, attempt, connAttempts)
			}
			return nil
		}
		if ctx.Err() != nil || !isConnErr(err) {
			return err
		}
		if attempt >= connAttempts {
			return fmt.Errorf("firebird: %s: %d tentativas de reconexão esgotadas: %w", name, connAttempts, err)
		}
		logf("firebird: %s: conexão inválida (tentativa %d/%d): %v — descartando conexões do pool e tentando de novo",
			name, attempt, connAttempts, err)
		flush()
		t := time.NewTimer(time.Duration(attempt) * connBackoffBase)
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-t.C:
		}
	}
}
