package api

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
)

// claims mirrors the maestro JWT exactly so a single login works across both
// services. See rps-maestro/internal/api/middleware/jwt_auth.go.
type claims struct {
	UserID int    `json:"user_id"`
	Email  string `json:"email"`
	Role   string `json:"role"`
	jwt.RegisteredClaims
}

// jwtAuth validates a maestro-issued HS256 token. Unlike maestro, it is
// FAIL-CLOSED: an empty secret is a configuration error rejected at boot
// (see cmd/api), so here the secret is always present and always enforced.
func jwtAuth(secret string) gin.HandlerFunc {
	key := []byte(secret)
	return func(c *gin.Context) {
		h := c.GetHeader("Authorization")
		if !strings.HasPrefix(h, "Bearer ") {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "token não fornecido"})
			return
		}
		tokenStr := strings.TrimPrefix(h, "Bearer ")
		cl := &claims{}
		tok, err := jwt.ParseWithClaims(tokenStr, cl, func(t *jwt.Token) (any, error) {
			if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, jwt.ErrSignatureInvalid
			}
			return key, nil
		})
		if err != nil || !tok.Valid {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "token inválido"})
			return
		}
		c.Set("user_id", cl.UserID)
		c.Set("email", cl.Email)
		c.Set("role", cl.Role)
		c.Next()
	}
}

// agentHMAC authenticates the file agent's ingest requests. The agent signs the
// raw request body with a shared secret; we recompute and compare in constant
// time. This avoids exposing AMQP and keeps a single HTTPS port for the agent.
func agentHMAC(secret string) gin.HandlerFunc {
	key := []byte(secret)
	return func(c *gin.Context) {
		sig := c.GetHeader("X-Agent-Signature")
		if sig == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "assinatura ausente"})
			return
		}
		body, err := readAndRestoreBody(c)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "corpo ilegível"})
			return
		}
		mac := hmac.New(sha256.New, key)
		mac.Write(body)
		want := hex.EncodeToString(mac.Sum(nil))
		if !hmac.Equal([]byte(sig), []byte(want)) {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "assinatura inválida"})
			return
		}
		c.Next()
	}
}

// Sign returns the hex HMAC-SHA256 of body with secret — used by the agent and
// by tests to produce a valid X-Agent-Signature.
func Sign(secret string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}
