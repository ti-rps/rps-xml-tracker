// Package signing holds the shared HMAC scheme used to authenticate the agent's
// ingest requests. Kept dependency-free so the agent binary doesn't pull in the
// HTTP framework.
package signing

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
)

// Sign returns the hex HMAC-SHA256 of body with secret. Both the agent (when
// sending) and the API (when verifying) compute this over the raw request body.
func Sign(secret string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}
