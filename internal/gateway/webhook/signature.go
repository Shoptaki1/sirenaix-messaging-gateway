package webhook

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"strconv"
	"strings"
	"time"
)

const signaturePrefix = "v1="

// Sign authenticates the exact stored body. Callers must not re-encode the
// event between signing attempts.
func Sign(secret []byte, timestamp time.Time, eventID string, body []byte) string {
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write([]byte(strconv.FormatInt(timestamp.Unix(), 10)))
	_, _ = mac.Write([]byte("."))
	_, _ = mac.Write([]byte(eventID))
	_, _ = mac.Write([]byte("."))
	_, _ = mac.Write(body)
	return signaturePrefix + hex.EncodeToString(mac.Sum(nil))
}

// Verify compares the expected HMAC in constant time. Invalid versions and
// encodings are rejected before the comparison.
func Verify(secret []byte, timestamp time.Time, eventID string, body []byte, signature string) bool {
	if !strings.HasPrefix(signature, signaturePrefix) {
		return false
	}
	provided, err := hex.DecodeString(strings.TrimPrefix(signature, signaturePrefix))
	if err != nil || len(provided) != sha256.Size {
		return false
	}
	expected := Sign(secret, timestamp, eventID, body)
	expectedBytes, _ := hex.DecodeString(strings.TrimPrefix(expected, signaturePrefix))
	return subtle.ConstantTimeCompare(provided, expectedBytes) == 1
}
