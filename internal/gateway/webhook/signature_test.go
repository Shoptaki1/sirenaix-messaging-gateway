package webhook_test

import (
	"testing"
	"time"

	"go.mau.fi/mautrix-gmessages/internal/gateway/webhook"
)

func TestSignatureUsesTimestampEventIDAndExactBody(t *testing.T) {
	secret := []byte("test-webhook-secret")
	timestamp := time.Unix(1700000000, 0).UTC()
	body := []byte("{\"b\":2,\"a\":1}\n")
	signature := webhook.Sign(secret, timestamp, "event-a", body)
	const want = "v1=a65538f420f38aa729fd2d23c95e6dba99ea271d15365fea77133b96c559e4c0"
	if signature != want {
		t.Fatalf("Sign() = %q, want %q", signature, want)
	}
	if !webhook.Verify(secret, timestamp, "event-a", body, signature) {
		t.Fatal("Verify() rejected exact body")
	}
	for _, mutation := range []struct {
		eventID, signature string
		body               []byte
	}{
		{eventID: "event-b", body: body, signature: signature},
		{eventID: "event-a", body: []byte("{\"a\":1,\"b\":2}\n"), signature: signature},
		{eventID: "event-a", body: body, signature: "v1=a65538f420f38aa729fd2d23c95e6dba99ea271d15365fea77133b96c559e4c00"},
	} {
		if webhook.Verify(secret, timestamp, mutation.eventID, mutation.body, mutation.signature) {
			t.Fatalf("Verify accepted mutation %+v", mutation)
		}
	}
}
