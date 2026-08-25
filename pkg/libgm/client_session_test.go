package libgm

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"go.mau.fi/mautrix-gmessages/pkg/libgm/gmproto"
)

func TestAuthDataFormattingIsRedacted(t *testing.T) {
	const sentinel = "auth-format-secret-sentinel"
	auth := NewAuthData()
	auth.SetCookies(map[string]string{"SID": sentinel})
	auth.SetTachyonAuth([]byte(sentinel), time.Now().Add(time.Hour), int64(time.Hour))

	for _, formatted := range []string{fmt.Sprintf("%v", auth), fmt.Sprintf("%+v", auth), fmt.Sprintf("%#v", auth)} {
		if strings.Contains(formatted, sentinel) {
			t.Fatalf("AuthData formatting exposed session material: %s", formatted)
		}
	}
}

func TestClearSessionSecretsClearsAllSessionFields(t *testing.T) {
	auth := NewAuthData()
	auth.SetCookies(map[string]string{"SID": "secret"})
	auth.SetDevices(&gmproto.Device{SourceID: "browser"}, &gmproto.Device{SourceID: "mobile"})
	auth.SetTachyonAuth([]byte("token"), time.Now().Add(time.Hour), int64(time.Hour))
	auth.SetSessionIdentifiers(uuid.New(), uuid.New(), uuid.New())
	auth.SetWebEncryptionKey([]byte("web-key"))
	client := NewClient(auth, &PushKeys{URL: "https://push.invalid", P256DH: []byte("push-key"), Auth: []byte("push-auth")}, zerolog.Nop())

	client.ClearSessionSecrets()
	snapshot, push := client.SnapshotSession()
	if snapshot == nil {
		t.Fatal("snapshot is nil")
	}
	if snapshot.RequestCrypto != nil || snapshot.RefreshKey != nil || snapshot.Browser != nil || snapshot.Mobile != nil ||
		len(snapshot.TachyonAuthToken) != 0 || !snapshot.TachyonExpiry.IsZero() || snapshot.TachyonTTL != 0 ||
		len(snapshot.WebEncryptionKey) != 0 || snapshot.SessionID != uuid.Nil || snapshot.DestRegID != uuid.Nil ||
		snapshot.PairingID != uuid.Nil || len(snapshot.Cookies) != 0 || push != nil {
		t.Fatalf("session fields remain after clear: %#v, push=%#v", snapshot, push)
	}
}

func TestClearSessionSecretsClearsPushKeysWhenAuthDataIsNil(t *testing.T) {
	p256dh := []byte("push-key-secret")
	auth := []byte("push-auth-secret")
	client := NewClient(nil, &PushKeys{URL: "https://push.invalid", P256DH: p256dh, Auth: auth}, zerolog.Nop())
	storedP256DH := client.PushKeys.P256DH
	storedAuth := client.PushKeys.Auth

	client.ClearSessionSecrets()

	if client.PushKeys != nil {
		t.Fatal("push keys remain attached to nil-auth client")
	}
	for _, value := range append(storedP256DH, storedAuth...) {
		if value != 0 {
			t.Fatal("push key bytes were not cleared")
		}
	}
}
