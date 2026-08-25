package httpapi

import (
	"net/http"
	"testing"
	"time"
)

func TestNewServerHasExplicitResourceTimeoutsAndHeaderLimit(t *testing.T) {
	handler := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
	server, err := NewServer("127.0.0.1:8080", handler)
	if err != nil {
		t.Fatal(err)
	}
	if server.Addr != "127.0.0.1:8080" || server.Handler == nil {
		t.Fatalf("server = %+v", server)
	}
	if server.ReadHeaderTimeout <= 0 || server.ReadHeaderTimeout > 10*time.Second ||
		server.ReadTimeout <= 0 || server.WriteTimeout <= 0 || server.IdleTimeout <= 0 {
		t.Fatalf("timeouts are not bounded: read_header=%s read=%s write=%s idle=%s",
			server.ReadHeaderTimeout, server.ReadTimeout, server.WriteTimeout, server.IdleTimeout)
	}
	if server.MaxHeaderBytes <= 0 || server.MaxHeaderBytes > 64<<10 {
		t.Fatalf("MaxHeaderBytes = %d", server.MaxHeaderBytes)
	}
	if _, err = NewServer("", handler); err == nil {
		t.Fatal("empty address accepted")
	}
	if _, err = NewServer("127.0.0.1:8080", nil); err == nil {
		t.Fatal("nil handler accepted")
	}
}
