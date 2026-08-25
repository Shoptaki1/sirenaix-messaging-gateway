package httpapi

import (
	"errors"
	"net/http"
	"strings"
	"time"
)

var ErrInvalidServerConfig = errors.New("invalid HTTP server configuration")

// NewServer returns the production-safe baseline used by gateway binaries.
// Callers still own listener lifecycle and graceful Shutdown.
func NewServer(address string, handler http.Handler) (*http.Server, error) {
	if strings.TrimSpace(address) == "" || handler == nil {
		return nil, ErrInvalidServerConfig
	}
	return &http.Server{
		Addr:              address,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       60 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    32 << 10,
	}, nil
}
