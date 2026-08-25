package auth

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"

	"github.com/coreos/go-oidc/v3/oidc"
	"go.mau.fi/mautrix-gmessages/internal/gateway/domain"
)

var (
	ErrInvalidConfiguration = errors.New("invalid authentication configuration")
	ErrInvalidToken         = errors.New("invalid authentication token")
)

type Principal struct {
	Subject  string
	TenantID domain.TenantID
	Scopes   []string
}

func (principal Principal) HasScope(required string) bool {
	for _, scope := range principal.Scopes {
		if scope == required {
			return true
		}
	}
	return false
}

type Verifier interface {
	Verify(ctx context.Context, rawToken string) (Principal, error)
}

type OIDCConfig struct {
	Issuer                      string
	Audience                    string
	TenantClaim                 string
	HTTPClient                  *http.Client
	AllowInsecureLoopbackIssuer bool
}

type OIDCVerifier struct {
	verifier    *oidc.IDTokenVerifier
	tenantClaim string
}

func NewOIDCVerifier(ctx context.Context, config OIDCConfig) (*OIDCVerifier, error) {
	issuer := strings.TrimSpace(config.Issuer)
	audience := strings.TrimSpace(config.Audience)
	parsedIssuer, err := url.Parse(issuer)
	if err != nil || audience == "" || !validIssuerURL(parsedIssuer, config.AllowInsecureLoopbackIssuer) {
		return nil, ErrInvalidConfiguration
	}
	tenantClaim := strings.TrimSpace(config.TenantClaim)
	if tenantClaim == "" {
		tenantClaim = "tenant_id"
	}
	if strings.ContainsAny(tenantClaim, " \t\r\n") {
		return nil, ErrInvalidConfiguration
	}
	if config.HTTPClient != nil {
		ctx = oidc.ClientContext(ctx, config.HTTPClient)
	}
	provider, err := oidc.NewProvider(ctx, issuer)
	if err != nil {
		return nil, fmt.Errorf("%w: OIDC discovery failed", ErrInvalidConfiguration)
	}
	return &OIDCVerifier{
		verifier:    provider.VerifierContext(ctx, &oidc.Config{ClientID: audience}),
		tenantClaim: tenantClaim,
	}, nil
}

func validIssuerURL(issuer *url.URL, allowInsecureLoopback bool) bool {
	if issuer == nil || issuer.Host == "" || issuer.Opaque != "" || issuer.User != nil ||
		issuer.RawQuery != "" || issuer.ForceQuery || issuer.Fragment != "" {
		return false
	}
	switch strings.ToLower(issuer.Scheme) {
	case "https":
		return true
	case "http":
		if !allowInsecureLoopback {
			return false
		}
		hostname := issuer.Hostname()
		if strings.EqualFold(hostname, "localhost") {
			return true
		}
		address := net.ParseIP(hostname)
		return address != nil && address.IsLoopback()
	default:
		return false
	}
}

func (verifier *OIDCVerifier) Verify(ctx context.Context, rawToken string) (Principal, error) {
	if verifier == nil || verifier.verifier == nil || strings.TrimSpace(rawToken) == "" {
		return Principal{}, ErrInvalidToken
	}
	token, err := verifier.verifier.Verify(ctx, rawToken)
	if err != nil {
		return Principal{}, ErrInvalidToken
	}
	subject := strings.TrimSpace(token.Subject)
	if subject == "" {
		return Principal{}, ErrInvalidToken
	}
	var claims map[string]json.RawMessage
	if err := token.Claims(&claims); err != nil {
		return Principal{}, ErrInvalidToken
	}
	var tenant string
	if rawTenant, ok := claims[verifier.tenantClaim]; !ok || json.Unmarshal(rawTenant, &tenant) != nil || strings.TrimSpace(tenant) == "" {
		return Principal{}, ErrInvalidToken
	}
	scopes, err := claimScopes(claims)
	if err != nil {
		return Principal{}, ErrInvalidToken
	}
	return Principal{Subject: subject, TenantID: domain.TenantID(strings.TrimSpace(tenant)), Scopes: scopes}, nil
}

func claimScopes(claims map[string]json.RawMessage) ([]string, error) {
	seen := make(map[string]struct{})
	var scopes []string
	for _, name := range []string{"scope", "scp"} {
		raw, exists := claims[name]
		if !exists || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			continue
		}
		var text string
		if err := json.Unmarshal(raw, &text); err == nil {
			for _, scope := range strings.Fields(text) {
				if _, exists := seen[scope]; !exists {
					seen[scope] = struct{}{}
					scopes = append(scopes, scope)
				}
			}
			continue
		}
		var array []string
		if err := json.Unmarshal(raw, &array); err != nil {
			return nil, err
		}
		for _, candidate := range array {
			for _, scope := range strings.Fields(candidate) {
				if _, exists := seen[scope]; !exists {
					seen[scope] = struct{}{}
					scopes = append(scopes, scope)
				}
			}
		}
	}
	return scopes, nil
}
