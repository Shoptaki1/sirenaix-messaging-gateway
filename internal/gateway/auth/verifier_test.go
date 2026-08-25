package auth

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestOIDCVerifierAcceptsValidSignedTokensAndCommonScopeClaims(t *testing.T) {
	issuer := newTestIssuer(t)
	verifier, err := NewOIDCVerifier(context.Background(), OIDCConfig{
		Issuer: issuer.server.URL, Audience: "sirenaix-api", TenantClaim: "tenant_id",
		HTTPClient: issuer.server.Client(), AllowInsecureLoopbackIssuer: true,
	})
	if err != nil {
		t.Fatalf("NewOIDCVerifier: %v", err)
	}

	for name, scopeClaim := range map[string]any{
		"space delimited": "openid messaging:read messaging:write",
		"array":           []string{"openid", "messaging:read", "messaging:write"},
	} {
		t.Run(name, func(t *testing.T) {
			principal, verifyErr := verifier.Verify(context.Background(), issuer.sign(t, map[string]any{
				"iss": issuer.server.URL, "aud": "sirenaix-api", "sub": "subject-123",
				"tenant_id": "tenant-example", "scope": scopeClaim,
				"exp": time.Now().Add(time.Hour).Unix(), "nbf": time.Now().Add(-time.Minute).Unix(),
			}))
			if verifyErr != nil {
				t.Fatalf("Verify: %v", verifyErr)
			}
			if principal.Subject != "subject-123" || principal.TenantID != "tenant-example" {
				t.Fatalf("unexpected principal: %#v", principal)
			}
			if !principal.HasScope("messaging:read") || !principal.HasScope("messaging:write") {
				t.Fatalf("scopes were not parsed: %#v", principal.Scopes)
			}
		})
	}
}

func TestOIDCVerifierRejectsInvalidIdentityAndTimeClaims(t *testing.T) {
	issuer := newTestIssuer(t)
	verifier, err := NewOIDCVerifier(context.Background(), OIDCConfig{
		Issuer: issuer.server.URL, Audience: "sirenaix-api", TenantClaim: "tenant_id",
		HTTPClient: issuer.server.Client(), AllowInsecureLoopbackIssuer: true,
	})
	if err != nil {
		t.Fatalf("NewOIDCVerifier: %v", err)
	}
	now := time.Now()
	valid := map[string]any{
		"iss": issuer.server.URL, "aud": "sirenaix-api", "sub": "subject-123",
		"tenant_id": "tenant-example", "scope": "messaging:read", "exp": now.Add(time.Hour).Unix(),
	}
	tests := map[string]func(map[string]any){
		"wrong issuer":   func(c map[string]any) { c["iss"] = "https://issuer.invalid.example" },
		"wrong audience": func(c map[string]any) { c["aud"] = "another-api" },
		"expired":        func(c map[string]any) { c["exp"] = now.Add(-time.Hour).Unix() },
		"not before":     func(c map[string]any) { c["nbf"] = now.Add(time.Hour).Unix() },
		"missing tenant": func(c map[string]any) { delete(c, "tenant_id") },
		"blank tenant":   func(c map[string]any) { c["tenant_id"] = "  " },
		"missing subject": func(c map[string]any) {
			delete(c, "sub")
		},
		"blank subject": func(c map[string]any) { c["sub"] = "  " },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			claims := cloneClaims(t, valid)
			mutate(claims)
			if _, verifyErr := verifier.Verify(context.Background(), issuer.sign(t, claims)); verifyErr == nil {
				t.Fatal("Verify unexpectedly accepted token")
			}
		})
	}
}

func TestOIDCVerifierRejectsTokensWithAnotherSignature(t *testing.T) {
	issuer := newTestIssuer(t)
	otherIssuer := newTestIssuer(t)
	verifier, err := NewOIDCVerifier(context.Background(), OIDCConfig{
		Issuer: issuer.server.URL, Audience: "sirenaix-api", HTTPClient: issuer.server.Client(), AllowInsecureLoopbackIssuer: true,
	})
	if err != nil {
		t.Fatalf("NewOIDCVerifier: %v", err)
	}
	token := otherIssuer.sign(t, map[string]any{
		"iss": issuer.server.URL, "aud": "sirenaix-api", "sub": "subject-123",
		"tenant_id": "tenant-example", "exp": time.Now().Add(time.Hour).Unix(),
	})
	if _, verifyErr := verifier.Verify(context.Background(), token); verifyErr == nil {
		t.Fatal("Verify unexpectedly accepted a token signed by another key")
	}
}

func TestOIDCVerifierConstructorFailsClosed(t *testing.T) {
	if _, err := NewOIDCVerifier(context.Background(), OIDCConfig{}); err == nil {
		t.Fatal("empty configuration unexpectedly succeeded")
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	if _, err := NewOIDCVerifier(context.Background(), OIDCConfig{
		Issuer: server.URL, Audience: "sirenaix-api", HTTPClient: server.Client(), AllowInsecureLoopbackIssuer: true,
	}); err == nil {
		t.Fatal("failed discovery unexpectedly succeeded")
	}
}

func TestOIDCVerifierRejectsUnsafeIssuerURLsBeforeDiscovery(t *testing.T) {
	tests := []struct {
		name      string
		issuer    string
		allowHTTP bool
	}{
		{name: "public HTTP", issuer: "http://issuer.example"},
		{name: "non-loopback HTTP opt-in", issuer: "http://192.0.2.10", allowHTTP: true},
		{name: "HTTP hostname opt-in", issuer: "http://issuer.example", allowHTTP: true},
		{name: "userinfo", issuer: "https://user:password@issuer.example"},
		{name: "query", issuer: "https://issuer.example?tenant=example"},
		{name: "fragment", issuer: "https://issuer.example#fragment"},
		{name: "unsupported scheme", issuer: "ftp://issuer.example"},
		{name: "missing host", issuer: "https:///issuer"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			transport := &countingTransport{}
			_, err := NewOIDCVerifier(context.Background(), OIDCConfig{
				Issuer: test.issuer, Audience: "sirenaix-api",
				AllowInsecureLoopbackIssuer: test.allowHTTP,
				HTTPClient:                  &http.Client{Transport: transport},
			})
			if !errors.Is(err, ErrInvalidConfiguration) {
				t.Fatalf("NewOIDCVerifier() error = %v, want ErrInvalidConfiguration", err)
			}
			if transport.calls != 0 {
				t.Fatalf("unsafe issuer attempted %d discovery requests", transport.calls)
			}
		})
	}
}

type countingTransport struct{ calls int }

func (transport *countingTransport) RoundTrip(*http.Request) (*http.Response, error) {
	transport.calls++
	return nil, errors.New("unexpected discovery request")
}

type testIssuer struct {
	server *httptest.Server
	key    *rsa.PrivateKey
}

func newTestIssuer(t *testing.T) *testIssuer {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate RSA key: %v", err)
	}
	fixture := &testIssuer{key: key}
	fixture.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/.well-known/openid-configuration":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"issuer": fixture.server.URL, "jwks_uri": fixture.server.URL + "/jwks",
				"authorization_endpoint":                fixture.server.URL + "/authorize",
				"token_endpoint":                        fixture.server.URL + "/token",
				"id_token_signing_alg_values_supported": []string{"RS256"},
			})
		case "/jwks":
			_ = json.NewEncoder(w).Encode(map[string]any{"keys": []any{map[string]any{
				"kty": "RSA", "kid": "fixture-key", "use": "sig", "alg": "RS256",
				"n": base64.RawURLEncoding.EncodeToString(key.N.Bytes()),
				"e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.E)).Bytes()),
			}}})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(fixture.server.Close)
	return fixture
}

func (issuer *testIssuer) sign(t *testing.T, claims map[string]any) string {
	t.Helper()
	header, err := json.Marshal(map[string]any{"alg": "RS256", "kid": "fixture-key", "typ": "JWT"})
	if err != nil {
		t.Fatalf("marshal header: %v", err)
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		t.Fatalf("marshal claims: %v", err)
	}
	unsigned := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(payload)
	digest := sha256.Sum256([]byte(unsigned))
	signature, err := rsa.SignPKCS1v15(rand.Reader, issuer.key, crypto.SHA256, digest[:])
	if err != nil {
		t.Fatalf("sign token: %v", err)
	}
	return fmt.Sprintf("%s.%s", unsigned, base64.RawURLEncoding.EncodeToString(signature))
}

func cloneClaims(t *testing.T, input map[string]any) map[string]any {
	t.Helper()
	encoded, err := json.Marshal(input)
	if err != nil {
		t.Fatalf("marshal claims: %v", err)
	}
	var output map[string]any
	if err := json.Unmarshal(encoded, &output); err != nil {
		t.Fatalf("unmarshal claims: %v", err)
	}
	return output
}

func TestPrincipalScopeMatchingIsExact(t *testing.T) {
	principal := Principal{Scopes: []string{"messaging:reader", " messaging:write "}}
	if principal.HasScope("messaging:read") {
		t.Fatal("prefix scope unexpectedly matched")
	}
	if principal.HasScope("messaging:write") {
		t.Fatal("un-normalized scope unexpectedly matched")
	}
	if !strings.Contains(fmt.Sprint(principal.Scopes), "messaging:reader") {
		t.Fatal("fixture is invalid")
	}
}
