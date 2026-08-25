//go:build postgres_integration

package libgm

import "net/http"

// SetPostgresIntegrationTransport replaces provider HTTP only in tagged,
// deterministic integration tests. It is intentionally absent from normal
// builds so production callers cannot bypass Client transport policy.
func (c *Client) SetPostgresIntegrationTransport(transport http.RoundTripper) {
	c.http = &http.Client{Transport: transport}
}
