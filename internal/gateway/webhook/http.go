package webhook

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"go.mau.fi/mautrix-gmessages/internal/gateway/media"
)

const maxResponseBody = 64 * 1024

var ErrUnsafeDestination = errors.New("unsafe webhook destination")

type DestinationGuard interface {
	ClientFor(context.Context, string) (*http.Client, error)
}

type HTTPDeliverer struct{ guard DestinationGuard }

func NewHTTPDeliverer(guard DestinationGuard) (*HTTPDeliverer, error) {
	if guard == nil {
		return nil, ErrUnsafeDestination
	}
	return &HTTPDeliverer{guard: guard}, nil
}

func (deliverer *HTTPDeliverer) Deliver(ctx context.Context, destination string, headers http.Header, body []byte) (HTTPResult, error) {
	client, err := deliverer.guard.ClientFor(ctx, destination)
	if err != nil || client == nil {
		return HTTPResult{}, ErrUnsafeDestination
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, destination, bytes.NewReader(body))
	if err != nil {
		return HTTPResult{}, ErrUnsafeDestination
	}
	request.Header = headers.Clone()
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("User-Agent", "SirenaIX-Webhook/1")
	response, err := client.Do(request)
	if err != nil {
		return HTTPResult{}, err
	}
	defer response.Body.Close()
	// Never retain an untrusted response body. Reading a small cap permits
	// connection reuse for ordinary responses without allowing memory growth.
	_, _ = io.CopyN(io.Discard, response.Body, maxResponseBody+1)
	return HTTPResult{
		StatusCode: response.StatusCode,
		RetryAfter: parseRetryAfter(response.Header.Get("Retry-After"), time.Now()),
	}, nil
}

type PublicDestinationGuard struct{ config media.URLPolicyConfig }

func NewPublicDestinationGuard(config media.URLPolicyConfig) *PublicDestinationGuard {
	return &PublicDestinationGuard{config: config}
}

func (guard *PublicDestinationGuard) ClientFor(ctx context.Context, destination string) (*http.Client, error) {
	parsed, err := url.Parse(destination)
	if err != nil || parsed.Hostname() == "" {
		return nil, ErrUnsafeDestination
	}
	config := guard.config
	config.AllowedHosts = []string{strings.ToLower(strings.TrimSuffix(parsed.Hostname(), "."))}
	policy, err := media.NewURLPolicy(config)
	if err != nil {
		return nil, ErrUnsafeDestination
	}
	target, err := policy.ValidateAndPin(ctx, destination)
	if err != nil {
		return nil, ErrUnsafeDestination
	}
	return policy.Client(target), nil
}

func parseRetryAfter(value string, now time.Time) time.Duration {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	if seconds, err := strconv.ParseInt(value, 10, 64); err == nil {
		if seconds <= 0 {
			return 0
		}
		// Compare in seconds before converting. A syntactically valid int64 can
		// overflow time.Duration during multiplication and otherwise turn a
		// server-requested minimum into a negative retry delay.
		if seconds >= int64(maxRetryAfter/time.Second) {
			return maxRetryAfter
		}
		return time.Duration(seconds) * time.Second
	}
	when, err := http.ParseTime(value)
	if err != nil || !when.After(now) {
		return 0
	}
	return min(when.Sub(now), maxRetryAfter)
}
