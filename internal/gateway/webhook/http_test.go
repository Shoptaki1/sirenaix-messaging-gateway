package webhook_test

import (
	"context"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"go.mau.fi/mautrix-gmessages/internal/gateway/webhook"
)

type destinationGuard struct {
	client *http.Client
	err    error
}

func (guard destinationGuard) ClientFor(context.Context, string) (*http.Client, error) {
	return guard.client, guard.err
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (roundTrip roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return roundTrip(request)
}

func TestHTTPDelivererPostsExactBodyWithCappedResponseAndRetryAfter(t *testing.T) {
	wantBody := []byte("{\"event_id\":\"event-a\"}")
	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Fatal(err)
		}
		if string(body) != string(wantBody) || request.Method != http.MethodPost || request.Header.Get("Content-Type") != "application/json" {
			t.Fatalf("request = %s %q %v", request.Method, body, request.Header)
		}
		return &http.Response{
			StatusCode: http.StatusTooManyRequests,
			Header:     http.Header{"Retry-After": []string{strconv.FormatInt(math.MaxInt64, 10)}},
			Body:       io.NopCloser(strings.NewReader(strings.Repeat("x", 128*1024))),
		}, nil
	})}
	deliverer, err := webhook.NewHTTPDeliverer(destinationGuard{client: client})
	if err != nil {
		t.Fatal(err)
	}
	result, err := deliverer.Deliver(context.Background(), "https://hooks.example/receive", http.Header{"X-Test": []string{"a"}}, wantBody)
	if err != nil {
		t.Fatal(err)
	}
	if result.StatusCode != http.StatusTooManyRequests || result.RetryAfter != time.Hour {
		t.Fatalf("result = %+v", result)
	}
}

func TestHTTPDelivererRejectsUnsafeDestinationBeforeRequest(t *testing.T) {
	guard := destinationGuard{err: webhook.ErrUnsafeDestination}
	deliverer, _ := webhook.NewHTTPDeliverer(guard)
	if _, err := deliverer.Deliver(context.Background(), "https://127.0.0.1/hook", nil, []byte("{}")); err == nil {
		t.Fatal("Deliver accepted unsafe destination")
	}
}
