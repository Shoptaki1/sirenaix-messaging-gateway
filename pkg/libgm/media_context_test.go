package libgm

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

type mediaPolicyFunc func(context.Context, string) (HTTPDoer, error)

func (function mediaPolicyFunc) ClientFor(ctx context.Context, rawURL string) (HTTPDoer, error) {
	return function(ctx, rawURL)
}

type mediaDoerFunc func(*http.Request) (*http.Response, error)

func (function mediaDoerFunc) Do(request *http.Request) (*http.Response, error) {
	return function(request)
}

func TestContextMediaMethodsFailClosedWithoutPolicyAndBeforeOversizeRead(t *testing.T) {
	client := NewClient(NewAuthData(), nil, zerolog.Nop())
	if _, err := client.UploadMediaContext(context.Background(), strings.NewReader("data"), 4, "a.png", "image/png", 16); err != ErrMediaPolicyRequired {
		t.Fatalf("UploadMediaContext policy error = %v", err)
	}
	called := false
	client.SetMediaRequestPolicy(mediaPolicyFunc(func(context.Context, string) (HTTPDoer, error) {
		called = true
		return mediaDoerFunc(func(*http.Request) (*http.Response, error) { return nil, nil }), nil
	}))
	if _, err := client.UploadMediaContext(context.Background(), bytes.NewReader(bytes.Repeat([]byte{1}, 17)), -1, "a.png", "image/png", 16); err != ErrMediaTooLarge {
		t.Fatalf("oversize upload error = %v", err)
	}
	if called {
		t.Fatal("oversize upload reached provider HTTP policy")
	}
}

func TestContextMediaDownloadCapsProviderResponseAndUsesPolicy(t *testing.T) {
	client := NewClient(NewAuthData(), nil, zerolog.Nop())
	requestedURL := ""
	client.SetMediaRequestPolicy(mediaPolicyFunc(func(_ context.Context, rawURL string) (HTTPDoer, error) {
		requestedURL = rawURL
		return mediaDoerFunc(func(request *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK, Header: make(http.Header), Request: request,
				Body: io.NopCloser(bytes.NewReader(bytes.Repeat([]byte{1}, 64))), ContentLength: 64,
			}, nil
		}), nil
	}))
	if _, err := client.DownloadMediaContext(context.Background(), "media-a", bytes.Repeat([]byte{2}, 32), 8); err != ErrMediaTooLarge {
		t.Fatalf("DownloadMediaContext error = %v", err)
	}
	if requestedURL == "" || !strings.HasPrefix(requestedURL, "https://") {
		t.Fatalf("policy URL = %q", requestedURL)
	}
}

func TestContextMediaPreservesUnsafeURLPolicyFailure(t *testing.T) {
	unsafe := errors.New("unsafe pinned provider URL")
	client := NewClient(NewAuthData(), nil, zerolog.Nop())
	client.SetMediaRequestPolicy(mediaPolicyFunc(func(context.Context, string) (HTTPDoer, error) {
		return nil, unsafe
	}))
	if _, err := client.DownloadMediaContext(context.Background(), "media-a", bytes.Repeat([]byte{2}, 32), 8); !errors.Is(err, unsafe) {
		t.Fatalf("DownloadMediaContext error = %v, want policy failure", err)
	}
}
