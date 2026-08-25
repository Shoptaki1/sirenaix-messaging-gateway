package libgm

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/rs/zerolog"

	"go.mau.fi/mautrix-gmessages/pkg/libgm/gmproto"
)

func TestHTTPProviderErrorLoggingDoesNotIncludeRawBody(t *testing.T) {
	const secret = "provider-private-cookie-marker"
	const urlSecret = "provider-url-secret-marker"
	var logs bytes.Buffer
	logger := zerolog.New(&logs)
	request := (&http.Request{Method: http.MethodPost, URL: mustURL(t, "https://messages.google.com/example?credential="+urlSecret)}).WithContext(logger.WithContext(context.Background()))
	response := &http.Response{
		StatusCode: http.StatusUnauthorized,
		Header:     http.Header{"Content-Type": []string{"text/plain"}},
		Body:       io.NopCloser(strings.NewReader(secret)),
		Request:    request,
	}
	_, _ = typedHTTPResponse[*gmproto.Config](response, nil)
	if strings.Contains(logs.String(), secret) || strings.Contains(logs.String(), base64.StdEncoding.EncodeToString([]byte(secret))) || strings.Contains(logs.String(), urlSecret) {
		t.Fatalf("provider response or URL leaked to logs: %s", logs.String())
	}
}

func TestHTTPProviderErrorNeverRetainsRequestHeadersOrRawBody(t *testing.T) {
	const cookie = "outbound-cookie-sentinel"
	const token = "outbound-token-sentinel"
	const rawBody = "provider-raw-body-sentinel"
	request := (&http.Request{Method: http.MethodPost, URL: mustURL(t, "https://messages.google.com/example")}).WithContext(context.Background())
	request.Header = http.Header{"Cookie": []string{cookie}, "Authorization": []string{token}}
	response := &http.Response{
		StatusCode: http.StatusUnauthorized,
		Header:     http.Header{"Content-Type": []string{"text/plain"}},
		Body:       io.NopCloser(strings.NewReader(rawBody)),
		Request:    request,
	}
	_, providerErr := typedHTTPResponse[*gmproto.Config](response, nil)
	if providerErr == nil {
		t.Fatal("expected provider error")
	}
	wrapped := fmt.Errorf("nested provider failure: %w", providerErr)
	encoded, marshalErr := json.Marshal(providerErr)
	if marshalErr != nil {
		t.Fatalf("Marshal error: %v", marshalErr)
	}
	outputs := []string{
		fmt.Sprintf("%v", providerErr), fmt.Sprintf("%+v", providerErr), fmt.Sprintf("%#v", providerErr),
		fmt.Sprintf("%v", wrapped), fmt.Sprintf("%+v", wrapped), fmt.Sprintf("%#v", wrapped), string(encoded),
	}
	for _, output := range outputs {
		for _, secret := range []string{cookie, token, rawBody} {
			if strings.Contains(output, secret) {
				t.Fatalf("provider error exposed %q in %q", secret, output)
			}
		}
	}
}

func TestHTTPProviderResponseIsBounded(t *testing.T) {
	reader := &finiteFiller{remaining: maxProviderResponseBytes + 2}
	request := (&http.Request{Method: http.MethodPost, URL: mustURL(t, "https://messages.google.com/example")}).WithContext(context.Background())
	response := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{ContentTypeProtobuf}},
		Body:       io.NopCloser(reader),
		Request:    request,
	}
	if _, err := typedHTTPResponse[*gmproto.Config](response, nil); err != errProviderResponseTooLarge {
		t.Fatalf("typedHTTPResponse error = %v, want %v", err, errProviderResponseTooLarge)
	}
	if reader.read > maxProviderResponseBytes+1 {
		t.Fatalf("provider body read %d bytes past cap", reader.read)
	}
}

func TestLongPollErrorsNeverLogRawURLOrKeyMaterialAtAnyEnabledLevel(t *testing.T) {
	const sentinel = "https://provider.invalid/media?key=decrypted-key-sentinel"
	for _, level := range []zerolog.Level{zerolog.TraceLevel, zerolog.DebugLevel, zerolog.WarnLevel} {
		t.Run(level.String(), func(t *testing.T) {
			var logs bytes.Buffer
			logger := zerolog.New(&logs).Level(level)
			client := &Client{}
			client.readLongPollContext(context.Background(), &logger, &openingThenError{err: errors.New(sentinel)}, false)
			if strings.Contains(logs.String(), sentinel) || strings.Contains(logs.String(), "decrypted-key-sentinel") {
				t.Fatalf("provider URL/key material leaked at %s: %s", level, logs.String())
			}
		})
	}
}

type openingThenError struct {
	opened bool
	err    error
}

func (reader *openingThenError) Read(destination []byte) (int, error) {
	if !reader.opened {
		reader.opened = true
		copy(destination, "[[")
		return 2, nil
	}
	return 0, reader.err
}

func (*openingThenError) Close() error { return nil }

type finiteFiller struct {
	remaining int64
	read      int64
}

func (reader *finiteFiller) Read(destination []byte) (int, error) {
	if reader.remaining == 0 {
		return 0, io.EOF
	}
	count := min(int64(len(destination)), reader.remaining)
	for index := int64(0); index < count; index++ {
		destination[index] = 'x'
	}
	reader.remaining -= count
	reader.read += count
	return int(count), nil
}

func mustURL(t *testing.T, value string) *url.URL {
	t.Helper()
	parsed, err := url.Parse(value)
	if err != nil {
		t.Fatalf("url.Parse: %v", err)
	}
	return parsed
}
