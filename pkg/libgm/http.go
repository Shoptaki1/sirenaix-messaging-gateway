package libgm

import (
	"bytes"
	"context"
	"crypto/sha1"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/util/pblite"
	"google.golang.org/protobuf/proto"

	"go.mau.fi/mautrix-gmessages/pkg/libgm/events"
	"go.mau.fi/mautrix-gmessages/pkg/libgm/gmproto"
	"go.mau.fi/mautrix-gmessages/pkg/libgm/util"
)

const ContentTypeProtobuf = "application/x-protobuf"
const ContentTypePBLite = "application/json+protobuf"

const ServerErrorMaxAttempts = 3
const ServerErrorRetryInterval = 1 * time.Second

const maxProviderResponseBytes = int64(16 << 20)

var errProviderResponseTooLarge = errors.New("provider response exceeds limit")

func (c *Client) makeProtobufHTTPRequest(url string, data proto.Message, contentType string) (*http.Response, error) {
	ctx := c.Logger.WithContext(context.TODO())
	return c.makeProtobufHTTPRequestContext(ctx, url, data, contentType, false)
}

func (c *Client) makeProtobufHTTPRequestContext(ctx context.Context, url string, data proto.Message, contentType string, longPoll bool) (*http.Response, error) {
	return c.makeProtobufHTTPRequestContextPolicy(ctx, url, data, contentType, longPoll, true)
}

func (c *Client) makeProtobufHTTPRequestContextNoRetry(ctx context.Context, url string, data proto.Message, contentType string) (*http.Response, error) {
	return c.makeProtobufHTTPRequestContextPolicy(ctx, url, data, contentType, false, false)
}

func (c *Client) makeProtobufHTTPRequestContextPolicy(ctx context.Context, url string, data proto.Message, contentType string, longPoll, retryServerErrors bool) (*http.Response, error) {
	var body []byte
	var err error
	switch contentType {
	case ContentTypeProtobuf:
		body, err = proto.Marshal(data)
	case ContentTypePBLite:
		body, err = pblite.Marshal(data)
	default:
		return nil, fmt.Errorf("unknown request content type %s", contentType)
	}
	if err != nil {
		return nil, err
	}
	client := c.http
	if longPoll {
		client = c.lphttp
	}
	for attempt := 1; ; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		util.BuildRelayHeaders(req, contentType, "*/*")
		c.AuthData.AddCookiesToRequest(req)
		res, reqErr := client.Do(req)
		if reqErr != nil {
			return res, reqErr
		}
		c.AuthData.UpdateCookiesFromResponse(res)
		if len(res.Cookies()) > 0 {
			c.emitLifecycleActivity(lifecycleActivitySessionChange)
		}
		if longPoll || !retryServerErrors || res.StatusCode < 500 || attempt >= ServerErrorMaxAttempts {
			return res, nil
		}
		retryIn := time.Duration(attempt) * ServerErrorRetryInterval
		zerolog.Ctx(ctx).Debug().
			Int("status_code", res.StatusCode).
			Int("attempt", attempt).
			Stringer("retry_in", retryIn).
			Msg("Server error from Google Messages, retrying in a while")
		_ = res.Body.Close()
		select {
		case <-time.After(retryIn):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func SAPISIDHash(origin, sapisid string) string {
	ts := time.Now().Unix()
	hash := sha1.Sum([]byte(fmt.Sprintf("%d %s %s", ts, sapisid, origin)))
	return fmt.Sprintf("SAPISIDHASH %d_%x", ts, hash[:])
}

func decodeProtoResp(body []byte, contentType string, into proto.Message) error {
	contentType, _, err := mime.ParseMediaType(contentType)
	if err != nil {
		return fmt.Errorf("failed to parse content-type: %w", err)
	}
	switch contentType {
	case ContentTypeProtobuf:
		return proto.Unmarshal(body, into)
	case ContentTypePBLite, "text/plain":
		return pblite.Unmarshal(body, into)
	default:
		return fmt.Errorf("unknown content type %s in response", contentType)
	}
}

func typedHTTPResponse[T proto.Message](resp *http.Response, err error) (parsed T, retErr error) {
	if err != nil {
		retErr = err
		return
	}
	defer resp.Body.Close()
	requestContext := context.Background()
	if resp.Request != nil {
		requestContext = resp.Request.Context()
	}
	body, err := readProviderResponseBounded(requestContext, resp.Body, maxProviderResponseBytes)
	if err != nil {
		if errors.Is(err, errProviderResponseTooLarge) {
			retErr = errProviderResponseTooLarge
		} else {
			retErr = errors.New("failed to read provider response")
		}
		return
	}
	defer zeroBytes(body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		logEvt := zerolog.Ctx(requestContext).Debug().
			Int("status_code", resp.StatusCode)
		httpErr := events.HTTPError{StatusCode: resp.StatusCode, Classification: classifyHTTPStatus(resp.StatusCode)}
		retErr = httpErr
		var errorResp gmproto.ErrorResponse
		errErr := decodeProtoResp(body, resp.Header.Get("Content-Type"), &errorResp)
		if errErr == nil && errorResp.Type != 0 {
			logEvt = logEvt.Int64("provider_error_type", errorResp.Type)
			retErr = events.RequestError{
				HTTP: &httpErr,
				Data: &gmproto.ErrorResponse{Type: errorResp.Type},
			}
		} else {
			logEvt = logEvt.Bool("parsed_provider_error", false)
		}
		logEvt.Msg("HTTP request to Google Messages failed")
		return
	}
	parsed = parsed.ProtoReflect().New().Interface().(T)
	if err := decodeProtoResp(body, resp.Header.Get("Content-Type"), parsed); err != nil {
		retErr = errors.New("failed to decode provider response")
	}
	successEvt := zerolog.Ctx(resp.Request.Context()).Trace()
	if successEvt.Enabled() {
		successEvt.
			Int("status_code", resp.StatusCode).
			Bool("parsed_has_unknown_fields", len(parsed.ProtoReflect().GetUnknown()) > 0).
			Type("parsed_data_type", parsed).
			Msg("HTTP request to Google Messages succeeded")
	}
	return
}

func readProviderResponseBounded(ctx context.Context, source io.Reader, maximum int64) ([]byte, error) {
	if maximum < 1 {
		return nil, errProviderResponseTooLarge
	}
	limited := &io.LimitedReader{R: &providerContextReader{ctx: ctx, source: source}, N: maximum + 1}
	var body bytes.Buffer
	body.Grow(int(min(maximum, 64<<10)))
	if _, err := io.CopyBuffer(&body, limited, make([]byte, 32<<10)); err != nil {
		return nil, err
	}
	if int64(body.Len()) > maximum {
		zeroBytes(body.Bytes())
		return nil, errProviderResponseTooLarge
	}
	return body.Bytes(), nil
}

type providerContextReader struct {
	ctx    context.Context
	source io.Reader
}

func (reader *providerContextReader) Read(destination []byte) (int, error) {
	if err := reader.ctx.Err(); err != nil {
		return 0, err
	}
	return reader.source.Read(destination)
}

func classifyHTTPStatus(statusCode int) string {
	switch {
	case statusCode == http.StatusUnauthorized || statusCode == http.StatusForbidden:
		return "authorization"
	case statusCode >= 500:
		return "server"
	default:
		return "request"
	}
}
