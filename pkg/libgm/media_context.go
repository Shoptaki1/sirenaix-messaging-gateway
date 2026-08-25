package libgm

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	"go.mau.fi/mautrix-gmessages/pkg/libgm/crypto"
	"go.mau.fi/mautrix-gmessages/pkg/libgm/gmproto"
	"go.mau.fi/mautrix-gmessages/pkg/libgm/util"
)

const (
	maxGatewayMediaBytes   = int64(25 << 20)
	maxUploadResponseBytes = int64(64 << 10)
)

var (
	ErrMediaPolicyRequired = errors.New("secure media HTTP policy is required")
	ErrMediaTooLarge       = errors.New("provider media exceeds limit")
	ErrMediaInvalid        = errors.New("provider media is invalid")
)

type HTTPDoer interface {
	Do(*http.Request) (*http.Response, error)
}

// MediaRequestPolicy validates every fixed or provider-returned media URL and
// returns a DNS-pinned, redirect-denying client for exactly that target.
type MediaRequestPolicy interface {
	ClientFor(context.Context, string) (HTTPDoer, error)
}

func (c *Client) SetMediaRequestPolicy(policy MediaRequestPolicy) {
	c.mediaPolicyMu.Lock()
	c.mediaPolicy = policy
	c.mediaPolicyMu.Unlock()
}

func (c *Client) mediaClient(ctx context.Context, rawURL string) (HTTPDoer, error) {
	c.mediaPolicyMu.RLock()
	policy := c.mediaPolicy
	c.mediaPolicyMu.RUnlock()
	if policy == nil {
		return nil, ErrMediaPolicyRequired
	}
	client, err := policy.ClientFor(ctx, rawURL)
	if err != nil {
		return nil, err
	}
	if client == nil {
		return nil, ErrMediaPolicyRequired
	}
	return client, nil
}

func (c *Client) hasMediaPolicy() bool {
	c.mediaPolicyMu.RLock()
	defer c.mediaPolicyMu.RUnlock()
	return c.mediaPolicy != nil
}

// UploadMediaContext is the bounded, cancellation-aware gateway upload path.
// It never uses the legacy contextless media helpers.
func (c *Client) UploadMediaContext(ctx context.Context, source io.Reader, declaredSize int64, fileName, mimeType string, maximum int64) (*gmproto.MediaContent, error) {
	if source == nil || maximum < 1 || maximum > maxGatewayMediaBytes || declaredSize > maximum || !gatewayImageMIME(mimeType) || len(fileName) > 255 {
		return nil, ErrMediaInvalid
	}
	if !c.hasMediaPolicy() {
		return nil, ErrMediaPolicyRequired
	}
	plaintext, err := readMediaBounded(ctx, source, maximum)
	if err != nil {
		return nil, err
	}
	defer zeroBytes(plaintext)
	if declaredSize >= 0 && int64(len(plaintext)) != declaredSize {
		return nil, ErrMediaInvalid
	}
	key := crypto.GenerateKey(32)
	defer zeroBytes(key)
	cryptor, err := crypto.NewAESGCMHelper(key)
	if err != nil {
		return nil, ErrMediaInvalid
	}
	encrypted, err := cryptor.EncryptData(plaintext)
	if err != nil {
		return nil, ErrMediaInvalid
	}
	defer zeroBytes(encrypted)
	uploadURL, err := c.startMediaUploadContext(ctx, encrypted, mimeType)
	if err != nil {
		return nil, err
	}
	mediaID, err := c.finalizeMediaUploadContext(ctx, uploadURL, encrypted, mimeType)
	if err != nil {
		return nil, err
	}
	mediaType := MimeToMediaType[mimeType]
	return &gmproto.MediaContent{
		Format: mediaType.Type, MediaID: mediaID, MediaName: fileName,
		Size: int64(len(plaintext)), DecryptionKey: append([]byte(nil), key...), MimeType: mimeType,
	}, nil
}

func (c *Client) startMediaUploadContext(ctx context.Context, encrypted []byte, mimeType string) (string, error) {
	payload, err := c.buildStartUploadPayload()
	if err != nil {
		return "", ErrMediaInvalid
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, util.UploadMediaURL, strings.NewReader(payload))
	if err != nil {
		return "", ErrMediaInvalid
	}
	request.Header = *util.NewMediaUploadHeaders(fmt.Sprint(len(encrypted)), "start", "", mimeType, "resumable")
	client, err := c.mediaClient(ctx, request.URL.String())
	if err != nil {
		return "", err
	}
	response, err := client.Do(request)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return "", errors.New("provider media upload start failed")
	}
	uploadURL := response.Header.Get("x-goog-upload-url")
	if uploadURL == "" {
		return "", ErrMediaInvalid
	}
	// Validate the provider-returned target before carrying it to finalize.
	if _, err = c.mediaClient(ctx, uploadURL); err != nil {
		return "", err
	}
	return uploadURL, nil
}

func (c *Client) finalizeMediaUploadContext(ctx context.Context, uploadURL string, encrypted []byte, mimeType string) (string, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, uploadURL, bytes.NewReader(encrypted))
	if err != nil {
		return "", ErrMediaInvalid
	}
	request.Header = *util.NewMediaUploadHeaders(fmt.Sprint(len(encrypted)), "upload, finalize", "0", mimeType, "")
	client, err := c.mediaClient(ctx, uploadURL)
	if err != nil {
		return "", err
	}
	response, err := client.Do(request)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return "", errors.New("provider media upload finalize failed")
	}
	body, err := readMediaBounded(ctx, response.Body, maxUploadResponseBytes)
	if err != nil {
		return "", ErrMediaInvalid
	}
	defer zeroBytes(body)
	if isStandardBase64(body) {
		decoded := make([]byte, base64.StdEncoding.DecodedLen(len(body)))
		count, decodeErr := base64.StdEncoding.Decode(decoded, body)
		if decodeErr != nil {
			zeroBytes(decoded)
			return "", ErrMediaInvalid
		}
		zeroBytes(body)
		body = decoded[:count]
		defer zeroBytes(decoded)
	}
	var parsed gmproto.UploadMediaResponse
	if err = proto.Unmarshal(body, &parsed); err != nil || parsed.GetMedia().GetMediaID() == "" {
		return "", ErrMediaInvalid
	}
	return parsed.GetMedia().GetMediaID(), nil
}

func (c *Client) DownloadMediaContext(ctx context.Context, mediaID string, key []byte, maximum int64) ([]byte, error) {
	if mediaID == "" || len(mediaID) > 1024 || len(key) != 32 || maximum < 1 || maximum > maxGatewayMediaBytes {
		return nil, ErrMediaInvalid
	}
	auth := c.AuthData.Snapshot()
	defer auth.ClearSecrets()
	metadata := &gmproto.DownloadAttachmentRequest{
		Info: &gmproto.AttachmentInfo{AttachmentID: mediaID, Encrypted: true},
		AuthData: &gmproto.AuthMessage{
			RequestID: uuid.NewString(), TachyonAuthToken: auth.TachyonAuthToken,
			Network: auth.AuthNetwork(), ConfigVersion: util.ConfigMessage,
		},
	}
	encoded, err := proto.Marshal(metadata)
	if err != nil {
		return nil, ErrMediaInvalid
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, util.UploadMediaURL, nil)
	if err != nil {
		return nil, ErrMediaInvalid
	}
	util.BuildUploadHeaders(request, base64.StdEncoding.EncodeToString(encoded))
	client, err := c.mediaClient(ctx, request.URL.String())
	if err != nil {
		return nil, err
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, errors.New("provider media download failed")
	}
	if response.ContentLength > maximum+32 {
		return nil, ErrMediaTooLarge
	}
	encrypted, err := readMediaBounded(ctx, response.Body, maximum+32)
	if err != nil {
		return nil, err
	}
	defer zeroBytes(encrypted)
	cryptor, err := crypto.NewAESGCMHelper(key)
	if err != nil {
		return nil, ErrMediaInvalid
	}
	plaintext, err := cryptor.DecryptData(encrypted)
	if err != nil {
		return nil, ErrMediaInvalid
	}
	if int64(len(plaintext)) > maximum {
		zeroBytes(plaintext)
		return nil, ErrMediaTooLarge
	}
	return plaintext, nil
}

func readMediaBounded(ctx context.Context, source io.Reader, maximum int64) ([]byte, error) {
	reader := &io.LimitedReader{R: &mediaContextReader{ctx: ctx, reader: source}, N: maximum + 1}
	var output bytes.Buffer
	output.Grow(int(min(maximum, 64<<10)))
	if _, err := io.CopyBuffer(&output, reader, make([]byte, 32<<10)); err != nil {
		return nil, err
	}
	if int64(output.Len()) > maximum {
		zeroBytes(output.Bytes())
		return nil, ErrMediaTooLarge
	}
	return output.Bytes(), nil
}

type mediaContextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (reader *mediaContextReader) Read(buffer []byte) (int, error) {
	if err := reader.ctx.Err(); err != nil {
		return 0, err
	}
	return reader.reader.Read(buffer)
}

func gatewayImageMIME(value string) bool {
	switch value {
	case "image/jpeg", "image/png", "image/gif", "image/webp":
		return true
	default:
		return false
	}
}
