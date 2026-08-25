package httpapi

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"go.mau.fi/mautrix-gmessages/internal/gateway/domain"
	"go.mau.fi/mautrix-gmessages/internal/gateway/media"
	"go.mau.fi/mautrix-gmessages/internal/gateway/messaging"
	"go.mau.fi/mautrix-gmessages/internal/gateway/webhook"
)

type messageAPI struct {
	message messaging.OutboundMessage
	err     error
	tenant  domain.TenantID
	key     string
	input   messaging.SendInput
	submits int
	lists   int
}

func (api *messageAPI) Submit(_ context.Context, tenant domain.TenantID, key string, input messaging.SendInput) (messaging.OutboundMessage, error) {
	api.submits++
	api.tenant, api.key, api.input = tenant, key, input
	return api.message, api.err
}
func (api *messageAPI) Get(_ context.Context, tenant domain.TenantID, _ domain.MessageID) (messaging.OutboundMessage, error) {
	api.tenant = tenant
	return api.message, api.err
}
func (api *messageAPI) List(_ context.Context, tenant domain.TenantID, _ messaging.ListOptions) (messaging.MessagePage, error) {
	api.lists++
	api.tenant = tenant
	return messaging.MessagePage{Messages: []messaging.OutboundMessage{api.message}}, api.err
}

type mediaAPI struct {
	record    media.Record
	body      []byte
	length    int64
	tenant    domain.TenantID
	openCalls int
}

func (api *mediaAPI) Upload(_ context.Context, tenant domain.TenantID, upload media.Upload) (media.Record, error) {
	api.tenant, api.length = tenant, upload.ContentLength
	api.body, _ = io.ReadAll(upload.Body)
	return api.record, nil
}
func (api *mediaAPI) GetMetadata(_ context.Context, tenant domain.TenantID, _ domain.MediaID) (media.Record, error) {
	api.tenant = tenant
	return api.record, nil
}
func (api *mediaAPI) Open(_ context.Context, tenant domain.TenantID, _ domain.MediaID) (io.ReadCloser, media.Record, error) {
	api.tenant = tenant
	api.openCalls++
	return io.NopCloser(bytes.NewReader(api.body)), api.record, nil
}

func TestPendingInboundMediaMetadataIsReadableWithoutOpeningContent(t *testing.T) {
	mediaService := &mediaAPI{record: media.Record{
		ID: "media-pending", TenantID: "tenant-example", MIMEType: "image/png", State: "pending",
		DisplayFilename: "pending.png", CreatedAt: time.Unix(1700000000, 0).UTC(),
	}}
	handler := task7Handler(t, nil, mediaService, nil)
	metadata := serve(handler, http.MethodGet, "/v1/media/media-pending", nil, "valid")
	if metadata.Code != http.StatusOK || !strings.Contains(metadata.Body.String(), `"state":"pending"`) || strings.Contains(metadata.Body.String(), "content_path") || mediaService.openCalls != 0 {
		t.Fatalf("metadata=%d body=%s content opens=%d", metadata.Code, metadata.Body, mediaService.openCalls)
	}
}

type webhookAPI struct {
	created     webhook.CreatedEndpoint
	tenant      domain.TenantID
	deleting    bool
	listOptions webhook.EndpointListOptions
	nextCursor  string
	createErr   error
}

func (api *webhookAPI) Create(_ context.Context, tenant domain.TenantID, _ string) (webhook.CreatedEndpoint, error) {
	api.tenant = tenant
	if api.createErr != nil {
		return webhook.CreatedEndpoint{}, api.createErr
	}
	return api.created, nil
}
func (api *webhookAPI) Rotate(context.Context, domain.TenantID, string) (string, error) {
	return "rotated-once", nil
}
func (api *webhookAPI) List(_ context.Context, _ domain.TenantID, options webhook.EndpointListOptions) (webhook.EndpointPage, error) {
	api.listOptions = options
	return webhook.EndpointPage{Endpoints: []webhook.Endpoint{api.created.Endpoint}, NextCursor: api.nextCursor}, nil
}
func (api *webhookAPI) Delete(context.Context, domain.TenantID, string) (webhook.DeleteResult, error) {
	return webhook.DeleteResult{Deleting: api.deleting}, nil
}
func (api *webhookAPI) Replay(context.Context, domain.TenantID, string) error { return nil }

func task7Handler(t *testing.T, messages MessageService, mediaService MediaService, webhooks WebhookService) http.Handler {
	t.Helper()
	handler, err := NewHandler(Dependencies{
		Store: newFakeStore(t), Syncer: &fakeSyncer{}, Pairing: &fakePairer{}, Verifier: validVerifier(),
		NewID: func() string { return "generated-id" }, Messages: messages, Media: mediaService, Webhooks: webhooks,
	})
	if err != nil {
		t.Fatal(err)
	}
	return handler
}

func TestMessageRouteRequiresIdempotencyAndUsesPrincipalTenant(t *testing.T) {
	messages := &messageAPI{message: messaging.OutboundMessage{
		ID: "message-a", TenantID: "tenant-example", ConnectionID: "connection-example", ConversationID: "conversation-a",
		Text: "hello", State: domain.MessageStateQueued, CreatedAt: time.Unix(1700000000, 0).UTC(),
	}}
	handler := task7Handler(t, messages, nil, nil)
	request := authenticatedJSONRequest(http.MethodPost, "/v1/connections/connection-example/messages", `{"conversation_id":"conversation-a","text":"hello"}`, "valid")
	request.Header.Set("Idempotency-Key", "idem-a")
	response := serveRequest(handler, request)
	if response.Code != http.StatusAccepted || messages.tenant != "tenant-example" || messages.key != "idem-a" || messages.input.ConnectionID != "connection-example" {
		t.Fatalf("response=%d body=%s tenant=%s key=%q input=%+v", response.Code, response.Body, messages.tenant, messages.key, messages.input)
	}
	missing := serveJSON(handler, http.MethodPost, "/v1/connections/connection-example/messages", `{"conversation_id":"conversation-a","text":"hello"}`, "valid")
	assertError(t, missing, http.StatusBadRequest, "invalid_request", "")
}

func TestMessageRouteRejectsReservedProviderConversationBeforeService(t *testing.T) {
	messages := &messageAPI{}
	handler := task7Handler(t, messages, nil, nil)
	request := authenticatedJSONRequest(
		http.MethodPost, "/v1/connections/connection-example/messages",
		`{"conversation_id":"_provider_page","text":"hello"}`, "valid",
	)
	request.Header.Set("Idempotency-Key", "reserved-provider-route")
	response := serveRequest(handler, request)
	assertError(t, response, http.StatusBadRequest, "invalid_request", "")
	if messages.submits != 0 {
		t.Fatalf("reserved provider conversation reached service %d times", messages.submits)
	}
}

func TestMessageRoutesRejectAmbiguousHeadersAndQueriesBeforeService(t *testing.T) {
	messages := &messageAPI{}
	handler := task7Handler(t, messages, nil, nil)

	for name, values := range map[string][]string{
		"duplicate": {"idem-a", "idem-a"},
		"comma":     {"idem-a,idem-b"},
		"space":     {" idem-a"},
	} {
		t.Run(name, func(t *testing.T) {
			request := authenticatedJSONRequest(http.MethodPost, "/v1/connections/connection-example/messages", `{"conversation_id":"conversation-a","text":"hello"}`, "valid")
			request.Header.Del("Idempotency-Key")
			for _, value := range values {
				request.Header.Add("Idempotency-Key", value)
			}
			response := serveRequest(handler, request)
			assertError(t, response, http.StatusBadRequest, "invalid_request", "")
		})
	}
	if messages.submits != 0 {
		t.Fatalf("Submit calls = %d, want 0", messages.submits)
	}

	for _, path := range []string{
		"/v1/messages?limit=1&limit=2",
		"/v1/messages?after=first&after=second",
		"/v1/messages?limit=",
		"/v1/messages?after=",
	} {
		response := serve(handler, http.MethodGet, path, nil, "valid")
		assertError(t, response, http.StatusBadRequest, "invalid_request", "")
	}
	if messages.lists != 0 {
		t.Fatalf("List calls = %d, want 0", messages.lists)
	}
}

func TestMessageSubmissionRejectsOversizeAndDuplicateContentTypeBeforeService(t *testing.T) {
	messages := &messageAPI{}
	handler := task7Handler(t, messages, nil, nil)

	oversized := authenticatedRequest(http.MethodPost, "/v1/connections/connection-example/messages", strings.NewReader(`{"text":"`+strings.Repeat("x", maxMessageJSONBytes)+`"}`), "valid")
	oversized.Header.Set("Content-Type", "application/json")
	oversized.Header.Set("Idempotency-Key", "idem-oversized")
	assertError(t, serveRequest(handler, oversized), http.StatusRequestEntityTooLarge, "request_too_large", "")

	duplicateType := authenticatedRequest(http.MethodPost, "/v1/connections/connection-example/messages", strings.NewReader(`{"conversation_id":"conversation-a","text":"hello"}`), "valid")
	duplicateType.Header.Add("Content-Type", "application/json")
	duplicateType.Header.Add("Content-Type", "application/json")
	duplicateType.Header.Set("Idempotency-Key", "idem-duplicate-content-type")
	assertError(t, serveRequest(handler, duplicateType), http.StatusUnsupportedMediaType, "unsupported_media_type", "")

	if messages.submits != 0 {
		t.Fatalf("Submit calls = %d, want 0", messages.submits)
	}
}

func TestMessageReadExposesDirectionTransportProviderIdentityAndOrderedAttachments(t *testing.T) {
	messages := &messageAPI{message: messaging.OutboundMessage{
		ID: "message-a", TenantID: "tenant-example", ConnectionID: "connection-example", ConversationID: "conversation-a",
		Direction: "inbound", Transport: "mms", ProviderMessageID: "provider-a", Text: "image",
		Attachments: []messaging.Attachment{{MediaID: "media-first", Position: 0}, {MediaID: "media-second", Position: 1}},
		State:       domain.MessageStateDelivered, CreatedAt: time.Unix(1700000000, 0).UTC(),
	}}
	response := serve(task7Handler(t, messages, nil, nil), http.MethodGet, "/v1/messages/message-a", nil, "valid")
	body := response.Body.String()
	if response.Code != http.StatusOK || !strings.Contains(body, `"direction":"inbound"`) ||
		!strings.Contains(body, `"transport":"mms"`) || !strings.Contains(body, `"provider_message_id":"provider-a"`) ||
		!strings.Contains(body, "media-first") || strings.Index(body, "media-first") > strings.Index(body, "media-second") {
		t.Fatalf("message response = %d %s", response.Code, body)
	}
}

func TestMediaUploadUsesRawBoundedBodyAndContentIsAttachment(t *testing.T) {
	mediaService := &mediaAPI{record: media.Record{
		ID: "media-a", TenantID: "tenant-example", MIMEType: "image/png", Size: 3, Width: 1, Height: 1,
		DisplayFilename: "image.png", State: "ready", CreatedAt: time.Unix(1700000000, 0).UTC(),
	}, body: []byte("abc")}
	handler := task7Handler(t, nil, mediaService, nil)
	upload := authenticatedRequest(http.MethodPost, "/v1/media", strings.NewReader("abc"), "valid")
	upload.Header.Set("Content-Type", "image/png")
	uploadResponse := serveRequest(handler, upload)
	if uploadResponse.Code != http.StatusCreated || string(mediaService.body) != "abc" || mediaService.tenant != "tenant-example" {
		t.Fatalf("upload=%d body=%s captured=%q", uploadResponse.Code, uploadResponse.Body, mediaService.body)
	}
	mediaService.body = []byte("abc")
	content := serve(handler, http.MethodGet, "/v1/media/media-a/content", nil, "valid")
	if content.Code != http.StatusOK || content.Body.String() != "abc" || !strings.HasPrefix(content.Header().Get("Content-Disposition"), "attachment;") || content.Header().Get("X-Content-Type-Options") != "nosniff" {
		t.Fatalf("content=%d headers=%v body=%q", content.Code, content.Header(), content.Body.String())
	}
}

func TestMediaUploadRejectsDuplicateSecurityHeadersBeforeService(t *testing.T) {
	mediaService := &mediaAPI{}
	handler := task7Handler(t, nil, mediaService, nil)

	duplicateType := authenticatedRequest(http.MethodPost, "/v1/media", strings.NewReader("body"), "valid")
	duplicateType.Header.Add("Content-Type", "image/png")
	duplicateType.Header.Add("Content-Type", "image/png")
	assertError(t, serveRequest(handler, duplicateType), http.StatusUnsupportedMediaType, "unsupported_media_type", "")

	duplicateName := authenticatedRequest(http.MethodPost, "/v1/media", strings.NewReader("body"), "valid")
	duplicateName.Header.Set("Content-Type", "image/png")
	duplicateName.Header.Add("X-Filename", "one.png")
	duplicateName.Header.Add("X-Filename", "two.png")
	assertError(t, serveRequest(handler, duplicateName), http.StatusBadRequest, "invalid_request", "")

	if mediaService.tenant != "" {
		t.Fatalf("Upload unexpectedly reached service for tenant %q", mediaService.tenant)
	}
}

func TestWebhookCreationRevealsSecretButListDoesNot(t *testing.T) {
	webhooks := &webhookAPI{created: webhook.CreatedEndpoint{Endpoint: webhook.Endpoint{
		ID: "endpoint-a", TenantID: "tenant-example", Destination: "https://hooks.example/receive", KeyID: "key-a", Active: true,
	}, Secret: "revealed-once"}, nextCursor: "endpoint-next"}
	handler := task7Handler(t, nil, nil, webhooks)
	created := serveJSON(handler, http.MethodPost, "/v1/webhooks", `{"destination":"https://hooks.example/receive"}`, "valid")
	if created.Code != http.StatusCreated || !strings.Contains(created.Body.String(), "revealed-once") || webhooks.tenant != "tenant-example" {
		t.Fatalf("create=%d body=%s", created.Code, created.Body)
	}
	listed := serve(handler, http.MethodGet, "/v1/webhooks?after=endpoint-before&limit=1", nil, "valid")
	if listed.Code != http.StatusOK || strings.Contains(listed.Body.String(), "revealed-once") || strings.Contains(listed.Body.String(), "secret") || !strings.Contains(listed.Body.String(), `"next_cursor":"endpoint-next"`) {
		t.Fatalf("list=%d body=%s", listed.Code, listed.Body)
	}
	if webhooks.listOptions != (webhook.EndpointListOptions{After: "endpoint-before", Limit: 1}) {
		t.Fatalf("list options = %+v", webhooks.listOptions)
	}
	for _, target := range []string{"/v1/webhooks?after=a&after=b", "/v1/webhooks?limit=1&limit=2", "/v1/webhooks?limit=201"} {
		if response := serve(handler, http.MethodGet, target, nil, "valid"); response.Code != http.StatusBadRequest {
			t.Fatalf("GET %s = %d", target, response.Code)
		}
	}
}

func TestWebhookCreationMapsEndpointQuota(t *testing.T) {
	webhooks := &webhookAPI{createErr: webhook.ErrEndpointQuotaExceeded}
	handler := task7Handler(t, nil, nil, webhooks)
	response := serveJSON(handler, http.MethodPost, "/v1/webhooks", `{"destination":"https://hooks.example/receive"}`, "valid")
	assertError(t, response, http.StatusConflict, "resource_limit", "")
}

func TestWebhookDeleteReturnsAcceptedWhileRequestIsAlreadyOnWire(t *testing.T) {
	webhooks := &webhookAPI{deleting: true}
	handler := task7Handler(t, nil, nil, webhooks)
	response := serve(handler, http.MethodDelete, "/v1/webhooks/endpoint-a", nil, "valid")
	if response.Code != http.StatusAccepted || !strings.Contains(response.Body.String(), `"status":"deleting"`) {
		t.Fatalf("delete status = %d body=%s, want 202", response.Code, response.Body)
	}
}

func serveRequest(handler http.Handler, request *http.Request) *httptest.ResponseRecorder {
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}
