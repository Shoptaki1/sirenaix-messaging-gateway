package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strconv"
	"strings"

	"go.mau.fi/mautrix-gmessages/internal/gateway/auth"
	"go.mau.fi/mautrix-gmessages/internal/gateway/domain"
	"go.mau.fi/mautrix-gmessages/internal/gateway/media"
	"go.mau.fi/mautrix-gmessages/internal/gateway/messaging"
	"go.mau.fi/mautrix-gmessages/internal/gateway/webhook"
)

const maxMessageJSONBytes = 128 * 1024

func (api *handler) submitMessage(response http.ResponseWriter, request *http.Request, principal auth.Principal, arguments []string) {
	if !queryIsExactly(request, nil) {
		writeInvalidRequest(response)
		return
	}
	key, ok := singleHeaderValue(request, "Idempotency-Key")
	if !ok || !validIdempotencyKey(key) {
		writeInvalidRequest(response)
		return
	}
	var body struct {
		ConversationID string   `json:"conversation_id,omitempty"`
		Recipient      string   `json:"recipient,omitempty"`
		LineID         string   `json:"line_id,omitempty"`
		RouteMode      string   `json:"route_mode,omitempty"`
		Text           string   `json:"text,omitempty"`
		MediaIDs       []string `json:"media_ids,omitempty"`
	}
	if !decodeStrictJSONWithLimit(response, request, &body, maxMessageJSONBytes) {
		return
	}
	if body.ConversationID != "" && !domain.ValidProviderConversationID(strings.TrimSpace(body.ConversationID)) {
		writeInvalidRequest(response)
		return
	}
	if _, ok := api.ownedConnection(response, request, principal.TenantID, domain.ConnectionID(arguments[0])); !ok {
		return
	}
	if api.messages == nil {
		writeInternalError(response)
		return
	}
	mediaIDs := make([]domain.MediaID, len(body.MediaIDs))
	for index, mediaID := range body.MediaIDs {
		mediaIDs[index] = domain.MediaID(mediaID)
	}
	message, err := api.messages.Submit(request.Context(), principal.TenantID, key, messaging.SendInput{
		ConnectionID: domain.ConnectionID(arguments[0]), ConversationID: body.ConversationID,
		Recipient: body.Recipient, LineID: domain.LineID(body.LineID), RouteMode: body.RouteMode,
		Text: body.Text, MediaIDs: mediaIDs,
	})
	if err != nil {
		switch {
		case errors.Is(err, messaging.ErrIdempotencyConflict):
			writeError(response, http.StatusConflict, "idempotency_conflict", "Idempotency-Key was already used for a different request", response.Header().Get("X-Request-ID"))
		case errors.Is(err, messaging.ErrInvalidCommand), errors.Is(err, messaging.ErrInvalidRoute):
			writeInvalidRequest(response)
		default:
			writeInternalError(response)
		}
		return
	}
	writeJSON(response, http.StatusAccepted, messagePayload(message))
}

func (api *handler) getMessage(response http.ResponseWriter, request *http.Request, principal auth.Principal, arguments []string) {
	if !queryIsExactly(request, nil) || !ensureEmptyBody(response, request) {
		return
	}
	if api.messages == nil {
		writeInternalError(response)
		return
	}
	message, err := api.messages.Get(request.Context(), principal.TenantID, domain.MessageID(arguments[0]))
	if err != nil {
		if errors.Is(err, messaging.ErrNotFound) {
			writeNotFound(response)
		} else {
			writeInternalError(response)
		}
		return
	}
	writeJSON(response, http.StatusOK, messagePayload(message))
}

func (api *handler) listMessages(response http.ResponseWriter, request *http.Request, principal auth.Principal, _ []string) {
	if !ensureEmptyBody(response, request) {
		return
	}
	if !queryIsExactly(request, map[string]bool{"after": true, "limit": true}) {
		writeInvalidRequest(response)
		return
	}
	query := request.URL.Query()
	if values, exists := query["after"]; exists && (values[0] == "" || len(values[0]) > 256 || strings.ContainsAny(values[0], "\x00\r\n")) {
		writeInvalidRequest(response)
		return
	}
	if values, exists := query["limit"]; exists && values[0] == "" {
		writeInvalidRequest(response)
		return
	}
	if api.messages == nil {
		writeInternalError(response)
		return
	}
	limit := defaultPageLimit
	if raw := query.Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 || parsed > maxPageLimit {
			writeInvalidRequest(response)
			return
		}
		limit = parsed
	}
	page, err := api.messages.List(request.Context(), principal.TenantID, messaging.ListOptions{
		After: domain.MessageID(query.Get("after")), Limit: limit,
	})
	if err != nil {
		writeInternalError(response)
		return
	}
	messages := make([]map[string]any, len(page.Messages))
	for index, message := range page.Messages {
		messages[index] = messagePayload(message)
	}
	payload := map[string]any{"messages": messages}
	if page.NextCursor != "" {
		payload["next_cursor"] = page.NextCursor
	}
	writeJSON(response, http.StatusOK, payload)
}

func messagePayload(message messaging.OutboundMessage) map[string]any {
	payload := map[string]any{
		"id": message.ID, "connection_id": message.ConnectionID, "conversation_id": message.ConversationID,
		"direction": message.Direction, "state": message.State, "text": message.Text, "created_at": message.CreatedAt.UTC(),
	}
	if message.ProviderMessageID != "" {
		payload["provider_message_id"] = message.ProviderMessageID
	}
	if message.Transport != "" {
		payload["transport"] = message.Transport
	}
	if message.Recipient != "" {
		payload["recipient"] = message.Recipient
	}
	if message.LineID != "" {
		payload["line_id"] = message.LineID
	}
	if message.RouteMode != "" {
		payload["route_mode"] = message.RouteMode
	}
	if len(message.MediaIDs) > 0 {
		payload["media_ids"] = message.MediaIDs
	}
	attachments := message.Attachments
	if len(attachments) == 0 && len(message.MediaIDs) > 0 {
		attachments = make([]messaging.Attachment, len(message.MediaIDs))
		for index, mediaID := range message.MediaIDs {
			attachments[index] = messaging.Attachment{MediaID: mediaID, Position: index}
		}
	}
	if len(attachments) > 0 {
		items := make([]map[string]any, len(attachments))
		for index, attachment := range attachments {
			items[index] = map[string]any{"media_id": attachment.MediaID, "position": attachment.Position}
		}
		payload["attachments"] = items
	}
	return payload
}

func (api *handler) uploadMedia(response http.ResponseWriter, request *http.Request, principal auth.Principal, _ []string) {
	if !queryIsExactly(request, nil) {
		writeInvalidRequest(response)
		return
	}
	contentType, ok := singleHeaderValue(request, "Content-Type")
	if !ok {
		writeError(response, http.StatusUnsupportedMediaType, "unsupported_media_type", "a supported raster image Content-Type is required", response.Header().Get("X-Request-ID"))
		return
	}
	mediaType, _, err := mime.ParseMediaType(contentType)
	if err != nil || !strings.HasPrefix(mediaType, "image/") {
		writeError(response, http.StatusUnsupportedMediaType, "unsupported_media_type", "a supported raster image Content-Type is required", response.Header().Get("X-Request-ID"))
		return
	}
	filenames := request.Header.Values("X-Filename")
	if len(filenames) > 1 {
		writeInvalidRequest(response)
		return
	}
	filename := ""
	if len(filenames) == 1 {
		filename = filenames[0]
	}
	if api.media == nil {
		writeInternalError(response)
		return
	}
	request.Body = http.MaxBytesReader(response, request.Body, media.HardMaxBytes+1)
	record, err := api.media.Upload(request.Context(), principal.TenantID, media.Upload{
		Body: request.Body, ContentLength: request.ContentLength, DeclaredMIME: mediaType,
		Filename: filename,
	})
	if err != nil {
		switch {
		case errors.Is(err, media.ErrTooLarge), errors.Is(err, media.ErrPixelLimit):
			writeError(response, http.StatusRequestEntityTooLarge, "media_too_large", "image exceeds configured limits", response.Header().Get("X-Request-ID"))
		case errors.Is(err, media.ErrUnsupportedImage), errors.Is(err, media.ErrLengthMismatch):
			writeInvalidRequest(response)
		default:
			writeInternalError(response)
		}
		return
	}
	writeJSON(response, http.StatusCreated, mediaPayload(record))
}

func (api *handler) getMedia(response http.ResponseWriter, request *http.Request, principal auth.Principal, arguments []string) {
	if !queryIsExactly(request, nil) || !ensureEmptyBody(response, request) {
		return
	}
	if api.media == nil {
		writeInternalError(response)
		return
	}
	record, err := api.media.GetMetadata(request.Context(), principal.TenantID, domain.MediaID(arguments[0]))
	if err != nil {
		if errors.Is(err, media.ErrNotFound) {
			writeNotFound(response)
		} else {
			writeInternalError(response)
		}
		return
	}
	writeJSON(response, http.StatusOK, mediaPayload(record))
}

func (api *handler) getMediaContent(response http.ResponseWriter, request *http.Request, principal auth.Principal, arguments []string) {
	if !queryIsExactly(request, nil) || !ensureEmptyBody(response, request) {
		return
	}
	if api.media == nil {
		writeInternalError(response)
		return
	}
	reader, record, err := api.media.Open(request.Context(), principal.TenantID, domain.MediaID(arguments[0]))
	if err != nil {
		if errors.Is(err, media.ErrNotFound) {
			writeNotFound(response)
		} else {
			writeInternalError(response)
		}
		return
	}
	defer reader.Close()
	response.Header().Set("Content-Type", record.MIMEType)
	response.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", record.DisplayFilename))
	response.Header().Set("X-Content-Type-Options", "nosniff")
	response.Header().Set("Content-Length", strconv.FormatInt(record.Size, 10))
	response.WriteHeader(http.StatusOK)
	_, _ = io.CopyN(response, reader, record.Size)
}

func mediaPayload(record media.Record) map[string]any {
	payload := map[string]any{
		"id": record.ID, "state": record.State, "display_filename": record.DisplayFilename,
		"created_at": record.CreatedAt.UTC(),
	}
	if record.MIMEType != "" {
		payload["mime_type"] = record.MIMEType
	}
	if record.Size > 0 {
		payload["size"] = record.Size
	}
	if record.Width > 0 && record.Height > 0 {
		payload["width"], payload["height"] = record.Width, record.Height
	}
	if record.State == "ready" {
		payload["content_path"] = "/v1/media/" + string(record.ID) + "/content"
	}
	return payload
}

func (api *handler) createWebhook(response http.ResponseWriter, request *http.Request, principal auth.Principal, _ []string) {
	if !queryIsExactly(request, nil) {
		writeInvalidRequest(response)
		return
	}
	var body struct {
		Destination string `json:"destination"`
	}
	if !decodeStrictJSON(response, request, &body) {
		return
	}
	if api.webhooks == nil {
		writeInternalError(response)
		return
	}
	created, err := api.webhooks.Create(request.Context(), principal.TenantID, body.Destination)
	if err != nil {
		if errors.Is(err, webhook.ErrInvalidEndpoint) || errors.Is(err, webhook.ErrUnsafeDestination) {
			writeInvalidRequest(response)
		} else if errors.Is(err, webhook.ErrEndpointQuotaExceeded) {
			writeError(response, http.StatusConflict, "resource_limit", "tenant webhook endpoint quota reached", response.Header().Get("X-Request-ID"))
		} else {
			writeInternalError(response)
		}
		return
	}
	writeJSON(response, http.StatusCreated, map[string]any{"endpoint": webhookPayload(created.Endpoint), "secret": created.Secret})
}

func (api *handler) listWebhooks(response http.ResponseWriter, request *http.Request, principal auth.Principal, _ []string) {
	if !queryIsExactly(request, map[string]bool{"after": true, "limit": true}) {
		writeInvalidRequest(response)
		return
	}
	if !ensureEmptyBody(response, request) {
		return
	}
	if api.webhooks == nil {
		writeInternalError(response)
		return
	}
	query := request.URL.Query()
	if values, exists := query["after"]; exists && (values[0] == "" || len(values[0]) > 256 || strings.ContainsAny(values[0], "\x00\r\n")) {
		writeInvalidRequest(response)
		return
	}
	limit := defaultPageLimit
	if values, exists := query["limit"]; exists {
		parsed, err := strconv.Atoi(values[0])
		if values[0] == "" || err != nil || parsed < 1 || parsed > maxPageLimit {
			writeInvalidRequest(response)
			return
		}
		limit = parsed
	}
	page, err := api.webhooks.List(request.Context(), principal.TenantID, webhook.EndpointListOptions{After: query.Get("after"), Limit: limit})
	if err != nil {
		writeInternalError(response)
		return
	}
	values := make([]map[string]any, len(page.Endpoints))
	for index, endpoint := range page.Endpoints {
		values[index] = webhookPayload(endpoint)
	}
	payload := map[string]any{"webhooks": values}
	if page.NextCursor != "" {
		payload["next_cursor"] = page.NextCursor
	}
	writeJSON(response, http.StatusOK, payload)
}

func (api *handler) deleteWebhook(response http.ResponseWriter, request *http.Request, principal auth.Principal, arguments []string) {
	if !queryIsExactly(request, nil) || !ensureEmptyBody(response, request) {
		return
	}
	if api.webhooks == nil {
		writeInternalError(response)
		return
	}
	result, err := api.webhooks.Delete(request.Context(), principal.TenantID, arguments[0])
	if err != nil {
		writeNotFound(response)
		return
	}
	if result.Deleting {
		writeJSON(response, http.StatusAccepted, map[string]string{"status": "deleting"})
		return
	}
	response.WriteHeader(http.StatusNoContent)
}

func (api *handler) rotateWebhook(response http.ResponseWriter, request *http.Request, principal auth.Principal, arguments []string) {
	if !queryIsExactly(request, nil) || !ensureEmptyBody(response, request) {
		return
	}
	if api.webhooks == nil {
		writeInternalError(response)
		return
	}
	secret, err := api.webhooks.Rotate(request.Context(), principal.TenantID, arguments[0])
	if err != nil {
		writeNotFound(response)
		return
	}
	writeJSON(response, http.StatusOK, map[string]string{"secret": secret})
}

func (api *handler) replayWebhookDLQ(response http.ResponseWriter, request *http.Request, principal auth.Principal, arguments []string) {
	if !queryIsExactly(request, nil) || !ensureEmptyBody(response, request) {
		return
	}
	if api.webhooks == nil {
		writeInternalError(response)
		return
	}
	if err := api.webhooks.Replay(request.Context(), principal.TenantID, arguments[0]); err != nil {
		writeNotFound(response)
		return
	}
	response.WriteHeader(http.StatusAccepted)
}

func webhookPayload(endpoint webhook.Endpoint) map[string]any {
	return map[string]any{
		"id": endpoint.ID, "destination": endpoint.Destination, "key_id": endpoint.KeyID,
		"active": endpoint.Active, "created_at": endpoint.CreatedAt.UTC(),
	}
}

func decodeStrictJSONWithLimit(response http.ResponseWriter, request *http.Request, target any, limit int64) bool {
	contentType, ok := singleHeaderValue(request, "Content-Type")
	if !ok {
		writeError(response, http.StatusUnsupportedMediaType, "unsupported_media_type", "Content-Type must be application/json", response.Header().Get("X-Request-ID"))
		return false
	}
	mediaType, _, err := mime.ParseMediaType(contentType)
	if err != nil || mediaType != "application/json" {
		writeError(response, http.StatusUnsupportedMediaType, "unsupported_media_type", "Content-Type must be application/json", response.Header().Get("X-Request-ID"))
		return false
	}
	request.Body = http.MaxBytesReader(response, request.Body, limit)
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(target); err != nil {
		writeJSONDecodeError(response, err)
		return false
	}
	var trailing any
	if err = decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		writeJSONDecodeError(response, err)
		return false
	}
	return true
}

func validIdempotencyKey(value string) bool {
	if len(value) < 1 || len(value) > 200 || strings.TrimSpace(value) != value || strings.Contains(value, ",") {
		return false
	}
	for index := range len(value) {
		if value[index] < 0x21 || value[index] > 0x7e {
			return false
		}
	}
	return true
}
