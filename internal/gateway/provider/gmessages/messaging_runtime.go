package gmessages

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"io"
	"strings"

	"go.mau.fi/mautrix-gmessages/internal/gateway/connectionactor"
	"go.mau.fi/mautrix-gmessages/internal/gateway/domain"
	"go.mau.fi/mautrix-gmessages/internal/gateway/media"
	"go.mau.fi/mautrix-gmessages/internal/gateway/messaging"
	"go.mau.fi/mautrix-gmessages/internal/gateway/session"
	"go.mau.fi/mautrix-gmessages/pkg/libgm"
	"go.mau.fi/mautrix-gmessages/pkg/libgm/gmproto"
)

type gatewayMessagingClient interface {
	GetConversation(context.Context, string) (*gmproto.Conversation, error)
	GetOrCreateConversation(context.Context, *gmproto.GetOrCreateConversationRequest) (*gmproto.GetOrCreateConversationResponse, error)
	SendMessage(context.Context, *gmproto.SendMessageRequest) (*gmproto.SendMessageResponse, error)
	UploadMediaContext(context.Context, io.Reader, int64, string, string, int64) (*gmproto.MediaContent, error)
	DownloadMediaContext(context.Context, string, []byte, int64) ([]byte, error)
}

type gatewayMessagingProvider interface {
	gatewayMessagingClient() gatewayMessagingClient
}

type LineResolver interface {
	GetLine(context.Context, domain.TenantID, domain.ConnectionID, domain.LineID) (domain.Line, error)
}

type MediaSource interface {
	Open(context.Context, domain.TenantID, domain.MediaID) (io.ReadCloser, media.Record, error)
}

type CreatedConversationStore interface {
	RecordCreatedConversationFenced(
		context.Context, domain.TenantID, domain.ConnectionID, domain.MessageID,
		string, string, bool, string, uint64,
	) error
}

type ActorSenderConfig struct {
	Executor      connectionactor.ProviderExecutor
	Lines         LineResolver
	Media         MediaSource
	Routes        CreatedConversationStore
	MaxMediaBytes int64
}

type ActorSender struct {
	executor      connectionactor.ProviderExecutor
	lines         LineResolver
	media         MediaSource
	routes        CreatedConversationStore
	maxMediaBytes int64
}

func NewActorSender(config ActorSenderConfig) (*ActorSender, error) {
	if config.Executor == nil || config.Lines == nil || config.Media == nil || config.Routes == nil {
		return nil, messaging.ErrInvalidCommand
	}
	if config.MaxMediaBytes == 0 {
		config.MaxMediaBytes = media.DefaultMaxBytes
	}
	if config.MaxMediaBytes < 1 || config.MaxMediaBytes > media.HardMaxBytes {
		return nil, media.ErrTooLarge
	}
	return &ActorSender{
		executor: config.Executor, lines: config.Lines, media: config.Media,
		routes: config.Routes, maxMediaBytes: config.MaxMediaBytes,
	}, nil
}

func (sender *ActorSender) SendOnce(ctx context.Context, command messaging.ProviderSendCommand) (messaging.ProviderSendResult, error) {
	message := command.Message
	if message.ID == "" || message.TenantID == "" || message.ConnectionID == "" || message.ProviderTmpID == "" ||
		command.FencingToken == 0 ||
		(message.ConversationID == "" && (message.RouteMode != messaging.RouteModePhoneDefault || message.LineID != "" || message.Recipient == "")) {
		return messaging.ProviderSendResult{}, messaging.ErrInvalidCommand
	}
	if message.ConversationID != "" && !domain.ValidProviderConversationID(message.ConversationID) {
		return messaging.ProviderSendResult{}, connectionactor.ErrProviderPermanentProtocol
	}
	var requestedLine *domain.Line
	if message.LineID != "" {
		line, err := sender.lines.GetLine(ctx, message.TenantID, message.ConnectionID, message.LineID)
		if err != nil {
			return messaging.ProviderSendResult{}, err
		}
		requestedLine = &line
	}
	var result messaging.ProviderSendResult
	key := connectionactor.Key{TenantID: message.TenantID, ConnectionID: message.ConnectionID}
	err := sender.executor.Execute(ctx, key, func(operationCtx context.Context, provider connectionactor.Provider) error {
		ownership, owned := connectionactor.ProviderOwnershipFromContext(operationCtx)
		if !owned || ownership.Key != key || ownership.FencingToken != command.FencingToken {
			return connectionactor.ErrProviderUnavailable
		}
		messagingProvider, ok := provider.(gatewayMessagingProvider)
		if !ok || messagingProvider.gatewayMessagingClient() == nil {
			return errors.New("active provider does not support messaging")
		}
		client := messagingProvider.gatewayMessagingClient()
		conversation, err := sender.resolveConversation(operationCtx, client, message)
		if err != nil {
			return err
		}
		providerOutgoingID := strings.TrimSpace(conversation.GetDefaultOutgoingID())
		result.ConversationID = conversation.GetConversationID()
		if !domain.ValidProviderConversationID(conversation.GetConversationID()) || !domain.ValidProviderIdentifier(providerOutgoingID) {
			return connectionactor.ErrProviderPermanentProtocol
		}
		if requestedLine != nil && (requestedLine.TenantID != message.TenantID || requestedLine.ConnectionID != message.ConnectionID || strings.TrimSpace(requestedLine.ProviderOutgoingID) != providerOutgoingID) {
			result.FailureReason = "route_mismatch"
			return nil
		}

		infos := make([]*gmproto.MessageInfo, 0, 1+len(message.MediaIDs))
		if message.Text != "" {
			infos = append(infos, &gmproto.MessageInfo{Data: &gmproto.MessageInfo_MessageContent{MessageContent: &gmproto.MessageContent{Content: message.Text}}})
		}
		for _, mediaID := range message.MediaIDs {
			reader, record, openErr := sender.media.Open(operationCtx, message.TenantID, mediaID)
			if openErr != nil {
				return openErr
			}
			if record.TenantID != message.TenantID || record.ID != mediaID || record.State != "ready" || record.Size < 0 || record.Size > sender.maxMediaBytes {
				_ = reader.Close()
				return media.ErrNotFound
			}
			providerMedia, uploadErr := client.UploadMediaContext(operationCtx, reader, record.Size, record.DisplayFilename, record.MIMEType, sender.maxMediaBytes)
			closeErr := reader.Close()
			if uploadErr != nil {
				return uploadErr
			}
			if closeErr != nil {
				return closeErr
			}
			infos = append(infos, &gmproto.MessageInfo{Data: &gmproto.MessageInfo_MediaContent{MediaContent: providerMedia}})
		}
		request := &gmproto.SendMessageRequest{
			ConversationID: conversation.GetConversationID(), TmpID: message.ProviderTmpID, ForceRCS: false,
			MessagePayload: &gmproto.MessagePayload{
				TmpID: message.ProviderTmpID, TmpID2: message.ProviderTmpID,
				ConversationID: conversation.GetConversationID(), ParticipantID: providerOutgoingID, MessageInfo: infos,
			},
		}
		response, sendErr := client.SendMessage(operationCtx, request)
		if sendErr != nil {
			return sendErr
		}
		switch response.GetStatus() {
		case gmproto.SendMessageResponse_SUCCESS:
			result.Accepted = true
		case gmproto.SendMessageResponse_FAILURE_2, gmproto.SendMessageResponse_FAILURE_3, gmproto.SendMessageResponse_FAILURE_4:
			result.FailureReason = "provider_rejected"
		default:
			// Unknown relay results remain ambiguous and are mapped to uncertain.
		}
		return nil
	})
	return result, err
}

func (sender *ActorSender) resolveConversation(ctx context.Context, client gatewayMessagingClient, message messaging.OutboundMessage) (*gmproto.Conversation, error) {
	if message.ConversationID != "" {
		return client.GetConversation(ctx, message.ConversationID)
	}
	response, err := client.GetOrCreateConversation(ctx, &gmproto.GetOrCreateConversationRequest{Numbers: []*gmproto.ContactNumber{{
		MysteriousInt: 2, Number: message.Recipient, Number2: message.Recipient,
	}}})
	if err != nil {
		// Get-or-create is mutating and no SendMessage may follow an ambiguous result.
		return nil, err
	}
	conversation := response.GetConversation()
	if response.GetStatus() != gmproto.GetOrCreateConversationResponse_SUCCESS || conversation == nil ||
		!domain.ValidProviderConversationID(conversation.GetConversationID()) || conversation.GetIsGroupChat() ||
		!domain.ValidProviderIdentifier(strings.TrimSpace(conversation.GetDefaultOutgoingID())) {
		return nil, connectionactor.ErrProviderPermanentProtocol
	}
	ownership, ok := connectionactor.ProviderOwnershipFromContext(ctx)
	if !ok {
		return nil, connectionactor.ErrProviderUnavailable
	}
	if err = sender.routes.RecordCreatedConversationFenced(
		ctx, message.TenantID, message.ConnectionID, message.ID,
		conversation.GetConversationID(), strings.TrimSpace(conversation.GetDefaultOutgoingID()), false,
		ownership.OwnerID, ownership.FencingToken,
	); err != nil {
		return nil, err
	}
	return conversation, nil
}

type MediaKeyOpener interface {
	Open(context.Context, session.Scope, session.Envelope) ([]byte, error)
}

type ActorMediaFetcherConfig struct {
	Executor connectionactor.ProviderExecutor
	Keys     MediaKeyOpener
	MaxBytes int64
}

type ActorMediaFetcher struct {
	executor connectionactor.ProviderExecutor
	keys     MediaKeyOpener
	maxBytes int64
}

func NewActorMediaFetcher(config ActorMediaFetcherConfig) (*ActorMediaFetcher, error) {
	if config.Executor == nil || config.Keys == nil {
		return nil, domain.ErrInvalidIdentifier
	}
	if config.MaxBytes == 0 {
		config.MaxBytes = media.DefaultMaxBytes
	}
	if config.MaxBytes < 1 || config.MaxBytes > media.HardMaxBytes {
		return nil, media.ErrTooLarge
	}
	return &ActorMediaFetcher{executor: config.Executor, keys: config.Keys, maxBytes: config.MaxBytes}, nil
}

func (fetcher *ActorMediaFetcher) Fetch(ctx context.Context, job media.FetchJob) (media.FetchContent, error) {
	encodedMediaID, found := strings.CutPrefix(job.Locator, "gmessages:")
	mediaID, decodeErr := base64.RawURLEncoding.DecodeString(encodedMediaID)
	if job.TenantID == "" || job.ConnectionID == "" || !found || encodedMediaID == "" || decodeErr != nil || len(mediaID) == 0 || len(mediaID) > 1024 || job.KeyEnvelope.Provider != "gmessages-media" {
		return media.FetchContent{}, media.ErrUnsafeURL
	}
	var content media.FetchContent
	err := fetcher.executor.Execute(ctx, connectionactor.Key{TenantID: job.TenantID, ConnectionID: job.ConnectionID}, func(operationCtx context.Context, provider connectionactor.Provider) error {
		messagingProvider, ok := provider.(gatewayMessagingProvider)
		if !ok || messagingProvider.gatewayMessagingClient() == nil {
			return errors.New("active provider does not support media")
		}
		key, openErr := fetcher.keys.Open(operationCtx, session.Scope{
			TenantID: string(job.TenantID), ConnectionID: string(job.ConnectionID), Provider: "gmessages-media",
		}, job.KeyEnvelope)
		if openErr != nil {
			return openErr
		}
		defer zeroBytes(key)
		data, downloadErr := messagingProvider.gatewayMessagingClient().DownloadMediaContext(operationCtx, string(mediaID), key, fetcher.maxBytes)
		if downloadErr != nil {
			return downloadErr
		}
		content = media.FetchContent{
			Body: io.NopCloser(bytes.NewReader(data)), ContentLength: int64(len(data)),
			MIMEType: job.DeclaredMIME, Filename: job.DisplayFilename,
		}
		return nil
	})
	return content, err
}

type PinnedMediaRequestPolicy struct{ policy *media.URLPolicy }

func NewPinnedMediaRequestPolicy(policy *media.URLPolicy) (*PinnedMediaRequestPolicy, error) {
	if policy == nil {
		return nil, media.ErrUnsafeURL
	}
	return &PinnedMediaRequestPolicy{policy: policy}, nil
}

func (policy *PinnedMediaRequestPolicy) ClientFor(ctx context.Context, rawURL string) (libgm.HTTPDoer, error) {
	target, err := policy.policy.ValidateAndPin(ctx, rawURL)
	if err != nil {
		return nil, err
	}
	return policy.policy.Client(target), nil
}

var _ messaging.ProviderSender = (*ActorSender)(nil)
var _ media.ActorFetcher = (*ActorMediaFetcher)(nil)
var _ libgm.MediaRequestPolicy = (*PinnedMediaRequestPolicy)(nil)
