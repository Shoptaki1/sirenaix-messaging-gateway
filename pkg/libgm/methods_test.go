package libgm

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/rs/zerolog"
	"go.mau.fi/mautrix-gmessages/pkg/libgm/gmproto"
	"go.mau.fi/util/pblite"
	"google.golang.org/protobuf/proto"
)

func TestListConversationsPublicMethodsForwardCursor(t *testing.T) {
	cursor := &gmproto.Cursor{LastItemID: "conversation-23", LastItemTimestamp: 1724400000023}
	tests := []struct {
		name   string
		invoke func(context.Context, *Client) error
		want   *gmproto.Cursor
	}{
		{
			name: "legacy method sends nil cursor",
			invoke: func(ctx context.Context, client *Client) error {
				_, err := client.ListConversations(ctx, 25, gmproto.ListConversationsRequest_ARCHIVE)
				return err
			},
		},
		{
			name: "cursor method sends supplied cursor",
			invoke: func(ctx context.Context, client *Client) error {
				_, err := client.ListConversationsWithCursor(ctx, 25, gmproto.ListConversationsRequest_ARCHIVE, cursor)
				return err
			},
			want: cursor,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var captured *gmproto.ListConversationsRequest
			client := newConversationRequestTestClient(t, &captured)

			if err := test.invoke(context.Background(), client); err != nil {
				t.Fatalf("public conversation list method error = %v", err)
			}
			if captured == nil {
				t.Fatal("public conversation list method sent no request")
			}
			if captured.GetCount() != 25 || captured.GetFolder() != gmproto.ListConversationsRequest_ARCHIVE {
				t.Fatalf("captured pagination fields = (%d, %s)", captured.GetCount(), captured.GetFolder())
			}
			if !proto.Equal(captured.GetCursor(), test.want) {
				t.Fatalf("captured cursor = %v, want %v", captured.GetCursor(), test.want)
			}
		})
	}
}

func TestBuildListConversationsRequestIncludesCursor(t *testing.T) {
	cursor := &gmproto.Cursor{LastItemID: "conversation-17", LastItemTimestamp: 1724400000000}

	request := buildListConversationsRequest(25, gmproto.ListConversationsRequest_ARCHIVE, cursor)

	if request.GetCount() != 25 || request.GetFolder() != gmproto.ListConversationsRequest_ARCHIVE {
		t.Fatalf("request pagination fields = (%d, %s)", request.GetCount(), request.GetFolder())
	}
	if !proto.Equal(request.GetCursor(), cursor) {
		t.Fatalf("request cursor = %v, want %v", request.GetCursor(), cursor)
	}
}

func TestBuildListConversationsRequestKeepsLegacyNilCursor(t *testing.T) {
	request := buildListConversationsRequest(50, gmproto.ListConversationsRequest_INBOX, nil)

	if request.GetCursor() != nil {
		t.Fatalf("legacy request cursor = %v, want nil", request.GetCursor())
	}
}

type conversationRequestTransport struct {
	client   *Client
	captured **gmproto.ListConversationsRequest
	poisoned bool
}

func (transport *conversationRequestTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	body, err := io.ReadAll(request.Body)
	if err != nil {
		return nil, fmt.Errorf("read request: %w", err)
	}
	var outgoing gmproto.OutgoingRPCMessage
	if err := pblite.Unmarshal(body, &outgoing); err != nil {
		return nil, fmt.Errorf("decode outgoing RPC message: %w", err)
	}
	var rpcData gmproto.OutgoingRPCData
	if err := proto.Unmarshal(outgoing.GetData().GetMessageData(), &rpcData); err != nil {
		return nil, fmt.Errorf("decode outgoing RPC data: %w", err)
	}
	plaintext, err := transport.client.AuthData.RequestCrypto.Decrypt(rpcData.GetEncryptedProtoData())
	if err != nil {
		return nil, fmt.Errorf("decrypt conversation request: %w", err)
	}
	var conversationRequest gmproto.ListConversationsRequest
	if err := proto.Unmarshal(plaintext, &conversationRequest); err != nil {
		return nil, fmt.Errorf("decode conversation request: %w", err)
	}
	*transport.captured = proto.Clone(&conversationRequest).(*gmproto.ListConversationsRequest)

	incoming := &IncomingRPCMessage{
		IncomingRPCMessage: &gmproto.IncomingRPCMessage{ResponseID: "response-1"},
		PayloadSource:      PayloadSourceEncryptedData,
		Message: &gmproto.RPCMessageData{
			SessionID:     rpcData.GetRequestID(),
			Action:        gmproto.ActionType_LIST_CONVERSATIONS,
			EncryptedData: []byte{1},
		},
		DecryptedMessage: &gmproto.ListConversationsResponse{Conversations: []*gmproto.Conversation{{ConversationID: "must-not-escape"}}},
	}
	if transport.poisoned {
		incoming.DurableOutcome = DurableOutcomePoisoned
		incoming.DurableError = ErrDurablePoisoned
	}
	received := transport.client.sessionHandler.receiveResponse(incoming)
	if !received {
		return nil, fmt.Errorf("conversation response waiter not found")
	}
	responseBody, err := pblite.Marshal(&gmproto.OutgoingRPCResponse{})
	if err != nil {
		return nil, fmt.Errorf("encode outgoing RPC response: %w", err)
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{ContentTypePBLite}},
		Body:       io.NopCloser(bytes.NewReader(responseBody)),
		Request:    request,
	}, nil
}

func TestConversationListWrappersCannotIgnoreDurablePoison(t *testing.T) {
	for name, invoke := range map[string]func(context.Context, *Client) (any, error){
		"durable": func(ctx context.Context, client *Client) (any, error) {
			return client.ListConversationsWithCursorDurable(ctx, 25, gmproto.ListConversationsRequest_INBOX, nil)
		},
		"legacy": func(ctx context.Context, client *Client) (any, error) {
			return client.ListConversationsWithCursor(ctx, 25, gmproto.ListConversationsRequest_INBOX, nil)
		},
	} {
		t.Run(name, func(t *testing.T) {
			var captured *gmproto.ListConversationsRequest
			client := NewClient(NewAuthData(), nil, zerolog.Nop())
			client.http = &http.Client{Transport: &conversationRequestTransport{client: client, captured: &captured, poisoned: true}}
			result, err := invoke(context.Background(), client)
			if !errors.Is(err, ErrDurablePoisoned) {
				t.Fatalf("poison wrapper error = %v", err)
			}
			switch typed := result.(type) {
			case DurableListConversationsResult:
				if typed.Response != nil || typed.Outcome != DurableOutcomeUnknown {
					t.Fatalf("durable wrapper exposed poison = %+v", typed)
				}
			case *gmproto.ListConversationsResponse:
				if typed != nil {
					t.Fatalf("legacy wrapper exposed poison = %+v", typed)
				}
			}
		})
	}
}

func newConversationRequestTestClient(t *testing.T, captured **gmproto.ListConversationsRequest) *Client {
	t.Helper()
	client := NewClient(NewAuthData(), nil, zerolog.Nop())
	client.http = &http.Client{Transport: &conversationRequestTransport{client: client, captured: captured}}
	return client
}
