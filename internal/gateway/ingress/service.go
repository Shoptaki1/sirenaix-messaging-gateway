// Package ingress defines the synchronous durable boundary between a provider
// poll and ACK eligibility.
package ingress

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"google.golang.org/protobuf/proto"

	"go.mau.fi/mautrix-gmessages/internal/gateway/domain"
	"go.mau.fi/mautrix-gmessages/internal/gateway/eventcontract"
	"go.mau.fi/mautrix-gmessages/internal/gateway/session"
	"go.mau.fi/mautrix-gmessages/pkg/libgm/gmproto"
)

var (
	ErrConflictingEnvelope       = errors.New("provider response ID reused with different bytes")
	ErrInvalidProviderResponseID = errors.New("invalid provider response ID")
	ErrProviderResponseCapacity  = errors.New("provider response quarantine capacity exhausted")
)

const PoisonReasonInvalidSettingsSnapshot = "invalid_settings_snapshot"

const (
	MaxRawEnvelopeBytes        = 4 << 20
	MaxProviderIdentifierBytes = domain.MaxProviderConversationIDBytes
	MaxCursorBytes             = 4 << 10
	// MaxProviderCursorAdvances is the durable per-scope pagination budget.
	// It bounds a provider that returns a long cycle which is larger than the
	// short cursor-history window, including when the gateway restarts between
	// every page. Completion/operator reset starts a new checkpoint budget.
	MaxProviderCursorAdvances = 256
	// MaxRejectedProviderResponses is the lifetime breaker capacity for a
	// connection between explicit operator resets. Identities beyond the cap
	// are not ACKed because their exact digest cannot be durably reserved.
	MaxRejectedProviderResponses    = 256
	MaxPoisonedProviderInboxEntries = 256
	// MaxPoisonedProviderInboxBytes bounds raw forensic evidence retained per
	// connection. At the 4 MiB envelope limit this admits at most eight frames
	// before fail-closed quarantine; an operator must inspect/reset to resume.
	MaxPoisonedProviderInboxBytes       = 32 << 20
	MaxProjectedConversations           = 256
	MaxProjectedMessages                = 256
	MaxProjectedLines                   = 16
	MaxAttachmentsPerMessage            = 16
	MaxProjectedAttachments             = 256
	MaxMessageTextBytes                 = 64 << 10
	MaxProjectionTextBytes              = 4 << 20
	MaxMediaLocatorBytes                = 2 << 10
	MaxMediaMetadataBytes               = 255
	MaxDeclaredMediaBytes         int64 = 25 << 20
)

type ProjectedMessage struct {
	ProviderMessageID  string
	ProviderTmpID      string
	ConversationID     string
	Direction          string
	Provenance         MessageProvenance
	ProviderStatus     string
	ProviderOccurredAt time.Time
	Actionable         bool
	// Sender and Recipients contain only exact provider-supplied E.164 values.
	// Empty means the provider event did not expose a canonical number.
	Sender     string
	Recipients []string
	Text       string
	Transport  string
	State      domain.MessageState
}

type MessageProvenance string

const (
	MessageProvenanceLive    MessageProvenance = "provider_live"
	MessageProvenanceHistory MessageProvenance = "provider_history"
	MessageProvenanceReplay  MessageProvenance = "provider_replay"
)

type ProjectedConversation struct {
	ConversationID    string
	DefaultOutgoingID string
	IsGroup           bool
}

type LineDiscoverySource string

const LineDiscoveryAuthenticatedGoogleSettings LineDiscoverySource = "authenticated_google_settings"

// ProjectedLine is a provider-authenticated SIM fact carried into the same
// fenced transaction as the raw Settings envelope. It is intentionally
// provider-neutral and never implies that the provider can select a SIM for a
// new conversation.
type ProjectedLine struct {
	ID                     domain.LineID
	TenantID               domain.TenantID
	ConnectionID           domain.ConnectionID
	ProviderParticipantID  string
	ProviderOutgoingID     string
	Phone                  string
	DisplayName            string
	CarrierName            string
	ColorHex               string
	RCSEnabled             bool
	ProviderSIMNumber      int32
	ProviderSIMPayloadType int32
	DiscoverySource        LineDiscoverySource
}

// CursorSource identifies the provider operation whose cursor was committed.
// Cursor routing must never be inferred from page contents because a valid
// provider page may be empty.
type CursorSource string

const (
	CursorSourceListMessages      CursorSource = "list_messages"
	CursorSourceListConversations CursorSource = "list_conversations"
	// ProviderPageCursorID is reserved for the parent conversation-list
	// checkpoint and can never be a provider conversation cursor target.
	ProviderPageCursorID = domain.ProviderPageCursorID
)

type Projection struct {
	Conversations []ProjectedConversation
	Messages      []ProjectedMessage
	Lines         []ProjectedLine
	// LineSnapshot means Lines is a complete, authenticated, validated provider
	// snapshot that atomically replaces the connection's active discovered set.
	// Storage may retain inactive rows solely for durable message references.
	LineSnapshot bool
	Cursor       []byte
	// CursorBase and CursorCandidate are canonical request/response cursor
	// bytes used by the repository's tenant-scoped bounded cycle history.
	// Cursor is populated only when this envelope may advance the committed
	// child cursor; parent list cursors remain candidates until children finish.
	CursorBase           []byte
	CursorCandidate      []byte
	CursorSource         CursorSource
	CursorConversationID string
}

type MediaLocator struct {
	ProviderMessageID    string
	Locator              string
	Position             int
	MIMEType             string
	DeclaredSize         int64
	DisplayFilename      string
	KeyEnvelope          session.Envelope
	ThumbnailKeyEnvelope session.Envelope
	// KeyDigest fields are SHA-256 fingerprints of the high-entropy provider
	// key bytes computed before envelope encryption. They make attachment
	// identity stable across randomized KMS sealing without persisting keys.
	KeyDigest          [32]byte
	ThumbnailKeyDigest [32]byte
}

type OutboxEvent struct {
	ID                    domain.EventID
	Type                  string
	AggregateID           string
	CanonicalBody         []byte
	PartitionTenant       domain.TenantID
	PartitionConnection   domain.ConnectionID
	PartitionConversation string
}

type Envelope struct {
	TenantID           domain.TenantID
	ConnectionID       domain.ConnectionID
	OwnerID            string
	FencingToken       uint64
	ProviderResponseID string
	Raw                []byte
	Projection         Projection
	Media              []MediaLocator
	DecodeError        error
	// ACKWithheld is reserved for a durable terminal quarantine whose provider
	// response must never enter ACK recovery. PoisonReason must identify the
	// bounded protocol condition explicitly.
	ACKWithheld  bool
	PoisonReason string
}

type EnvelopeRecord struct {
	TenantID           domain.TenantID
	ConnectionID       domain.ConnectionID
	OwnerID            string
	FencingToken       uint64
	ProviderResponseID string
	Raw                []byte
	Digest             [32]byte
	Projection         Projection
	Media              []MediaLocator
	Events             []OutboxEvent
	Poisoned           bool
	PoisonReason       string
	ACKPending         bool
	ACKWithheld        bool
	ReceivedAt         time.Time
}

type CommitResult uint8

const (
	CommitInserted CommitResult = iota + 1
	CommitDuplicate
	// CommitConflict means the repository durably recorded the collision in
	// quarantine without overwriting the original inbox row.
	CommitConflict
	// CommitPoisoned means the envelope was committed for ACK, but no caller
	// may treat its provider projection as a successful checkpoint.
	CommitPoisoned
	// CommitDuplicatePoisoned is an exact redelivery of an already committed
	// poison envelope.
	CommitDuplicatePoisoned
	// CommitDuplicateACKWithheld is an exact redelivery of a durable terminal
	// poison whose provider response must remain outside ACK recovery even if a
	// restarted decoder now interprets the same bytes differently.
	CommitDuplicateACKWithheld
)

// ACKCoordinationResult describes the exact durable subset admitted for one
// provider ACK request. ProviderError is separate from repository errors so a
// transport failure can roll back and retry without being misclassified as a
// shared database outage.
type ACKCoordinationResult struct {
	AdmittedIDs   []string
	ProviderError error
}

type Store interface {
	// CommitEnvelope is one tenant transaction containing inbox/raw, all
	// projections/statuses, media jobs, immutable events/outbox, cursor, and
	// ACK-pending state.
	CommitEnvelope(context.Context, EnvelopeRecord) (CommitResult, error)
}

type Service struct {
	store Store
	now   func() time.Time
}

func NewService(store Store) (*Service, error) {
	if store == nil {
		return nil, errors.New("durable inbox store is required")
	}
	return &Service{store: store, now: time.Now}, nil
}

type ProcessResult struct {
	ACKEligible bool
	ACKWithheld bool
	Duplicate   bool
	Poisoned    bool
}

func (service *Service) Process(ctx context.Context, envelope Envelope) (ProcessResult, error) {
	if envelope.TenantID == "" || envelope.ConnectionID == "" || envelope.OwnerID == "" || envelope.FencingToken == 0 || len(envelope.Raw) == 0 || len(envelope.Raw) > MaxRawEnvelopeBytes {
		return ProcessResult{}, domain.ErrInvalidIdentifier
	}
	if !domain.ValidProviderResponseID(envelope.ProviderResponseID) {
		return ProcessResult{}, errors.Join(ErrInvalidProviderResponseID, domain.ErrInvalidIdentifier)
	}
	if envelope.ACKWithheld && (envelope.DecodeError == nil || envelope.PoisonReason != PoisonReasonInvalidSettingsSnapshot) {
		return ProcessResult{}, domain.ErrInvalidIdentifier
	}
	record := EnvelopeRecord{
		TenantID: envelope.TenantID, ConnectionID: envelope.ConnectionID, OwnerID: envelope.OwnerID, FencingToken: envelope.FencingToken,
		ProviderResponseID: envelope.ProviderResponseID,
		Raw:                append([]byte(nil), envelope.Raw...), Digest: sha256.Sum256(envelope.Raw),
		Projection: cloneProjection(envelope.Projection), Media: cloneMedia(envelope.Media),
		ACKPending: true, ACKWithheld: envelope.ACKWithheld, ReceivedAt: service.now().UTC(),
	}
	projectionErr := ValidateProjection(envelope.Projection, envelope.Media)
	if envelope.DecodeError != nil || projectionErr != nil {
		record.Poisoned = true
		if envelope.ACKWithheld {
			record.PoisonReason = envelope.PoisonReason
		} else if envelope.DecodeError != nil {
			record.PoisonReason = "decode_failed"
		} else {
			record.PoisonReason = "projection_invalid"
		}
		record.Projection = Projection{}
		record.Media = nil
		record.Events = []OutboxEvent{buildEvent(record, "inbox.poisoned", envelope.ProviderResponseID, map[string]string{"reason": record.PoisonReason})}
	} else {
		record.Events = projectionEvents(record)
	}
	result, err := service.store.CommitEnvelope(ctx, record)
	if err != nil {
		return ProcessResult{}, err
	}
	switch result {
	case CommitInserted:
		return ProcessResult{ACKEligible: !record.ACKWithheld, ACKWithheld: record.ACKWithheld, Poisoned: record.Poisoned}, nil
	case CommitDuplicate:
		return ProcessResult{ACKEligible: !record.ACKWithheld, ACKWithheld: record.ACKWithheld, Duplicate: true, Poisoned: record.Poisoned}, nil
	case CommitPoisoned:
		return ProcessResult{ACKEligible: !record.ACKWithheld, ACKWithheld: record.ACKWithheld, Poisoned: true}, nil
	case CommitDuplicatePoisoned:
		return ProcessResult{ACKEligible: !record.ACKWithheld, ACKWithheld: record.ACKWithheld, Duplicate: true, Poisoned: true}, nil
	case CommitDuplicateACKWithheld:
		return ProcessResult{ACKWithheld: true, Duplicate: true, Poisoned: true}, nil
	case CommitConflict:
		return ProcessResult{}, ErrConflictingEnvelope
	default:
		return ProcessResult{}, errors.New("invalid inbox commit result")
	}
}

// ValidateProjection rejects provider-controlled projections before any SQL is
// built. Its limits bound both memory amplification and the number of rows a
// single envelope can add to the atomic inbox transaction.
func ValidateProjection(projection Projection, media []MediaLocator) error {
	if len(projection.Conversations) > MaxProjectedConversations || len(projection.Messages) > MaxProjectedMessages ||
		len(projection.Lines) > MaxProjectedLines ||
		len(media) > MaxProjectedAttachments || len(projection.Cursor) > MaxCursorBytes ||
		len(projection.CursorBase) > MaxCursorBytes || len(projection.CursorCandidate) > MaxCursorBytes {
		return errors.New("provider projection cardinality exceeds limit")
	}
	if projection.LineSnapshot != (len(projection.Lines) > 0) {
		return errors.New("provider line snapshot is empty or ambiguous")
	}
	lineIDs := make(map[domain.LineID]struct{}, len(projection.Lines))
	participantIDs := make(map[string]struct{}, len(projection.Lines))
	for _, line := range projection.Lines {
		if line.ID == "" || line.TenantID == "" || line.ConnectionID == "" ||
			!domain.ValidProviderIdentifier(string(line.ID)) || !domain.ValidProviderIdentifier(line.ProviderParticipantID) ||
			!domain.ValidProviderIdentifier(line.ProviderOutgoingID) || line.ProviderSIMNumber < 0 || line.ProviderSIMPayloadType < 0 ||
			line.DiscoverySource != LineDiscoveryAuthenticatedGoogleSettings ||
			!validProviderString(line.DisplayName, 255, true) || !validProviderString(line.CarrierName, 255, true) ||
			!validProviderString(line.ColorHex, 64, true) {
			return errors.New("provider line fields are invalid")
		}
		phone, phoneErr := domain.ParseE164(line.Phone)
		if phoneErr != nil || phone.String() != line.Phone {
			return errors.New("provider line phone is invalid")
		}
		if _, duplicate := lineIDs[line.ID]; duplicate {
			return errors.New("provider line identity is duplicated")
		}
		if _, duplicate := participantIDs[line.ProviderParticipantID]; duplicate {
			return errors.New("provider line participant is duplicated")
		}
		lineIDs[line.ID] = struct{}{}
		participantIDs[line.ProviderParticipantID] = struct{}{}
	}
	candidate := projection.CursorCandidate
	if len(candidate) == 0 && len(projection.Cursor) > 0 {
		candidate = projection.Cursor
	}
	if len(candidate) > 0 {
		canonicalCandidate, err := CanonicalProviderCursor(candidate)
		if err != nil {
			return errors.New("provider cursor candidate is invalid")
		}
		var canonicalBase []byte
		if len(projection.CursorBase) > 0 {
			canonicalBase, err = CanonicalProviderCursor(projection.CursorBase)
			if err != nil {
				return errors.New("provider cursor base is invalid")
			}
		}
		if len(canonicalBase) > 0 && bytes.Equal(canonicalBase, canonicalCandidate) {
			return errors.New("provider cursor did not advance")
		}
		switch projection.CursorSource {
		case CursorSourceListMessages:
			canonicalCursor, cursorErr := CanonicalProviderCursor(projection.Cursor)
			if !domain.ValidProviderConversationID(projection.CursorConversationID) || cursorErr != nil ||
				!bytes.Equal(canonicalCursor, canonicalCandidate) {
				return errors.New("provider message cursor source or target is invalid")
			}
		case CursorSourceListConversations:
			if projection.CursorConversationID != ProviderPageCursorID || len(projection.Cursor) != 0 {
				return errors.New("provider conversation cursor source or target is invalid")
			}
		default:
			return errors.New("provider cursor source or target is invalid")
		}
	} else if len(projection.Cursor) != 0 || len(projection.CursorBase) != 0 || projection.CursorSource != "" || projection.CursorConversationID != "" {
		return errors.New("provider cursor metadata without a cursor is invalid")
	}
	conversationIDs := make(map[string]struct{}, len(projection.Conversations))
	for _, conversation := range projection.Conversations {
		if !domain.ValidProviderConversationID(conversation.ConversationID) ||
			!validOptionalProviderIdentifier(conversation.DefaultOutgoingID) {
			return errors.New("provider conversation identity is invalid")
		}
		if _, duplicate := conversationIDs[conversation.ConversationID]; duplicate {
			return errors.New("provider conversation is duplicated")
		}
		conversationIDs[conversation.ConversationID] = struct{}{}
	}
	messageIDs := make(map[string]struct{}, len(projection.Messages))
	totalText := 0
	for _, message := range projection.Messages {
		if !domain.ValidProviderIdentifier(message.ProviderMessageID) ||
			!validOptionalProviderIdentifier(message.ProviderTmpID) ||
			!domain.ValidProviderConversationID(message.ConversationID) ||
			!utf8.ValidString(message.Text) || strings.IndexByte(message.Text, 0) >= 0 || len(message.Text) > MaxMessageTextBytes {
			return errors.New("provider message fields are invalid")
		}
		if message.Transport != "" && message.Transport != "sms" && message.Transport != "mms" && message.Transport != "rcs" {
			return errors.New("provider transport is invalid")
		}
		if message.Direction != "" && message.Direction != "inbound" && message.Direction != "outbound" && message.Direction != "unknown" {
			return errors.New("provider message direction is invalid")
		}
		if message.Provenance != "" && message.Provenance != MessageProvenanceLive && message.Provenance != MessageProvenanceHistory && message.Provenance != MessageProvenanceReplay {
			return errors.New("provider message provenance is invalid")
		}
		if message.Actionable && (message.Direction != "inbound" || message.Provenance != MessageProvenanceLive) {
			return errors.New("only live inbound messages may be actionable")
		}
		if len(message.ProviderStatus) > 128 || !utf8.ValidString(message.ProviderStatus) || strings.ContainsAny(message.ProviderStatus, "\x00\r\n") {
			return errors.New("provider message status is invalid")
		}
		if !message.ProviderOccurredAt.IsZero() && (message.ProviderOccurredAt.Year() < 2000 || message.ProviderOccurredAt.Year() > 2200) {
			return errors.New("provider message timestamp is invalid")
		}
		if message.Sender != "" {
			if _, err := domain.ParseE164(message.Sender); err != nil {
				return errors.New("provider message sender is invalid")
			}
		}
		for _, recipient := range message.Recipients {
			if _, err := domain.ParseE164(recipient); err != nil {
				return errors.New("provider message recipient is invalid")
			}
		}
		if message.State != "" {
			if err := message.State.Validate(); err != nil {
				return err
			}
		}
		if _, duplicate := messageIDs[message.ProviderMessageID]; duplicate {
			return errors.New("provider message is duplicated")
		}
		messageIDs[message.ProviderMessageID] = struct{}{}
		totalText += len(message.Text)
		if totalText > MaxProjectionTextBytes {
			return errors.New("provider projection text exceeds aggregate limit")
		}
	}
	positions := make(map[string]map[int]struct{}, len(messageIDs))
	for _, attachment := range media {
		if _, exists := messageIDs[attachment.ProviderMessageID]; !exists || attachment.Position < 0 || attachment.Position >= MaxAttachmentsPerMessage ||
			!validProviderString(attachment.Locator, MaxMediaLocatorBytes, false) ||
			!validProviderString(attachment.MIMEType, MaxMediaMetadataBytes, true) ||
			!validProviderString(attachment.DisplayFilename, MaxMediaMetadataBytes, true) ||
			attachment.DeclaredSize < 0 || attachment.DeclaredSize > MaxDeclaredMediaBytes {
			return errors.New("provider media fields are invalid")
		}
		if !validAttachmentKeyIdentity(attachment.KeyEnvelope, attachment.KeyDigest) ||
			!validAttachmentKeyIdentity(attachment.ThumbnailKeyEnvelope, attachment.ThumbnailKeyDigest) {
			return errors.New("provider media key identity is invalid")
		}
		perMessage := positions[attachment.ProviderMessageID]
		if perMessage == nil {
			perMessage = make(map[int]struct{}, MaxAttachmentsPerMessage)
			positions[attachment.ProviderMessageID] = perMessage
		}
		if _, duplicate := perMessage[attachment.Position]; duplicate || len(perMessage) >= MaxAttachmentsPerMessage {
			return errors.New("provider media position is duplicated or excessive")
		}
		perMessage[attachment.Position] = struct{}{}
	}
	return nil
}

// CanonicalProviderCursor validates and deterministically encodes the provider
// cursor's semantic tuple. Unknown protobuf fields and wire-field ordering do
// not create artificial pagination progress or evade durable cycle detection.
func CanonicalProviderCursor(encoded []byte) ([]byte, error) {
	if len(encoded) == 0 || len(encoded) > MaxCursorBytes {
		return nil, errors.New("provider cursor is absent or excessive")
	}
	var cursor gmproto.Cursor
	if err := proto.Unmarshal(encoded, &cursor); err != nil ||
		!domain.ValidProviderIdentifier(cursor.GetLastItemID()) || cursor.GetLastItemTimestamp() <= 0 {
		return nil, errors.New("provider cursor semantic tuple is invalid")
	}
	cursor.ProtoReflect().SetUnknown(nil)
	canonical, err := proto.MarshalOptions{Deterministic: true}.Marshal(&cursor)
	if err != nil || len(canonical) == 0 || len(canonical) > MaxCursorBytes {
		return nil, errors.New("provider cursor canonical encoding is invalid")
	}
	return canonical, nil
}

func validProviderString(value string, limit int, optional bool) bool {
	if value == "" {
		return optional
	}
	return len(value) <= limit && utf8.ValidString(value) && strings.IndexByte(value, 0) < 0 && strings.TrimSpace(value) != ""
}

func validOptionalProviderIdentifier(value string) bool {
	return value == "" || domain.ValidProviderIdentifier(value)
}

func cloneProjection(input Projection) Projection {
	output := Projection{
		Conversations: append([]ProjectedConversation(nil), input.Conversations...),
		Messages:      append([]ProjectedMessage(nil), input.Messages...), Lines: append([]ProjectedLine(nil), input.Lines...), LineSnapshot: input.LineSnapshot,
		Cursor:     append([]byte(nil), input.Cursor...),
		CursorBase: append([]byte(nil), input.CursorBase...), CursorCandidate: append([]byte(nil), input.CursorCandidate...),
		CursorSource: input.CursorSource, CursorConversationID: input.CursorConversationID,
	}
	for index := range output.Messages {
		output.Messages[index].Recipients = append([]string(nil), input.Messages[index].Recipients...)
	}
	return output
}

func cloneMedia(input []MediaLocator) []MediaLocator {
	output := make([]MediaLocator, len(input))
	copy(output, input)
	for index := range output {
		output[index].KeyEnvelope = input[index].KeyEnvelope.Clone()
		output[index].ThumbnailKeyEnvelope = input[index].ThumbnailKeyEnvelope.Clone()
	}
	return output
}

func projectionEvents(record EnvelopeRecord) []OutboxEvent {
	events := make([]OutboxEvent, 0, len(record.Projection.Conversations)+len(record.Projection.Messages)+len(record.Media))
	for _, conversation := range record.Projection.Conversations {
		events = append(events, buildEvent(record, "conversation.updated", conversation.ConversationID, map[string]string{
			"conversation_id": conversation.ConversationID,
		}))
	}
	for _, message := range record.Projection.Messages {
		eventType := "message.updated"
		if message.Actionable {
			eventType = "message.received"
		} else if message.Provenance == MessageProvenanceHistory || message.Provenance == MessageProvenanceReplay {
			eventType = "message.imported"
		}
		events = append(events, buildEvent(record, eventType, message.ProviderMessageID, map[string]string{
			"provider_message_id": message.ProviderMessageID,
			"conversation_id":     message.ConversationID,
		}))
	}
	for _, media := range record.Media {
		aggregateID := AttachmentAggregateID(media)
		events = append(events, buildStableAggregateEvent(record, "media.pending", aggregateID, map[string]string{
			"provider_message_id": media.ProviderMessageID, "attachment_index": strconv.Itoa(media.Position),
		}))
	}
	if len(events) == 0 {
		events = append(events, buildEvent(record, "inbox.received", record.ProviderResponseID, map[string]string{
			"provider_response_id": record.ProviderResponseID,
		}))
	}
	return events
}

// AttachmentIdentityDigest binds every provider-controlled field that affects
// fetching or presentation. MIME, size, and filename use exact-match semantics:
// any change at an occupied provider message position is quarantined. Provider
// key fingerprints make the identity stable even though KMS envelope sealing is
// intentionally randomized. Tests and legacy callers without fingerprints use
// the exact sealed envelope representation as a fail-closed fallback.
func AttachmentIdentityDigest(locator MediaLocator) [32]byte {
	digest := sha256.New()
	writeIdentityField(digest, []byte(locator.Locator))
	writeIdentityField(digest, []byte(locator.MIMEType))
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(locator.DeclaredSize))
	writeIdentityField(digest, size[:])
	writeIdentityField(digest, []byte(locator.DisplayFilename))
	writeKeyIdentity(digest, locator.KeyDigest, locator.KeyEnvelope)
	writeKeyIdentity(digest, locator.ThumbnailKeyDigest, locator.ThumbnailKeyEnvelope)
	var result [32]byte
	copy(result[:], digest.Sum(nil))
	return result
}

func AttachmentAggregateID(locator MediaLocator) string {
	digest := sha256.New()
	writeIdentityField(digest, []byte(locator.ProviderMessageID))
	var position [8]byte
	binary.BigEndian.PutUint64(position[:], uint64(locator.Position))
	writeIdentityField(digest, position[:])
	identity := AttachmentIdentityDigest(locator)
	writeIdentityField(digest, identity[:])
	return "attachment_" + stringHex(digest.Sum(nil)[:16])
}

func validAttachmentKeyIdentity(envelope session.Envelope, fingerprint [32]byte) bool {
	emptyEnvelope := len(envelope.Ciphertext) == 0 && len(envelope.WrappedDEK) == 0 && len(envelope.Nonce) == 0 && envelope.KeyID == "" && envelope.KeyVersion == 0 && envelope.Version == 0 && envelope.Provider == "" && envelope.Revision == 0
	emptyFingerprint := fingerprint == [32]byte{}
	if emptyEnvelope {
		return emptyFingerprint
	}
	if envelope.Validate() != nil {
		return false
	}
	return true
}

type identityWriter interface{ Write([]byte) (int, error) }

func writeIdentityField(destination identityWriter, value []byte) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = destination.Write(length[:])
	_, _ = destination.Write(value)
}

func writeKeyIdentity(destination identityWriter, fingerprint [32]byte, envelope session.Envelope) {
	if fingerprint != [32]byte{} {
		writeIdentityField(destination, []byte{1})
		writeIdentityField(destination, fingerprint[:])
		return
	}
	writeIdentityField(destination, []byte{0})
	writeIdentityField(destination, []byte(strconv.FormatUint(envelope.Revision, 10)))
	writeIdentityField(destination, []byte(strconv.Itoa(envelope.Version)))
	writeIdentityField(destination, []byte(envelope.Provider))
	writeIdentityField(destination, envelope.Ciphertext)
	writeIdentityField(destination, envelope.WrappedDEK)
	writeIdentityField(destination, envelope.Nonce)
	writeIdentityField(destination, []byte(envelope.KeyID))
	writeIdentityField(destination, []byte(strconv.Itoa(envelope.KeyVersion)))
}

func buildStableAggregateEvent(record EnvelopeRecord, eventType, aggregateID string, data map[string]string) OutboxEvent {
	digest := sha256.Sum256([]byte(string(record.TenantID) + "\x00" + string(record.ConnectionID) + "\x00" + eventType + "\x00" + aggregateID))
	return contractEvent(record, domain.EventID("evt_"+stringHex(digest[:16])), eventType, aggregateID, data)
}

func buildEvent(record EnvelopeRecord, eventType, aggregateID string, data map[string]string) OutboxEvent {
	digest := sha256.Sum256([]byte(string(record.TenantID) + "\x00" + string(record.ConnectionID) + "\x00" + record.ProviderResponseID + "\x00" + eventType + "\x00" + aggregateID))
	return contractEvent(record, domain.EventID("evt_"+stringHex(digest[:16])), eventType, aggregateID, data)
}

func contractEvent(record EnvelopeRecord, eventID domain.EventID, eventType, aggregateID string, data map[string]string) OutboxEvent {
	legacy := make(map[string]any, len(data))
	for key, value := range data {
		legacy[key] = value
	}
	envelope := eventcontract.Envelope{
		EventID: string(eventID), Type: eventType, OccurredAt: record.ReceivedAt,
		TenantID: string(record.TenantID), ConnectionID: string(record.ConnectionID),
		ConversationID: data["conversation_id"], ProviderMessageID: data["provider_message_id"],
		ProviderResponseID: data["provider_response_id"], Reason: data["reason"], Data: legacy,
	}
	if strings.HasPrefix(eventType, "message.") {
		for _, message := range record.Projection.Messages {
			if message.ProviderMessageID != aggregateID {
				continue
			}
			envelope.Direction = message.Direction
			envelope.Sender = message.Sender
			envelope.Recipients = append([]string(nil), message.Recipients...)
			envelope.Text = message.Text
			envelope.Transport = message.Transport
			envelope.Status = string(message.State)
			envelope.State = string(message.State)
			envelope.Provenance = string(message.Provenance)
			envelope.ProviderStatus = message.ProviderStatus
			envelope.Actionable = message.Actionable
			envelope.IngestedAt = record.ReceivedAt
			if !message.ProviderOccurredAt.IsZero() {
				envelope.OccurredAt = message.ProviderOccurredAt
			}
			break
		}
	}
	body, _ := eventcontract.Marshal(envelope)
	return OutboxEvent{
		ID: eventID, Type: eventType, AggregateID: aggregateID,
		CanonicalBody: body, PartitionTenant: record.TenantID, PartitionConnection: record.ConnectionID,
		PartitionConversation: envelope.ConversationID,
	}
}

const lowerHex = "0123456789abcdef"

func stringHex(input []byte) string {
	output := make([]byte, len(input)*2)
	for index, value := range input {
		output[index*2] = lowerHex[value>>4]
		output[index*2+1] = lowerHex[value&0x0f]
	}
	return string(output)
}
