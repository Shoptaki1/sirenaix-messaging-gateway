package webhook

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"io"
	"net/url"
	"strings"
	"time"

	"go.mau.fi/mautrix-gmessages/internal/gateway/domain"
	"go.mau.fi/mautrix-gmessages/internal/gateway/session"
)

const (
	secretSize                   = 32
	DefaultMaxEndpointsPerTenant = 32
	MaxEndpointsPerTenant        = 128
)

var ErrInvalidEndpoint = errors.New("invalid webhook endpoint")
var ErrEndpointQuotaExceeded = errors.New("tenant webhook endpoint quota exceeded")

type Endpoint struct {
	ID          string
	TenantID    domain.TenantID
	Destination string
	KeyID       string
	Active      bool
	CreatedAt   time.Time
}

type EndpointRecord struct {
	Endpoint Endpoint
	Secret   session.Envelope
}

type EndpointRotation struct {
	TenantID           domain.TenantID
	EndpointID         string
	KeyID              string
	Secret             session.Envelope
	PreviousValidUntil time.Time
}

type CreatedEndpoint struct {
	Endpoint Endpoint
	Secret   string
}

type DeleteResult struct {
	Deleting bool
}

type EndpointListOptions struct {
	After string
	Limit int
}

type EndpointPage struct {
	Endpoints  []Endpoint
	NextCursor string
}

type EndpointStore interface {
	CreateEndpoint(context.Context, EndpointRecord, int) error
	RotateEndpoint(context.Context, EndpointRotation) error
	ListEndpoints(context.Context, domain.TenantID, EndpointListOptions) (EndpointPage, error)
	DeleteEndpoint(context.Context, domain.TenantID, string) (DeleteResult, error)
	ReplayDLQ(context.Context, domain.TenantID, string) error
}

type SecretSealer interface {
	Seal(context.Context, session.Scope, []byte) (session.Envelope, error)
}

type EndpointConfig struct {
	Store         EndpointStore
	Secrets       SecretSealer
	Destinations  DestinationGuard
	NewID         func() string
	Random        io.Reader
	Now           func() time.Time
	RotationGrace time.Duration
	MaxEndpoints  int
}

type EndpointService struct {
	store         EndpointStore
	secrets       SecretSealer
	destinations  DestinationGuard
	newID         func() string
	random        io.Reader
	now           func() time.Time
	rotationGrace time.Duration
	maxEndpoints  int
}

func NewEndpointService(config EndpointConfig) (*EndpointService, error) {
	if config.Store == nil || config.Secrets == nil || config.Destinations == nil || config.NewID == nil {
		return nil, ErrInvalidEndpoint
	}
	if config.Random == nil {
		config.Random = rand.Reader
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	if config.RotationGrace == 0 {
		config.RotationGrace = 24 * time.Hour
	}
	if config.MaxEndpoints == 0 {
		config.MaxEndpoints = DefaultMaxEndpointsPerTenant
	}
	if config.RotationGrace < 0 || config.RotationGrace > 7*24*time.Hour {
		return nil, ErrInvalidEndpoint
	}
	if config.MaxEndpoints < 1 || config.MaxEndpoints > MaxEndpointsPerTenant {
		return nil, ErrInvalidEndpoint
	}
	return &EndpointService{
		store: config.Store, secrets: config.Secrets, destinations: config.Destinations,
		newID: config.NewID, random: config.Random, now: config.Now, rotationGrace: config.RotationGrace,
		maxEndpoints: config.MaxEndpoints,
	}, nil
}

func (service *EndpointService) Create(ctx context.Context, tenantID domain.TenantID, destination string) (CreatedEndpoint, error) {
	if tenantID == "" || !validDestinationSyntax(destination) {
		return CreatedEndpoint{}, ErrInvalidEndpoint
	}
	if _, err := service.destinations.ClientFor(ctx, destination); err != nil {
		return CreatedEndpoint{}, ErrUnsafeDestination
	}
	endpointID, keyID := service.newID(), service.newID()
	if endpointID == "" || keyID == "" || endpointID == keyID {
		return CreatedEndpoint{}, ErrInvalidEndpoint
	}
	secret, envelope, encoded, err := service.generateSecret(ctx, tenantID, endpointID)
	zero(secret)
	if err != nil {
		return CreatedEndpoint{}, err
	}
	endpoint := Endpoint{
		ID: endpointID, TenantID: tenantID, Destination: destination,
		KeyID: keyID, Active: true, CreatedAt: service.now().UTC(),
	}
	if err = service.store.CreateEndpoint(ctx, EndpointRecord{Endpoint: endpoint, Secret: envelope}, service.maxEndpoints); err != nil {
		return CreatedEndpoint{}, err
	}
	return CreatedEndpoint{Endpoint: endpoint, Secret: encoded}, nil
}

func (service *EndpointService) Rotate(ctx context.Context, tenantID domain.TenantID, endpointID string) (string, error) {
	if tenantID == "" || endpointID == "" {
		return "", ErrInvalidEndpoint
	}
	keyID := service.newID()
	if keyID == "" {
		return "", ErrInvalidEndpoint
	}
	secret, envelope, encoded, err := service.generateSecret(ctx, tenantID, endpointID)
	zero(secret)
	if err != nil {
		return "", err
	}
	if err = service.store.RotateEndpoint(ctx, EndpointRotation{
		TenantID: tenantID, EndpointID: endpointID, KeyID: keyID, Secret: envelope,
		PreviousValidUntil: service.now().UTC().Add(service.rotationGrace),
	}); err != nil {
		return "", err
	}
	return encoded, nil
}

func (service *EndpointService) List(ctx context.Context, tenantID domain.TenantID, options EndpointListOptions) (EndpointPage, error) {
	if tenantID == "" || options.Limit < 1 || options.Limit > 200 || len(options.After) > 256 {
		return EndpointPage{}, ErrInvalidEndpoint
	}
	page, err := service.store.ListEndpoints(ctx, tenantID, options)
	if err != nil {
		return EndpointPage{}, err
	}
	for _, endpoint := range page.Endpoints {
		if endpoint.TenantID != tenantID {
			return EndpointPage{}, errors.New("webhook store returned a cross-tenant endpoint")
		}
	}
	return page, nil
}

func (service *EndpointService) Delete(ctx context.Context, tenantID domain.TenantID, endpointID string) (DeleteResult, error) {
	if tenantID == "" || endpointID == "" {
		return DeleteResult{}, ErrInvalidEndpoint
	}
	return service.store.DeleteEndpoint(ctx, tenantID, endpointID)
}

func (service *EndpointService) Replay(ctx context.Context, tenantID domain.TenantID, dlqID string) error {
	if tenantID == "" || dlqID == "" {
		return ErrInvalidEndpoint
	}
	return service.store.ReplayDLQ(ctx, tenantID, dlqID)
}

func (service *EndpointService) generateSecret(ctx context.Context, tenantID domain.TenantID, endpointID string) ([]byte, session.Envelope, string, error) {
	secret := make([]byte, secretSize)
	if _, err := io.ReadFull(service.random, secret); err != nil {
		zero(secret)
		return nil, session.Envelope{}, "", err
	}
	envelope, err := service.secrets.Seal(ctx, session.Scope{
		TenantID: string(tenantID), ConnectionID: endpointID, Provider: "webhook",
	}, secret)
	if err != nil {
		zero(secret)
		return nil, session.Envelope{}, "", err
	}
	encoded := base64.RawURLEncoding.EncodeToString(secret)
	return secret, envelope, encoded, nil
}

func validDestinationSyntax(destination string) bool {
	if strings.TrimSpace(destination) != destination || len(destination) > 2048 {
		return false
	}
	parsed, err := url.Parse(destination)
	return err == nil && parsed.Scheme == "https" && parsed.Hostname() != "" && parsed.User == nil && parsed.Fragment == ""
}
