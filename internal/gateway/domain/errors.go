package domain

import "errors"

var (
	ErrInvalidTenantID        = errors.New("invalid tenant ID")
	ErrInvalidIdentifier      = errors.New("invalid identifier")
	ErrInvalidPhoneNumber     = errors.New("invalid E.164 phone number")
	ErrInvalidLabelSlug       = errors.New("invalid label slug")
	ErrInvalidConnectionState = errors.New("invalid connection state")
	ErrTenantBoundary         = errors.New("tenant boundary violation")
	ErrConnectionBoundary     = errors.New("connection boundary violation")
	ErrInvalidMessageState    = errors.New("invalid message state")
)
