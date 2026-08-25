package events

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go.mau.fi/mautrix-gmessages/pkg/libgm/gmproto"
)

type ClientReady struct {
	SessionID     string
	Conversations []*gmproto.Conversation
}

type AuthTokenRefreshed struct{}

type GaiaLoggedOut struct{}

type NoDataReceived struct{}

type AccountChange struct {
	*gmproto.AccountChangeOrSomethingEvent
	IsFake bool
}

var ErrRequestedEntityNotFound = RequestError{
	Data: &gmproto.ErrorResponse{Type: 5},
}

var ErrInvalidCredentials = RequestError{
	Data: &gmproto.ErrorResponse{Type: 16},
}

var ErrCallerNoPermission = RequestError{
	Data: &gmproto.ErrorResponse{Type: 7},
}

type RequestError struct {
	Data *gmproto.ErrorResponse
	HTTP *HTTPError
}

func (re RequestError) Unwrap() error {
	if re.HTTP == nil {
		return nil
	}
	return *re.HTTP
}

func (re RequestError) Error() string {
	if re.HTTP == nil {
		return fmt.Sprintf("provider request failed (type %d)", re.Data.GetType())
	}
	return fmt.Sprintf("HTTP %d: provider request failed (type %d)", re.HTTP.StatusCode, re.Data.GetType())
}

func (re RequestError) Is(other error) bool {
	var otherRe RequestError
	if !errors.As(other, &otherRe) {
		return re.HTTP != nil && errors.Is(*re.HTTP, other)
	}
	return otherRe.Data.GetType() == re.Data.GetType()
}

type HTTPError struct {
	Action         string        `json:"action,omitempty"`
	StatusCode     int           `json:"status_code"`
	Classification string        `json:"classification,omitempty"`
	RetryAfter     time.Duration `json:"retry_after,omitempty"`
}

func (he HTTPError) Error() string {
	if he.Action == "" {
		return fmt.Sprintf("unexpected http %d", he.StatusCode)
	}
	return fmt.Sprintf("http %d while %s", he.StatusCode, he.Action)
}

func (he HTTPError) GoString() string { return he.Error() }

func (he HTTPError) MarshalJSON() ([]byte, error) {
	type safeHTTPError HTTPError
	return json.Marshal(safeHTTPError(he))
}

type ListenFatalError struct {
	Error error
}

type ListenTemporaryError struct {
	Error error
}

type ListenRecovered struct{}

type PhoneNotResponding struct{}

type PhoneRespondingAgain struct{}

type PingFailed struct {
	Error      error
	ErrorCount int
}

type HackySetActiveMayFail struct{}
