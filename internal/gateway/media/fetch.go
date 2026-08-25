package media

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"go.mau.fi/mautrix-gmessages/internal/gateway/domain"
	"go.mau.fi/mautrix-gmessages/internal/gateway/eventcontract"
	"go.mau.fi/mautrix-gmessages/internal/gateway/session"
)

type FetchJob struct {
	TenantID             domain.TenantID
	JobID                string
	MediaID              domain.MediaID
	ConnectionID         domain.ConnectionID
	ProviderMessageID    string
	Locator              string
	DeclaredMIME         string
	DeclaredSize         int64
	DisplayFilename      string
	KeyEnvelope          session.Envelope
	ThumbnailKeyEnvelope session.Envelope
	AttemptCount         int
	OwnerID              string
	ClaimToken           uint64
	Imported             *Record
}

var ErrFetchFenceLost = errors.New("media fetch claim lost")

type FetchContent struct {
	Body          io.ReadCloser
	ContentLength int64
	MIMEType      string
	Filename      string
}

// ActorFetcher is implemented by the currently fenced connection actor. It is
// the only permitted boundary for Google locator/decryption I/O.
type ActorFetcher interface {
	Fetch(context.Context, FetchJob) (FetchContent, error)
}

type FetchStore interface {
	ClaimFetch(context.Context, domain.TenantID, string) (FetchJob, bool, error)
	RenewFetch(context.Context, FetchJob) (bool, error)
	CompleteReady(context.Context, FetchJob, Record, string, []byte) error
	// A zero retryAt is terminal and must atomically mark the object failed and
	// persist the supplied media.failed event/outbox. A nonzero retryAt requeues.
	CompleteFailed(context.Context, FetchJob, string, string, []byte, time.Time) error
}

type AssignedImporter interface {
	Import(context.Context, domain.TenantID, domain.MediaID, Upload) (Record, error)
	Verify(context.Context, domain.TenantID, domain.MediaID, Record) (Record, error)
}

type FetchWorkerConfig struct {
	TenantID      domain.TenantID
	OwnerID       string
	Store         FetchStore
	Importer      AssignedImporter
	Fetcher       ActorFetcher
	NewID         func() string
	Now           func() time.Time
	MaxAttempts   int
	RetryDelay    func(int) time.Duration
	RenewInterval time.Duration
}

type FetchWorker struct {
	tenantID      domain.TenantID
	ownerID       string
	store         FetchStore
	importer      AssignedImporter
	fetcher       ActorFetcher
	newID         func() string
	now           func() time.Time
	maxAttempts   int
	retryDelay    func(int) time.Duration
	renewInterval time.Duration
}

func NewFetchWorker(config FetchWorkerConfig) (*FetchWorker, error) {
	if config.TenantID == "" || config.OwnerID == "" || len(config.OwnerID) > 256 || config.Store == nil || config.Importer == nil || config.Fetcher == nil || config.NewID == nil {
		return nil, domain.ErrInvalidIdentifier
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	if config.MaxAttempts == 0 {
		config.MaxAttempts = 5
	}
	if config.MaxAttempts < 1 || config.MaxAttempts > 100 {
		return nil, domain.ErrInvalidIdentifier
	}
	if config.RetryDelay == nil {
		config.RetryDelay = func(attempt int) time.Duration {
			delay := time.Second << min(attempt, 10)
			return min(delay, 15*time.Minute)
		}
	}
	if config.RenewInterval == 0 {
		config.RenewInterval = 20 * time.Second
	}
	if config.RenewInterval < time.Millisecond || config.RenewInterval > 30*time.Second {
		return nil, domain.ErrInvalidIdentifier
	}
	return &FetchWorker{
		tenantID: config.TenantID, ownerID: config.OwnerID, store: config.Store,
		importer: config.Importer, fetcher: config.Fetcher, newID: config.NewID,
		now: config.Now, maxAttempts: config.MaxAttempts, retryDelay: config.RetryDelay, renewInterval: config.RenewInterval,
	}, nil
}

func (worker *FetchWorker) RunOne(ctx context.Context) (bool, error) {
	job, ok, err := worker.store.ClaimFetch(ctx, worker.tenantID, worker.ownerID)
	if err != nil || !ok {
		return false, err
	}
	if job.TenantID != worker.tenantID || job.JobID == "" || job.MediaID == "" || job.ConnectionID == "" || job.OwnerID != worker.ownerID || job.ClaimToken == 0 || job.AttemptCount < 1 {
		return true, errors.New("invalid media fetch claim")
	}
	workCtx, cancelWork := context.WithCancel(ctx)
	renewCtx, stopRenew := context.WithCancel(context.Background())
	renewResult := make(chan error, 1)
	go worker.renewClaim(renewCtx, cancelWork, job, renewResult)
	workErr := worker.processClaim(workCtx, job)
	stopRenew()
	cancelWork()
	renewErr := <-renewResult
	if renewErr != nil {
		return true, renewErr
	}
	return true, workErr
}

func (worker *FetchWorker) processClaim(ctx context.Context, job FetchJob) error {
	if job.Imported != nil {
		record, err := worker.importer.Verify(ctx, job.TenantID, job.MediaID, *job.Imported)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return worker.completeFailure(ctx, job, err)
		}
		return worker.completeReady(ctx, job, record)
	}
	content, fetchErr := worker.fetcher.Fetch(ctx, job)
	if fetchErr == nil {
		if content.Body == nil {
			fetchErr = errors.New("empty provider media body")
		} else {
			defer content.Body.Close()
			filename := content.Filename
			if filename == "" {
				filename = job.DisplayFilename
			}
			declaredMIME := content.MIMEType
			if declaredMIME == "" {
				declaredMIME = job.DeclaredMIME
			}
			record, importErr := worker.importer.Import(ctx, job.TenantID, job.MediaID, Upload{
				Body: content.Body, ContentLength: content.ContentLength, DeclaredMIME: declaredMIME, Filename: filename,
			})
			if importErr == nil {
				return worker.completeReady(ctx, job, record)
			}
			fetchErr = importErr
		}
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return worker.completeFailure(ctx, job, fetchErr)
}

func (worker *FetchWorker) completeReady(ctx context.Context, job FetchJob, record Record) error {
	eventID := worker.newID()
	body, err := mediaEventBody(eventID, "media.ready", job, worker.now().UTC(), "")
	if eventID == "" || err != nil {
		return errors.New("create media ready event")
	}
	return worker.store.CompleteReady(ctx, job, record, eventID, body)
}

func (worker *FetchWorker) renewClaim(ctx context.Context, cancel context.CancelFunc, job FetchJob, result chan<- error) {
	ticker := time.NewTicker(worker.renewInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			result <- nil
			return
		case <-ticker.C:
			owned, err := worker.store.RenewFetch(ctx, job)
			if err != nil || !owned {
				cancel()
				if err == nil {
					err = ErrFetchFenceLost
				}
				result <- err
				return
			}
		}
	}
}

func (worker *FetchWorker) completeFailure(ctx context.Context, job FetchJob, cause error) error {
	reason := safeFetchReason(cause)
	terminal := job.AttemptCount >= worker.maxAttempts || reason == "unsafe_locator" || reason == "invalid_media"
	if !terminal {
		retryAt := worker.now().UTC().Add(worker.retryDelay(job.AttemptCount))
		return worker.store.CompleteFailed(ctx, job, reason, "", nil, retryAt)
	}
	eventID := worker.newID()
	body, err := mediaEventBody(eventID, "media.failed", job, worker.now().UTC(), reason)
	if eventID == "" || err != nil {
		return errors.New("create media failed event")
	}
	return worker.store.CompleteFailed(ctx, job, reason, eventID, body, time.Time{})
}

func mediaEventBody(eventID, eventType string, job FetchJob, occurredAt time.Time, reason string) ([]byte, error) {
	status := "ready"
	metadataPath := "/v1/media/" + string(job.MediaID)
	contentPath := "/v1/media/" + string(job.MediaID) + "/content"
	if eventType == "media.failed" {
		status = "failed"
		contentPath = ""
	}
	return eventcontract.Marshal(eventcontract.Envelope{
		EventID: eventID, Type: eventType, OccurredAt: occurredAt,
		TenantID: string(job.TenantID), ConnectionID: string(job.ConnectionID),
		ProviderMessageID: job.ProviderMessageID, MediaID: string(job.MediaID), MetadataPath: metadataPath,
		ContentPath: contentPath, Status: status, Reason: reason,
		Media: []eventcontract.Media{{
			ID: string(job.MediaID), Status: status, MetadataPath: metadataPath, ContentPath: contentPath,
		}},
	})
}

func safeFetchReason(err error) string {
	switch {
	case errors.Is(err, ErrUnsafeURL), errors.Is(err, ErrRedirectDenied):
		return "unsafe_locator"
	case errors.Is(err, ErrTooLarge), errors.Is(err, ErrLengthMismatch), errors.Is(err, ErrUnsupportedImage), errors.Is(err, ErrPixelLimit):
		return "invalid_media"
	case errors.Is(err, ErrNotFound), errors.Is(err, ErrStoredMediaCorrupt):
		return "invalid_media"
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return "fetch_canceled"
	default:
		return "fetch_failed"
	}
}

func (job FetchJob) String() string {
	return fmt.Sprintf("media fetch job %s/%s", job.TenantID, job.JobID)
}
