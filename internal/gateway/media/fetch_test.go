package media_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"
	"time"

	"go.mau.fi/mautrix-gmessages/internal/gateway/domain"
	"go.mau.fi/mautrix-gmessages/internal/gateway/media"
)

type fetchJobStore struct {
	job           media.FetchJob
	claimed       bool
	ready         media.Record
	readyBody     []byte
	failed        bool
	failedBody    []byte
	failedEventID string
	retryAt       time.Time
	reason        string
	renewed       int
	renewOK       bool
}

func (store *fetchJobStore) ClaimFetch(context.Context, domain.TenantID, string) (media.FetchJob, bool, error) {
	return store.job, store.claimed, nil
}
func (store *fetchJobStore) CompleteReady(_ context.Context, job media.FetchJob, record media.Record, _ string, body []byte) error {
	store.ready, store.readyBody = record, append([]byte(nil), body...)
	return nil
}
func (store *fetchJobStore) CompleteFailed(_ context.Context, _ media.FetchJob, reason string, eventID string, body []byte, retryAt time.Time) error {
	store.failed, store.reason, store.retryAt = true, reason, retryAt
	store.failedEventID, store.failedBody = eventID, append([]byte(nil), body...)
	return nil
}
func (store *fetchJobStore) RenewFetch(context.Context, media.FetchJob) (bool, error) {
	store.renewed++
	return store.renewOK, nil
}

type actorFetcher struct {
	content media.FetchContent
	err     error
	calls   *int
	block   bool
}

func (fetcher actorFetcher) Fetch(ctx context.Context, _ media.FetchJob) (media.FetchContent, error) {
	if fetcher.calls != nil {
		*fetcher.calls++
	}
	if fetcher.block {
		<-ctx.Done()
		return media.FetchContent{}, ctx.Err()
	}
	return fetcher.content, fetcher.err
}

type assignedImporter struct {
	record      media.Record
	verifyCalls int
	verifyErr   error
}

func (importer *assignedImporter) Import(_ context.Context, tenant domain.TenantID, id domain.MediaID, upload media.Upload) (media.Record, error) {
	if tenant == "" || id == "" || upload.Body == nil {
		return media.Record{}, errors.New("invalid import")
	}
	_, _ = io.Copy(io.Discard, upload.Body)
	importer.record.ID, importer.record.TenantID = id, tenant
	return importer.record, nil
}
func (importer *assignedImporter) Verify(_ context.Context, tenant domain.TenantID, id domain.MediaID, expected media.Record) (media.Record, error) {
	importer.verifyCalls++
	if importer.verifyErr != nil {
		return media.Record{}, importer.verifyErr
	}
	if expected.TenantID != tenant || expected.ID != id || len(expected.SHA256) != 32 || expected.Size < 1 {
		return media.Record{}, errors.New("invalid ready import")
	}
	return expected, nil
}

func TestFetchWorkerBoundsImportedReadyVerificationFailures(t *testing.T) {
	ready := media.Record{
		ID: "media-a", TenantID: "tenant-a", ObjectKey: "objects/a/b", MIMEType: "image/png", Size: 4,
		SHA256: bytes.Repeat([]byte{7}, 32), Width: 1, Height: 1, DisplayFilename: "a.png", State: "ready",
	}
	for _, test := range []struct {
		name      string
		attempt   int
		verifyErr error
		wantRetry bool
		reason    string
	}{
		{name: "operational retry", attempt: 1, verifyErr: errors.New("object backend unavailable"), wantRetry: true, reason: "fetch_failed"},
		{name: "operational exhausted", attempt: 3, verifyErr: errors.New("object backend unavailable"), reason: "fetch_failed"},
		{name: "definitive missing", attempt: 1, verifyErr: media.ErrNotFound, reason: "invalid_media"},
		{name: "definitive corrupt", attempt: 1, verifyErr: media.ErrStoredMediaCorrupt, reason: "invalid_media"},
	} {
		t.Run(test.name, func(t *testing.T) {
			job := media.FetchJob{
				TenantID: "tenant-a", JobID: "job-a", MediaID: "media-a", ConnectionID: "connection-a",
				ProviderMessageID: "provider-a", AttemptCount: test.attempt, OwnerID: "worker-a", ClaimToken: uint64(test.attempt), Imported: &ready,
			}
			store := &fetchJobStore{job: job, claimed: true, renewOK: true}
			worker, err := media.NewFetchWorker(media.FetchWorkerConfig{
				TenantID: "tenant-a", OwnerID: "worker-a", Store: store,
				Importer: &assignedImporter{verifyErr: test.verifyErr}, Fetcher: actorFetcher{},
				NewID: func() string { return "event-failed" }, MaxAttempts: 3,
				RetryDelay: func(int) time.Duration { return time.Minute }, RenewInterval: time.Millisecond,
			})
			if err != nil {
				t.Fatal(err)
			}
			if worked, runErr := worker.RunOne(context.Background()); runErr != nil || !worked || !store.failed {
				t.Fatalf("RunOne() = (%v, %v), failed=%v", worked, runErr, store.failed)
			}
			if gotRetry := !store.retryAt.IsZero(); gotRetry != test.wantRetry || store.reason != test.reason {
				t.Fatalf("retry=%v reason=%q, want retry=%v reason=%q", gotRetry, store.reason, test.wantRetry, test.reason)
			}
		})
	}
}

func TestFetchWorkerClaimsActorFetchAndCompletesReadyEvent(t *testing.T) {
	job := media.FetchJob{
		TenantID: "tenant-a", JobID: "job-a", MediaID: "media-a", ConnectionID: "connection-a",
		ProviderMessageID: "provider-a", Locator: "https://allowed.example/object", AttemptCount: 1,
		OwnerID: "worker-a", ClaimToken: 4,
	}
	store := &fetchJobStore{job: job, claimed: true}
	importer := &assignedImporter{record: media.Record{MIMEType: "image/png", Size: 4, State: "ready"}}
	worker, err := media.NewFetchWorker(media.FetchWorkerConfig{
		TenantID: "tenant-a", OwnerID: "worker-a", Store: store, Importer: importer,
		Fetcher: actorFetcher{content: media.FetchContent{Body: io.NopCloser(bytes.NewReader([]byte("data"))), ContentLength: 4, MIMEType: "image/png", Filename: "a.png"}},
		NewID:   func() string { return "event-ready" }, Now: func() time.Time { return time.Unix(1700000000, 0).UTC() },
	})
	if err != nil {
		t.Fatal(err)
	}
	didWork, err := worker.RunOne(context.Background())
	if err != nil || !didWork || store.ready.ID != "media-a" || store.failed {
		t.Fatalf("RunOne() = (%v, %v), ready=%+v failed=%v", didWork, err, store.ready, store.failed)
	}
	if !bytes.Contains(store.readyBody, []byte(`"type":"media.ready"`)) || bytes.Contains(store.readyBody, []byte(job.Locator)) {
		t.Fatalf("canonical ready event is unsafe: %s", store.readyBody)
	}
	var event map[string]any
	if err = json.Unmarshal(store.readyBody, &event); err != nil || event["event_id"] != "event-ready" || event["version"] != float64(1) ||
		event["tenant_id"] != "tenant-a" || event["connection_id"] != "connection-a" || event["media_id"] != "media-a" ||
		event["provider_message_id"] != "provider-a" || event["status"] != "ready" || event["content_path"] != "/v1/media/media-a/content" ||
		event["occurred_at"] != "2023-11-14T22:13:20Z" {
		t.Fatalf("media ready event contract = %#v, error %v", event, err)
	}
}

func TestFetchWorkerRetriesSafeFailureThenDeadLettersMedia(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	for _, test := range []struct {
		name       string
		attempts   int
		wantRetry  bool
		fetchError error
	}{
		{name: "transient", attempts: 1, wantRetry: true, fetchError: errors.New("secret remote failure")},
		{name: "exhausted", attempts: 3, fetchError: errors.New("secret remote failure")},
		{name: "unsafe", attempts: 1, fetchError: media.ErrUnsafeURL},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := &fetchJobStore{claimed: true, job: media.FetchJob{
				TenantID: "tenant-a", JobID: "job-a", MediaID: "media-a", ConnectionID: "connection-a",
				ProviderMessageID: "provider-a", AttemptCount: test.attempts, OwnerID: "worker-a", ClaimToken: 2,
			}}
			worker, err := media.NewFetchWorker(media.FetchWorkerConfig{
				TenantID: "tenant-a", OwnerID: "worker-a", Store: store, Importer: &assignedImporter{},
				Fetcher: actorFetcher{err: test.fetchError}, NewID: func() string { return "event-failed" },
				Now: func() time.Time { return now }, MaxAttempts: 3, RetryDelay: func(int) time.Duration { return time.Minute },
			})
			if err != nil {
				t.Fatal(err)
			}
			if didWork, runErr := worker.RunOne(context.Background()); runErr != nil || !didWork || !store.failed {
				t.Fatalf("RunOne() = (%v, %v), failed=%v", didWork, runErr, store.failed)
			}
			if test.wantRetry != !store.retryAt.IsZero() {
				t.Fatalf("retryAt=%v wantRetry=%v", store.retryAt, test.wantRetry)
			}
			if store.reason == "" || bytes.Contains([]byte(store.reason), []byte("secret")) {
				t.Fatalf("unsafe failure reason %q", store.reason)
			}
			if !test.wantRetry {
				var event map[string]any
				if err = json.Unmarshal(store.failedBody, &event); err != nil || store.failedEventID != "event-failed" ||
					event["type"] != "media.failed" || event["media_id"] != "media-a" || event["status"] != "failed" ||
					event["reason"] != store.reason || event["metadata_path"] != "/v1/media/media-a" ||
					bytes.Contains(store.failedBody, []byte("secret")) {
					t.Fatalf("safe terminal media event = %#v body=%s error=%v", event, store.failedBody, err)
				}
			}
		})
	}
}

func TestFetchWorkerCrashAfterImportVerifiesExistingBytesAndCompletesWithoutRefetch(t *testing.T) {
	ready := media.Record{
		ID: "media-a", TenantID: "tenant-a", ObjectKey: "objects/a/b", MIMEType: "image/png", Size: 4,
		SHA256: bytes.Repeat([]byte{7}, 32), Width: 1, Height: 1, DisplayFilename: "a.png", State: "ready",
	}
	job := media.FetchJob{
		TenantID: "tenant-a", JobID: "job-a", MediaID: "media-a", ConnectionID: "connection-a",
		ProviderMessageID: "provider-a", AttemptCount: 2, OwnerID: "worker-a", ClaimToken: 8, Imported: &ready,
	}
	store := &fetchJobStore{job: job, claimed: true, renewOK: true}
	importer := &assignedImporter{}
	fetchCalls := 0
	worker, err := media.NewFetchWorker(media.FetchWorkerConfig{
		TenantID: "tenant-a", OwnerID: "worker-a", Store: store, Importer: importer,
		Fetcher: actorFetcher{calls: &fetchCalls}, NewID: func() string { return "event-ready" }, RenewInterval: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if didWork, runErr := worker.RunOne(context.Background()); runErr != nil || !didWork {
		t.Fatalf("RunOne() = (%v, %v)", didWork, runErr)
	}
	if fetchCalls != 0 || importer.verifyCalls != 1 || store.ready.ObjectKey != ready.ObjectKey {
		t.Fatalf("fetch=%d verify=%d completed=%+v", fetchCalls, importer.verifyCalls, store.ready)
	}
}

func TestFetchWorkerRenewsClaimAndCancelsSlowProviderOnFenceLoss(t *testing.T) {
	job := media.FetchJob{
		TenantID: "tenant-a", JobID: "job-a", MediaID: "media-a", ConnectionID: "connection-a",
		ProviderMessageID: "provider-a", AttemptCount: 1, OwnerID: "worker-a", ClaimToken: 4,
	}
	store := &fetchJobStore{job: job, claimed: true, renewOK: false}
	worker, err := media.NewFetchWorker(media.FetchWorkerConfig{
		TenantID: "tenant-a", OwnerID: "worker-a", Store: store, Importer: &assignedImporter{},
		Fetcher: actorFetcher{block: true}, NewID: func() string { return "unused" }, RenewInterval: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	didWork, runErr := worker.RunOne(context.Background())
	if !didWork || !errors.Is(runErr, media.ErrFetchFenceLost) || store.renewed == 0 || store.failed || store.ready.ID != "" {
		t.Fatalf("RunOne() = (%v, %v), renewed=%d failed=%v ready=%+v", didWork, runErr, store.renewed, store.failed, store.ready)
	}
}
