package s3store_test

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"go.mau.fi/mautrix-gmessages/internal/gateway/media/s3store"
)

type fakeS3 struct {
	put      *s3.PutObjectInput
	get      *s3.GetObjectInput
	delete   *s3.DeleteObjectInput
	head     *s3.HeadBucketInput
	deadline time.Time
}

func (client *fakeS3) HeadBucket(_ context.Context, input *s3.HeadBucketInput, _ ...func(*s3.Options)) (*s3.HeadBucketOutput, error) {
	client.head = input
	return &s3.HeadBucketOutput{}, nil
}

func (client *fakeS3) PutObject(ctx context.Context, input *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	client.put = input
	client.deadline, _ = ctx.Deadline()
	return &s3.PutObjectOutput{ETag: aws.String("etag-a")}, nil
}

func (client *fakeS3) GetObject(_ context.Context, input *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	client.get = input
	now := time.Unix(1700000000, 0).UTC()
	return &s3.GetObjectOutput{
		Body: io.NopCloser(bytes.NewReader([]byte("abc"))), ContentLength: aws.Int64(3),
		ETag: aws.String("etag-a"), LastModified: &now,
	}, nil
}

func (client *fakeS3) DeleteObject(_ context.Context, input *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	client.delete = input
	return &s3.DeleteObjectOutput{}, nil
}

func TestStoreUsesOpaquePrefixExactLengthConditionalCreateAndEncryption(t *testing.T) {
	client := &fakeS3{}
	store, err := s3store.New(context.Background(), s3store.Config{
		Bucket: "sirenaix-media", Prefix: "tenant-objects", ExpectedBucketOwner: "123456789012", Client: client,
	})
	if err != nil {
		t.Fatal(err)
	}
	info, err := store.Put(context.Background(), "objects/0123/abcdef", bytes.NewReader([]byte("abc")), 3, "image/png")
	if err != nil {
		t.Fatal(err)
	}
	if aws.ToString(client.put.Bucket) != "sirenaix-media" || aws.ToString(client.put.Key) != "tenant-objects/objects/0123/abcdef" ||
		aws.ToInt64(client.put.ContentLength) != 3 || aws.ToString(client.put.IfNoneMatch) != "*" ||
		client.put.ServerSideEncryption != types.ServerSideEncryptionAes256 || info.Size != 3 || info.Key != "objects/0123/abcdef" {
		t.Fatalf("put=%+v info=%+v", client.put, info)
	}
	body, err := io.ReadAll(client.put.Body)
	if err != nil || string(body) != "abc" {
		t.Fatalf("put body = %q, %v", body, err)
	}
}

func TestStoreReadinessChecksExactBucketAndExpectedOwner(t *testing.T) {
	client := &fakeS3{}
	store, err := s3store.New(context.Background(), s3store.Config{
		Bucket: "sirenaix-media", ExpectedBucketOwner: "123456789012", Client: client,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = store.Check(context.Background()); err != nil {
		t.Fatalf("Check() error = %v", err)
	}
	if aws.ToString(client.head.Bucket) != "sirenaix-media" || aws.ToString(client.head.ExpectedBucketOwner) != "123456789012" {
		t.Fatalf("head bucket input = %+v", client.head)
	}
}

func TestStoreReadsAndDeletesOnlyValidatedServerGeneratedKeys(t *testing.T) {
	client := &fakeS3{}
	store, _ := s3store.New(context.Background(), s3store.Config{Bucket: "bucket-a", ExpectedBucketOwner: "123456789012", Client: client})
	reader, info, err := store.Open(context.Background(), "objects/0123/abcdef")
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if info.Key != "objects/0123/abcdef" || info.Size != 3 || aws.ToString(client.get.Key) != "objects/0123/abcdef" {
		t.Fatalf("get=%+v info=%+v", client.get, info)
	}
	if err = store.Delete(context.Background(), "objects/0123/abcdef"); err != nil || client.delete == nil {
		t.Fatalf("delete=%+v err=%v", client.delete, err)
	}
	if _, _, err = store.Open(context.Background(), "../tenant-b/secret"); err == nil {
		t.Fatal("Open accepted caller-controlled traversal key")
	}
}

func TestStoreRejectsInsecureCustomEndpoint(t *testing.T) {
	if _, err := s3store.New(context.Background(), s3store.Config{
		Bucket: "bucket-a", Endpoint: "http://s3.example", Client: &fakeS3{},
	}); err == nil {
		t.Fatal("New accepted insecure S3 endpoint")
	}
}

func TestStoreRequiresExpectedOwnerForAWSAndBoundsEveryOperation(t *testing.T) {
	if _, err := s3store.New(context.Background(), s3store.Config{Bucket: "bucket-a", Client: &fakeS3{}}); err == nil {
		t.Fatal("AWS endpoint accepted without ExpectedBucketOwner")
	}
	if _, err := s3store.New(context.Background(), s3store.Config{
		Bucket: "bucket-a", Endpoint: "https://s3.compat.example", Client: &fakeS3{},
	}); err == nil {
		t.Fatal("custom endpoint accepted undocumented owner exception")
	}
	client := &fakeS3{}
	store, err := s3store.New(context.Background(), s3store.Config{
		Bucket: "bucket-a", Endpoint: "https://s3.compat.example", AllowMissingExpectedOwner: true,
		OperationTimeout: 2 * time.Second, Client: client,
	})
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	if _, err = store.Put(context.Background(), "objects/aa/bb", bytes.NewReader([]byte("x")), 1, "image/png"); err != nil {
		t.Fatal(err)
	}
	if client.deadline.Before(started.Add(time.Second)) || client.deadline.After(started.Add(3*time.Second)) {
		t.Fatalf("S3 operation deadline = %v", client.deadline)
	}
}
