// Package s3store provides the production S3-compatible media object backend.
package s3store

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"go.mau.fi/mautrix-gmessages/internal/gateway/media"
)

var ErrInvalidConfig = errors.New("invalid S3 object store configuration")

type Client interface {
	PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	DeleteObject(context.Context, *s3.DeleteObjectInput, ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
	HeadBucket(context.Context, *s3.HeadBucketInput, ...func(*s3.Options)) (*s3.HeadBucketOutput, error)
}

// Check verifies that the configured media bucket is reachable with the
// expected owner boundary. The operations endpoint redacts any provider error.
func (store *Store) Check(ctx context.Context) error {
	if store == nil || store.client == nil || ctx == nil {
		return ErrInvalidConfig
	}
	checkCtx, cancel := context.WithTimeout(ctx, store.operationTimeout)
	defer cancel()
	input := &s3.HeadBucketInput{Bucket: aws.String(store.bucket)}
	if store.expectedBucketOwner != "" {
		input.ExpectedBucketOwner = aws.String(store.expectedBucketOwner)
	}
	_, err := store.client.HeadBucket(checkCtx, input)
	return err
}

type Config struct {
	Bucket                    string
	Prefix                    string
	Region                    string
	Endpoint                  string
	UsePathStyle              bool
	ExpectedBucketOwner       string
	AllowMissingExpectedOwner bool
	OperationTimeout          time.Duration
	KMSKeyID                  string
	Client                    Client
}

type Store struct {
	client              Client
	bucket              string
	prefix              string
	expectedBucketOwner string
	kmsKeyID            string
	operationTimeout    time.Duration
}

func New(ctx context.Context, config Config) (*Store, error) {
	config.Bucket = strings.TrimSpace(config.Bucket)
	config.Prefix = strings.Trim(strings.TrimSpace(config.Prefix), "/")
	config.ExpectedBucketOwner = strings.TrimSpace(config.ExpectedBucketOwner)
	if config.OperationTimeout == 0 {
		config.OperationTimeout = 30 * time.Second
	}
	if config.Bucket == "" || !validPrefix(config.Prefix) || !validEndpoint(config.Endpoint) ||
		config.OperationTimeout < time.Second || config.OperationTimeout > 2*time.Minute ||
		(config.Endpoint == "" && config.ExpectedBucketOwner == "") ||
		(config.Endpoint != "" && config.ExpectedBucketOwner == "" && !config.AllowMissingExpectedOwner) {
		return nil, ErrInvalidConfig
	}
	client := config.Client
	if client == nil {
		loadOptions := []func(*awsconfig.LoadOptions) error{}
		if config.Region != "" {
			loadOptions = append(loadOptions, awsconfig.WithRegion(config.Region))
		}
		awsConfig, err := awsconfig.LoadDefaultConfig(ctx, loadOptions...)
		if err != nil {
			return nil, fmt.Errorf("load AWS configuration: %w", err)
		}
		client = s3.NewFromConfig(awsConfig, func(options *s3.Options) {
			options.UsePathStyle = config.UsePathStyle
			if config.Endpoint != "" {
				options.BaseEndpoint = aws.String(config.Endpoint)
			}
		})
	}
	return &Store{
		client: client, bucket: config.Bucket, prefix: config.Prefix,
		expectedBucketOwner: strings.TrimSpace(config.ExpectedBucketOwner), kmsKeyID: strings.TrimSpace(config.KMSKeyID),
		operationTimeout: config.OperationTimeout,
	}, nil
}

func (store *Store) Put(ctx context.Context, key string, source io.Reader, size int64, contentType string) (media.ObjectInfo, error) {
	if !validObjectKey(key) || source == nil || size < 0 || size > media.HardMaxBytes || !allowedContentType(contentType) {
		return media.ObjectInfo{}, ErrInvalidConfig
	}
	ctx, cancel := context.WithTimeout(ctx, store.operationTimeout)
	defer cancel()
	input := &s3.PutObjectInput{
		Bucket: aws.String(store.bucket), Key: aws.String(store.storageKey(key)), Body: source,
		ContentLength: aws.Int64(size), ContentType: aws.String(contentType), IfNoneMatch: aws.String("*"),
		ServerSideEncryption: types.ServerSideEncryptionAes256,
	}
	if store.expectedBucketOwner != "" {
		input.ExpectedBucketOwner = aws.String(store.expectedBucketOwner)
	}
	if store.kmsKeyID != "" {
		input.ServerSideEncryption = types.ServerSideEncryptionAwsKms
		input.SSEKMSKeyId = aws.String(store.kmsKeyID)
	}
	output, err := store.client.PutObject(ctx, input)
	if err != nil {
		return media.ObjectInfo{}, err
	}
	return media.ObjectInfo{Key: key, Size: size, ETag: strings.Trim(aws.ToString(output.ETag), `"`)}, nil
}

func (store *Store) Open(ctx context.Context, key string) (io.ReadCloser, media.ObjectInfo, error) {
	if !validObjectKey(key) {
		return nil, media.ObjectInfo{}, ErrInvalidConfig
	}
	ctx, cancel := context.WithTimeout(ctx, store.operationTimeout)
	input := &s3.GetObjectInput{Bucket: aws.String(store.bucket), Key: aws.String(store.storageKey(key))}
	if store.expectedBucketOwner != "" {
		input.ExpectedBucketOwner = aws.String(store.expectedBucketOwner)
	}
	output, err := store.client.GetObject(ctx, input)
	if err != nil {
		cancel()
		return nil, media.ObjectInfo{}, err
	}
	size := aws.ToInt64(output.ContentLength)
	if output.Body == nil || size < 0 || size > media.HardMaxBytes {
		if output.Body != nil {
			_ = output.Body.Close()
		}
		cancel()
		return nil, media.ObjectInfo{}, media.ErrTooLarge
	}
	info := media.ObjectInfo{Key: key, Size: size, ETag: strings.Trim(aws.ToString(output.ETag), `"`)}
	if output.LastModified != nil {
		info.LastModified = output.LastModified.UTC()
	}
	return &boundedReadCloser{Reader: &io.LimitedReader{R: output.Body, N: size + 1}, closer: output.Body, cancel: cancel}, info, nil
}

func (store *Store) Delete(ctx context.Context, key string) error {
	if !validObjectKey(key) {
		return ErrInvalidConfig
	}
	ctx, cancel := context.WithTimeout(ctx, store.operationTimeout)
	defer cancel()
	input := &s3.DeleteObjectInput{Bucket: aws.String(store.bucket), Key: aws.String(store.storageKey(key))}
	if store.expectedBucketOwner != "" {
		input.ExpectedBucketOwner = aws.String(store.expectedBucketOwner)
	}
	_, err := store.client.DeleteObject(ctx, input)
	return err
}

type boundedReadCloser struct {
	io.Reader
	closer io.Closer
	cancel context.CancelFunc
}

func (reader *boundedReadCloser) Close() error {
	reader.cancel()
	return reader.closer.Close()
}

func (store *Store) storageKey(key string) string {
	if store.prefix == "" {
		return key
	}
	return store.prefix + "/" + key
}

func validObjectKey(key string) bool {
	return strings.HasPrefix(key, "objects/") && validPrefix(key) && !strings.HasSuffix(key, "/")
}

func validPrefix(value string) bool {
	if value == "" {
		return true
	}
	if strings.HasPrefix(value, "/") || strings.HasSuffix(value, "/") || strings.Contains(value, "//") || strings.Contains(value, "..") || strings.ContainsAny(value, `\:`) {
		return false
	}
	for _, character := range value {
		if !(character == '/' || character == '-' || character == '_' || character >= '0' && character <= '9' || character >= 'a' && character <= 'z') {
			return false
		}
	}
	return true
}

func validEndpoint(raw string) bool {
	if raw == "" {
		return true
	}
	parsed, err := url.Parse(raw)
	return err == nil && parsed.Scheme == "https" && parsed.Hostname() != "" && parsed.User == nil && parsed.RawQuery == "" && parsed.Fragment == "" && parsed.Path == ""
}

func allowedContentType(value string) bool {
	switch value {
	case "image/jpeg", "image/png", "image/gif", "image/webp":
		return true
	default:
		return false
	}
}

var _ media.ObjectStore = (*Store)(nil)
