// Package awskms implements production DEK wrapping with AWS KMS. The
// provider session/media/webhook scope remains authenticated by the outer
// AES-GCM envelope; KMS additionally binds wrapped keys to this application
// and envelope version through EncryptionContext.
package awskms

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/aws-sdk-go-v2/service/kms/types"
	"go.mau.fi/mautrix-gmessages/internal/gateway/session"
)

const (
	dekBytes           = 32
	maxCiphertextBytes = 16 * 1024
)

var ErrInvalidConfig = errors.New("invalid AWS KMS key wrapper configuration")

type Client interface {
	Encrypt(context.Context, *kms.EncryptInput, ...func(*kms.Options)) (*kms.EncryptOutput, error)
	Decrypt(context.Context, *kms.DecryptInput, ...func(*kms.Options)) (*kms.DecryptOutput, error)
}

type Config struct {
	// KeyID/KeyVersion configure a single key. KeyIDs plus CurrentVersion
	// retain historical key identifiers during explicit key migrations.
	KeyID            string
	KeyVersion       int
	KeyIDs           map[int]string
	CurrentVersion   int
	Region           string
	OperationTimeout time.Duration
	Client           Client
}

type Wrapper struct {
	client           Client
	keys             map[int]string
	currentVersion   int
	operationTimeout time.Duration
}

func New(ctx context.Context, config Config) (*Wrapper, error) {
	if config.OperationTimeout == 0 {
		config.OperationTimeout = 10 * time.Second
	}
	if config.OperationTimeout < time.Second || config.OperationTimeout > time.Minute {
		return nil, ErrInvalidConfig
	}
	keys := make(map[int]string, len(config.KeyIDs)+1)
	for version, keyID := range config.KeyIDs {
		if version < 1 || !validKeyID(keyID) {
			return nil, ErrInvalidConfig
		}
		keys[version] = keyID
	}
	if config.KeyID != "" || config.KeyVersion != 0 {
		if len(keys) != 0 || config.KeyVersion < 1 || !validKeyID(config.KeyID) {
			return nil, ErrInvalidConfig
		}
		keys[config.KeyVersion] = config.KeyID
		config.CurrentVersion = config.KeyVersion
	}
	if len(keys) == 0 || config.CurrentVersion < 1 || keys[config.CurrentVersion] == "" {
		return nil, ErrInvalidConfig
	}
	client := config.Client
	if client == nil {
		loadCtx, cancel := context.WithTimeout(ctx, config.OperationTimeout)
		defer cancel()
		options := []func(*awsconfig.LoadOptions) error{}
		if strings.TrimSpace(config.Region) != "" {
			options = append(options, awsconfig.WithRegion(strings.TrimSpace(config.Region)))
		}
		awsConfig, err := awsconfig.LoadDefaultConfig(loadCtx, options...)
		if err != nil {
			return nil, ErrInvalidConfig
		}
		client = kms.NewFromConfig(awsConfig)
	}
	return &Wrapper{client: client, keys: keys, currentVersion: config.CurrentVersion, operationTimeout: config.OperationTimeout}, nil
}

func (wrapper *Wrapper) WrapKey(ctx context.Context, plaintextKey []byte) (session.WrappedKey, error) {
	if wrapper == nil || wrapper.client == nil || len(plaintextKey) != dekBytes {
		return session.WrappedKey{}, session.ErrKeyWrapper
	}
	keyID := wrapper.keys[wrapper.currentVersion]
	operationCtx, cancel := context.WithTimeout(ctx, wrapper.operationTimeout)
	defer cancel()
	requestPlaintext := append([]byte(nil), plaintextKey...)
	defer zero(requestPlaintext)
	output, err := wrapper.client.Encrypt(operationCtx, &kms.EncryptInput{
		KeyId: aws.String(keyID), Plaintext: requestPlaintext,
		EncryptionAlgorithm: types.EncryptionAlgorithmSpecSymmetricDefault,
		EncryptionContext:   encryptionContext(),
	})
	if err != nil || output == nil || len(output.CiphertextBlob) == 0 || len(output.CiphertextBlob) > maxCiphertextBytes {
		return session.WrappedKey{}, session.ErrKeyWrapper
	}
	return session.WrappedKey{
		KeyID: keyID, KeyVersion: wrapper.currentVersion,
		Ciphertext: append([]byte(nil), output.CiphertextBlob...),
	}, nil
}

func (wrapper *Wrapper) UnwrapKey(ctx context.Context, wrapped session.WrappedKey) ([]byte, error) {
	if wrapper == nil || wrapper.client == nil || wrapped.KeyVersion < 1 || wrapper.keys[wrapped.KeyVersion] != wrapped.KeyID ||
		len(wrapped.Ciphertext) == 0 || len(wrapped.Ciphertext) > maxCiphertextBytes {
		return nil, session.ErrKeyWrapper
	}
	operationCtx, cancel := context.WithTimeout(ctx, wrapper.operationTimeout)
	defer cancel()
	output, err := wrapper.client.Decrypt(operationCtx, &kms.DecryptInput{
		KeyId: aws.String(wrapped.KeyID), CiphertextBlob: append([]byte(nil), wrapped.Ciphertext...),
		EncryptionAlgorithm: types.EncryptionAlgorithmSpecSymmetricDefault,
		EncryptionContext:   encryptionContext(),
	})
	if err != nil || output == nil || len(output.Plaintext) != dekBytes {
		if output != nil {
			zero(output.Plaintext)
		}
		return nil, session.ErrKeyWrapper
	}
	plaintext := append([]byte(nil), output.Plaintext...)
	zero(output.Plaintext)
	return plaintext, nil
}

func encryptionContext() map[string]string {
	return map[string]string{"application": "sirenaix-messaging-gateway", "envelope_version": "1"}
}

func validKeyID(keyID string) bool {
	if strings.TrimSpace(keyID) != keyID || len(keyID) < 1 || len(keyID) > 2048 {
		return false
	}
	for _, char := range keyID {
		if !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || strings.ContainsRune("_:/.-", char)) {
			return false
		}
	}
	return true
}

func zero(value []byte) {
	for index := range value {
		value[index] = 0
	}
}

var _ session.KeyWrapper = (*Wrapper)(nil)
