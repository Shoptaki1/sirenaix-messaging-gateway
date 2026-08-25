package awskms_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/kms"
	"go.mau.fi/mautrix-gmessages/internal/gateway/session"
	"go.mau.fi/mautrix-gmessages/internal/gateway/session/awskms"
)

type kmsClient struct {
	encryptInput     *kms.EncryptInput
	encryptPlaintext []byte
	decryptInput     *kms.DecryptInput
	hadDeadline      bool
}

func (client *kmsClient) Encrypt(ctx context.Context, input *kms.EncryptInput, _ ...func(*kms.Options)) (*kms.EncryptOutput, error) {
	_, client.hadDeadline = ctx.Deadline()
	client.encryptInput = input
	client.encryptPlaintext = append([]byte(nil), input.Plaintext...)
	return &kms.EncryptOutput{CiphertextBlob: []byte("wrapped-data-key")}, nil
}

func (client *kmsClient) Decrypt(ctx context.Context, input *kms.DecryptInput, _ ...func(*kms.Options)) (*kms.DecryptOutput, error) {
	_, client.hadDeadline = ctx.Deadline()
	client.decryptInput = input
	return &kms.DecryptOutput{Plaintext: bytes.Repeat([]byte{7}, 32)}, nil
}

func TestWrapperUsesBoundedAWSKMSOperationsAndStableEncryptionContext(t *testing.T) {
	client := &kmsClient{}
	wrapper, err := awskms.New(context.Background(), awskms.Config{
		KeyID:      "arn:aws:kms:us-east-1:123456789012:key/00000000-0000-0000-0000-000000000000",
		KeyVersion: 4, OperationTimeout: time.Second, Client: client,
	})
	if err != nil {
		t.Fatal(err)
	}
	dek := bytes.Repeat([]byte{1}, 32)
	wrapped, err := wrapper.WrapKey(context.Background(), dek)
	if err != nil || wrapped.KeyID == "" || wrapped.KeyVersion != 4 || !bytes.Equal(wrapped.Ciphertext, []byte("wrapped-data-key")) {
		t.Fatalf("WrapKey() = (%+v, %v)", wrapped, err)
	}
	if !client.hadDeadline || client.encryptInput == nil || !bytes.Equal(client.encryptPlaintext, dek) ||
		client.encryptInput.EncryptionContext["application"] != "sirenaix-messaging-gateway" ||
		client.encryptInput.EncryptionContext["envelope_version"] != "1" {
		t.Fatalf("Encrypt input = %+v, deadline=%v", client.encryptInput, client.hadDeadline)
	}
	if !bytes.Equal(client.encryptInput.Plaintext, make([]byte, 32)) {
		t.Fatalf("AWS Encrypt request retained plaintext DEK: %x", client.encryptInput.Plaintext)
	}
	plaintext, err := wrapper.UnwrapKey(context.Background(), wrapped)
	if err != nil || len(plaintext) != 32 || client.decryptInput == nil || !client.hadDeadline ||
		client.decryptInput.EncryptionContext["application"] != "sirenaix-messaging-gateway" {
		t.Fatalf("UnwrapKey() = (%d bytes, %v), input=%+v deadline=%v", len(plaintext), err, client.decryptInput, client.hadDeadline)
	}
}

func TestWrapperRejectsWrongKeyMetadataAndSizesBeforeAWS(t *testing.T) {
	client := &kmsClient{}
	wrapper, err := awskms.New(context.Background(), awskms.Config{KeyID: "alias/sirenaix", KeyVersion: 1, Client: client})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = wrapper.WrapKey(context.Background(), make([]byte, 31)); err == nil || client.encryptInput != nil {
		t.Fatal("short DEK reached AWS")
	}
	for _, wrapped := range []session.WrappedKey{
		{KeyID: "another-key", KeyVersion: 1, Ciphertext: []byte{1}},
		{KeyID: "alias/sirenaix", KeyVersion: 2, Ciphertext: []byte{1}},
		{KeyID: "alias/sirenaix", KeyVersion: 1},
		{KeyID: "alias/sirenaix", KeyVersion: 1, Ciphertext: make([]byte, 16*1024+1)},
	} {
		if _, err = wrapper.UnwrapKey(context.Background(), wrapped); err == nil {
			t.Fatalf("UnwrapKey(%+v) unexpectedly succeeded", wrapped)
		}
	}
}
