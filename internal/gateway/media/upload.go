// Package media implements bounded binary ingestion and tenant-owned object
// metadata. It accepts only passive raster image formats.
package media

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"image"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"io"
	"mime"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"go.mau.fi/mautrix-gmessages/internal/gateway/domain"
)

const (
	DefaultMaxBytes  int64 = 10 << 20
	HardMaxBytes     int64 = 25 << 20
	DefaultMaxPixels int64 = 40_000_000
	defaultUploads         = 8
)

var (
	ErrTooLarge           = errors.New("image exceeds byte limit")
	ErrLengthMismatch     = errors.New("declared and actual image length differ")
	ErrUnsupportedImage   = errors.New("unsupported or mismatched image type")
	ErrPixelLimit         = errors.New("image exceeds decoded pixel limit")
	ErrNotFound           = errors.New("media not found")
	ErrStoredMediaCorrupt = errors.New("stored media failed integrity verification")
)

type ObjectInfo struct {
	Key          string
	Size         int64
	ETag         string
	LastModified time.Time
}

type ObjectStore interface {
	Put(context.Context, string, io.Reader, int64, string) (ObjectInfo, error)
	Open(context.Context, string) (io.ReadCloser, ObjectInfo, error)
	Delete(context.Context, string) error
}

type Record struct {
	ID              domain.MediaID
	TenantID        domain.TenantID
	ObjectKey       string
	MIMEType        string
	Size            int64
	SHA256          []byte
	Width           int
	Height          int
	DisplayFilename string
	State           string
	CreatedAt       time.Time
}

type Metadata interface {
	Create(context.Context, Record) error
	Get(context.Context, domain.TenantID, domain.MediaID) (Record, error)
}

type Upload struct {
	Body          io.Reader
	ContentLength int64
	DeclaredMIME  string
	Filename      string
}

type UploadConfig struct {
	Objects       ObjectStore
	Metadata      Metadata
	NewID         func() string
	MaxBytes      int64
	MaxPixels     int64
	MaxConcurrent int
	TempDirectory string
	Now           func() time.Time
}

type Uploader struct {
	objects       ObjectStore
	metadata      Metadata
	newID         func() string
	maxBytes      int64
	maxPixels     int64
	tempDirectory string
	now           func() time.Time
	quota         chan struct{}
}

func NewUploader(config UploadConfig) (*Uploader, error) {
	if config.Objects == nil || config.Metadata == nil || config.NewID == nil {
		return nil, errors.New("media object store, metadata store, and ID generator are required")
	}
	if config.MaxBytes == 0 {
		config.MaxBytes = DefaultMaxBytes
	}
	if config.MaxBytes < 1 || config.MaxBytes > HardMaxBytes {
		return nil, ErrTooLarge
	}
	if config.MaxPixels == 0 {
		config.MaxPixels = DefaultMaxPixels
	}
	if config.MaxPixels < 1 {
		return nil, ErrPixelLimit
	}
	if config.MaxConcurrent == 0 {
		config.MaxConcurrent = defaultUploads
	}
	if config.MaxConcurrent < 1 || config.MaxConcurrent > 1024 {
		return nil, errors.New("invalid media concurrency")
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	return &Uploader{
		objects: config.Objects, metadata: config.Metadata, newID: config.NewID,
		maxBytes: config.MaxBytes, maxPixels: config.MaxPixels, tempDirectory: config.TempDirectory,
		now: config.Now, quota: make(chan struct{}, config.MaxConcurrent),
	}, nil
}

func (uploader *Uploader) Upload(ctx context.Context, tenantID domain.TenantID, upload Upload) (Record, error) {
	return uploader.ingest(ctx, tenantID, "", upload, true)
}

// Import persists provider media under the already-durable opaque media ID.
// Replays converge on the same object key and metadata identity.
func (uploader *Uploader) Import(ctx context.Context, tenantID domain.TenantID, mediaID domain.MediaID, upload Upload) (Record, error) {
	if mediaID == "" {
		return Record{}, domain.ErrInvalidIdentifier
	}
	return uploader.ingest(ctx, tenantID, mediaID, upload, false)
}

// Verify reopens an already-imported object after a worker crash and proves
// its exact bounded size and SHA-256 before the durable fetch job is completed.
func (uploader *Uploader) Verify(ctx context.Context, tenantID domain.TenantID, mediaID domain.MediaID, expected Record) (Record, error) {
	if tenantID == "" || mediaID == "" || expected.TenantID != tenantID || expected.ID != mediaID || expected.Size < 1 || expected.Size > uploader.maxBytes || len(expected.SHA256) != sha256.Size || expected.ObjectKey == "" {
		return Record{}, domain.ErrInvalidIdentifier
	}
	reader, info, err := uploader.objects.Open(ctx, expected.ObjectKey)
	if err != nil {
		if errors.Is(err, ErrNotFound) || errors.Is(err, os.ErrNotExist) {
			return Record{}, ErrNotFound
		}
		return Record{}, err
	}
	defer reader.Close()
	if info.Size != expected.Size {
		return Record{}, ErrStoredMediaCorrupt
	}
	digest := sha256.New()
	written, err := copyBounded(ctx, digest, reader, expected.Size)
	if err != nil {
		if errors.Is(err, ErrTooLarge) {
			return Record{}, ErrStoredMediaCorrupt
		}
		return Record{}, err
	}
	if written != expected.Size || subtle.ConstantTimeCompare(digest.Sum(nil), expected.SHA256) != 1 {
		return Record{}, ErrStoredMediaCorrupt
	}
	return expected, nil
}

func (uploader *Uploader) ingest(ctx context.Context, tenantID domain.TenantID, assignedID domain.MediaID, upload Upload, deleteOnMetadataFailure bool) (Record, error) {
	if tenantID == "" || upload.Body == nil {
		return Record{}, domain.ErrInvalidIdentifier
	}
	if upload.ContentLength > uploader.maxBytes || upload.ContentLength > HardMaxBytes {
		return Record{}, ErrTooLarge
	}
	select {
	case uploader.quota <- struct{}{}:
		defer func() { <-uploader.quota }()
	case <-ctx.Done():
		return Record{}, ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return Record{}, err
	}

	temporary, err := os.CreateTemp(uploader.tempDirectory, "sirenaix-media-*")
	if err != nil {
		return Record{}, fmt.Errorf("create media spool: %w", err)
	}
	temporaryName := temporary.Name()
	defer func() {
		_ = temporary.Close()
		_ = os.Remove(temporaryName)
	}()
	if err = temporary.Chmod(0o600); err != nil {
		return Record{}, fmt.Errorf("secure media spool: %w", err)
	}

	digest := sha256.New()
	actual, err := copyBounded(ctx, io.MultiWriter(temporary, digest), upload.Body, uploader.maxBytes)
	if err != nil {
		return Record{}, err
	}
	if upload.ContentLength >= 0 && upload.ContentLength != actual {
		return Record{}, ErrLengthMismatch
	}
	if _, err = temporary.Seek(0, io.SeekStart); err != nil {
		return Record{}, fmt.Errorf("rewind media spool: %w", err)
	}
	actualMIME, width, height, err := inspectImage(temporary, uploader.maxPixels)
	if err != nil {
		return Record{}, err
	}
	if !matchingDeclaredMIME(upload.DeclaredMIME, actualMIME) {
		return Record{}, ErrUnsupportedImage
	}
	if _, err = temporary.Seek(0, io.SeekStart); err != nil {
		return Record{}, fmt.Errorf("rewind media spool: %w", err)
	}

	id := assignedID
	if id == "" {
		id = domain.MediaID(uploader.newID())
	}
	if id == "" {
		return Record{}, domain.ErrInvalidIdentifier
	}
	key := opaqueObjectKey(tenantID, id)
	if err = uploader.putOrVerify(ctx, key, temporary, actual, actualMIME, digestBytes(digest)); err != nil {
		return Record{}, err
	}
	record := Record{
		ID: id, TenantID: tenantID, ObjectKey: key, MIMEType: actualMIME, Size: actual,
		SHA256: digestBytes(digest), Width: width, Height: height,
		DisplayFilename: sanitizeFilename(upload.Filename, actualMIME), State: "ready", CreatedAt: uploader.now().UTC(),
	}
	if err = uploader.metadata.Create(ctx, record); err != nil {
		if deleteOnMetadataFailure {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = uploader.objects.Delete(cleanupCtx, key)
			cancel()
		}
		return Record{}, err
	}
	return record, nil
}

func (uploader *Uploader) putOrVerify(ctx context.Context, key string, source io.Reader, size int64, contentType string, expectedDigest []byte) error {
	if _, err := uploader.objects.Put(ctx, key, source, size, contentType); err == nil {
		return nil
	}
	existing, info, openErr := uploader.objects.Open(ctx, key)
	if openErr != nil {
		return openErr
	}
	defer existing.Close()
	if info.Size != size {
		return errors.New("media object identity conflict")
	}
	digest := sha256.New()
	written, copyErr := copyBounded(ctx, digest, existing, size)
	if copyErr != nil || written != size || subtle.ConstantTimeCompare(digest.Sum(nil), expectedDigest) != 1 {
		return errors.New("media object identity conflict")
	}
	return nil
}

func (uploader *Uploader) Open(ctx context.Context, tenantID domain.TenantID, mediaID domain.MediaID) (io.ReadCloser, Record, error) {
	record, err := uploader.GetMetadata(ctx, tenantID, mediaID)
	if err != nil || record.TenantID != tenantID || record.ID != mediaID || record.State != "ready" || record.ObjectKey == "" {
		if err != nil {
			return nil, Record{}, err
		}
		return nil, Record{}, ErrNotFound
	}
	reader, info, err := uploader.objects.Open(ctx, record.ObjectKey)
	if err != nil {
		return nil, Record{}, err
	}
	if (info.Key != "" && info.Key != record.ObjectKey) || info.Size != record.Size || info.Size < 0 || info.Size > HardMaxBytes {
		_ = reader.Close()
		return nil, Record{}, ErrNotFound
	}
	return reader, record, nil
}

// GetMetadata returns durable state even while provider bytes are pending or
// have failed. Content access remains limited to Open and ready objects.
func (uploader *Uploader) GetMetadata(ctx context.Context, tenantID domain.TenantID, mediaID domain.MediaID) (Record, error) {
	if tenantID == "" || mediaID == "" {
		return Record{}, ErrNotFound
	}
	record, err := uploader.metadata.Get(ctx, tenantID, mediaID)
	if err != nil || record.TenantID != tenantID || record.ID != mediaID {
		if err != nil {
			return Record{}, err
		}
		return Record{}, ErrNotFound
	}
	return record, nil
}

func copyBounded(ctx context.Context, destination io.Writer, source io.Reader, maximum int64) (int64, error) {
	limited := &io.LimitedReader{R: &contextReader{ctx: ctx, reader: source}, N: maximum + 1}
	buffer := make([]byte, 32*1024)
	written, err := io.CopyBuffer(destination, limited, buffer)
	if err != nil {
		return written, err
	}
	if written > maximum {
		return written, ErrTooLarge
	}
	return written, nil
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (reader *contextReader) Read(buffer []byte) (int, error) {
	if err := reader.ctx.Err(); err != nil {
		return 0, err
	}
	return reader.reader.Read(buffer)
}

func inspectImage(input io.ReadSeeker, maxPixels int64) (string, int, int, error) {
	header := make([]byte, 32)
	count, err := io.ReadFull(input, header)
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
		return "", 0, 0, ErrUnsupportedImage
	}
	header = header[:count]
	actualMIME := sniffRasterMIME(header)
	if actualMIME == "" {
		return "", 0, 0, ErrUnsupportedImage
	}
	if _, err = input.Seek(0, io.SeekStart); err != nil {
		return "", 0, 0, err
	}
	if err = validateStaticRasterStructure(input, actualMIME, maxPixels); err != nil {
		return "", 0, 0, ErrUnsupportedImage
	}
	if _, err = input.Seek(0, io.SeekStart); err != nil {
		return "", 0, 0, err
	}
	var width, height int
	if actualMIME == "image/webp" {
		width, height, err = webPDimensions(header)
	} else {
		config, _, configErr := image.DecodeConfig(bufio.NewReader(input))
		err = configErr
		width, height = config.Width, config.Height
	}
	if err != nil || width <= 0 || height <= 0 {
		return "", 0, 0, ErrUnsupportedImage
	}
	if int64(width) > maxPixels/int64(height) {
		return "", 0, 0, ErrPixelLimit
	}
	return actualMIME, width, height, nil
}

func validateStaticRasterStructure(input io.ReadSeeker, mimeType string, maxPixels int64) error {
	switch mimeType {
	case "image/gif":
		return validateStaticGIF(input, maxPixels)
	case "image/webp":
		return validateStaticWebP(input, maxPixels)
	default:
		return nil
	}
}

func validateStaticGIF(input io.Reader, maxPixels int64) error {
	header := make([]byte, 13)
	if _, err := io.ReadFull(input, header); err != nil ||
		(!bytes.Equal(header[:6], []byte("GIF87a")) && !bytes.Equal(header[:6], []byte("GIF89a"))) {
		return ErrUnsupportedImage
	}
	canvasWidth := int64(binary.LittleEndian.Uint16(header[6:8]))
	canvasHeight := int64(binary.LittleEndian.Uint16(header[8:10]))
	if canvasWidth < 1 || canvasHeight < 1 || canvasWidth > maxPixels/canvasHeight {
		return ErrUnsupportedImage
	}
	if header[10]&0x80 != 0 {
		if err := discardExact(input, int64(3*(2<<uint(header[10]&0x07)))); err != nil {
			return err
		}
	}
	images := 0
	for {
		var marker [1]byte
		if _, err := io.ReadFull(input, marker[:]); err != nil {
			return ErrUnsupportedImage
		}
		switch marker[0] {
		case 0x3b:
			var trailing [1]byte
			if count, err := input.Read(trailing[:]); count != 0 || !errors.Is(err, io.EOF) || images != 1 {
				return ErrUnsupportedImage
			}
			return nil
		case 0x21:
			var label [1]byte
			if _, err := io.ReadFull(input, label[:]); err != nil || discardGIFSubBlocks(input) != nil {
				return ErrUnsupportedImage
			}
		case 0x2c:
			images++
			if images > 1 {
				return ErrUnsupportedImage
			}
			descriptor := make([]byte, 9)
			if _, err := io.ReadFull(input, descriptor); err != nil {
				return ErrUnsupportedImage
			}
			left := int64(binary.LittleEndian.Uint16(descriptor[0:2]))
			top := int64(binary.LittleEndian.Uint16(descriptor[2:4]))
			width := int64(binary.LittleEndian.Uint16(descriptor[4:6]))
			height := int64(binary.LittleEndian.Uint16(descriptor[6:8]))
			if width < 1 || height < 1 || left > canvasWidth-width || top > canvasHeight-height || width > maxPixels/height {
				return ErrUnsupportedImage
			}
			if descriptor[8]&0x80 != 0 {
				if err := discardExact(input, int64(3*(2<<uint(descriptor[8]&0x07)))); err != nil {
					return ErrUnsupportedImage
				}
			}
			var codeSize [1]byte
			if _, err := io.ReadFull(input, codeSize[:]); err != nil || codeSize[0] < 2 || codeSize[0] > 8 || discardGIFSubBlocks(input) != nil {
				return ErrUnsupportedImage
			}
		default:
			return ErrUnsupportedImage
		}
	}
}

func discardGIFSubBlocks(input io.Reader) error {
	for {
		var size [1]byte
		if _, err := io.ReadFull(input, size[:]); err != nil {
			return err
		}
		if size[0] == 0 {
			return nil
		}
		if err := discardExact(input, int64(size[0])); err != nil {
			return err
		}
	}
}

func validateStaticWebP(input io.ReadSeeker, maxPixels int64) error {
	total, err := input.Seek(0, io.SeekEnd)
	if err != nil || total < 20 {
		return ErrUnsupportedImage
	}
	if _, err = input.Seek(0, io.SeekStart); err != nil {
		return err
	}
	header := make([]byte, 12)
	if _, err = io.ReadFull(input, header); err != nil || !bytes.Equal(header[:4], []byte("RIFF")) || !bytes.Equal(header[8:12], []byte("WEBP")) || int64(binary.LittleEndian.Uint32(header[4:8]))+8 != total {
		return ErrUnsupportedImage
	}
	remaining := total - 12
	imageChunks := 0
	canvasWidth, canvasHeight := 0, 0
	imageWidth, imageHeight := 0, 0
	for remaining > 0 {
		if remaining < 8 {
			return ErrUnsupportedImage
		}
		chunkHeader := make([]byte, 8)
		if _, err = io.ReadFull(input, chunkHeader); err != nil {
			return ErrUnsupportedImage
		}
		remaining -= 8
		chunkSize := int64(binary.LittleEndian.Uint32(chunkHeader[4:8]))
		padded := chunkSize + chunkSize%2
		if chunkSize < 0 || padded > remaining {
			return ErrUnsupportedImage
		}
		chunkType := string(chunkHeader[:4])
		if chunkType == "ANIM" || chunkType == "ANMF" {
			return ErrUnsupportedImage
		}
		if chunkType == "VP8 " || chunkType == "VP8L" {
			imageChunks++
			if imageChunks > 1 {
				return ErrUnsupportedImage
			}
		}
		if chunkType == "VP8X" {
			if chunkSize != 10 {
				return ErrUnsupportedImage
			}
			flags := make([]byte, 10)
			if _, err = io.ReadFull(input, flags); err != nil || flags[0]&0x02 != 0 {
				return ErrUnsupportedImage
			}
			canvasWidth = 1 + int(flags[4]) + int(flags[5])<<8 + int(flags[6])<<16
			canvasHeight = 1 + int(flags[7]) + int(flags[8])<<8 + int(flags[9])<<16
			if !validImageDimensions(canvasWidth, canvasHeight, maxPixels) {
				return ErrUnsupportedImage
			}
		} else if chunkType == "VP8L" {
			prefix := make([]byte, min(chunkSize, 5))
			if _, err = io.ReadFull(input, prefix); err != nil || len(prefix) < 5 || prefix[0] != 0x2f {
				return ErrUnsupportedImage
			}
			bits := uint32(prefix[1]) | uint32(prefix[2])<<8 | uint32(prefix[3])<<16 | uint32(prefix[4])<<24
			imageWidth, imageHeight = int(bits&0x3fff)+1, int((bits>>14)&0x3fff)+1
			if !validImageDimensions(imageWidth, imageHeight, maxPixels) || discardExact(input, chunkSize-int64(len(prefix))) != nil {
				return ErrUnsupportedImage
			}
		} else if chunkType == "VP8 " {
			prefix := make([]byte, min(chunkSize, 10))
			if _, err = io.ReadFull(input, prefix); err != nil || len(prefix) < 10 || !bytes.Equal(prefix[3:6], []byte{0x9d, 0x01, 0x2a}) {
				return ErrUnsupportedImage
			}
			imageWidth = int(prefix[6]) | int(prefix[7]&0x3f)<<8
			imageHeight = int(prefix[8]) | int(prefix[9]&0x3f)<<8
			if !validImageDimensions(imageWidth, imageHeight, maxPixels) || discardExact(input, chunkSize-int64(len(prefix))) != nil {
				return ErrUnsupportedImage
			}
		} else if err = discardExact(input, chunkSize); err != nil {
			return ErrUnsupportedImage
		}
		if chunkSize%2 != 0 {
			var padding [1]byte
			if _, err = io.ReadFull(input, padding[:]); err != nil || padding[0] != 0 {
				return ErrUnsupportedImage
			}
		}
		remaining -= padded
	}
	if imageChunks != 1 {
		return ErrUnsupportedImage
	}
	if canvasWidth != 0 && (canvasWidth != imageWidth || canvasHeight != imageHeight) {
		return ErrUnsupportedImage
	}
	return nil
}

func validImageDimensions(width, height int, maxPixels int64) bool {
	return width > 0 && height > 0 && int64(width) <= maxPixels/int64(height)
}

func discardExact(input io.Reader, count int64) error {
	written, err := io.CopyN(io.Discard, input, count)
	if err != nil || written != count {
		return ErrUnsupportedImage
	}
	return nil
}

func sniffRasterMIME(header []byte) string {
	switch {
	case len(header) >= 8 && bytes.Equal(header[:8], []byte("\x89PNG\r\n\x1a\n")):
		return "image/png"
	case len(header) >= 3 && bytes.Equal(header[:3], []byte{0xff, 0xd8, 0xff}):
		return "image/jpeg"
	case len(header) >= 6 && (bytes.Equal(header[:6], []byte("GIF87a")) || bytes.Equal(header[:6], []byte("GIF89a"))):
		return "image/gif"
	case len(header) >= 16 && bytes.Equal(header[:4], []byte("RIFF")) && bytes.Equal(header[8:12], []byte("WEBP")):
		return "image/webp"
	default:
		return ""
	}
}

func webPDimensions(header []byte) (int, int, error) {
	if len(header) < 30 {
		return 0, 0, ErrUnsupportedImage
	}
	switch string(header[12:16]) {
	case "VP8X":
		width := 1 + int(header[24]) + int(header[25])<<8 + int(header[26])<<16
		height := 1 + int(header[27]) + int(header[28])<<8 + int(header[29])<<16
		return width, height, nil
	case "VP8L":
		if header[20] != 0x2f {
			return 0, 0, ErrUnsupportedImage
		}
		bits := uint32(header[21]) | uint32(header[22])<<8 | uint32(header[23])<<16 | uint32(header[24])<<24
		return int(bits&0x3fff) + 1, int((bits>>14)&0x3fff) + 1, nil
	case "VP8 ":
		if len(header) < 30 || !bytes.Equal(header[23:26], []byte{0x9d, 0x01, 0x2a}) {
			return 0, 0, ErrUnsupportedImage
		}
		return int(header[26]) | int(header[27]&0x3f)<<8, int(header[28]) | int(header[29]&0x3f)<<8, nil
	default:
		return 0, 0, ErrUnsupportedImage
	}
}

func matchingDeclaredMIME(declared, actual string) bool {
	if strings.TrimSpace(declared) == "" {
		return true
	}
	parsed, _, err := mime.ParseMediaType(declared)
	return err == nil && strings.EqualFold(parsed, actual)
}

func opaqueObjectKey(tenantID domain.TenantID, mediaID domain.MediaID) string {
	tenantDigest := sha256.Sum256([]byte(tenantID))
	objectDigest := sha256.Sum256([]byte(string(tenantID) + "\x00" + string(mediaID)))
	return fmt.Sprintf("objects/%x/%x", tenantDigest[:16], objectDigest[:])
}

func digestBytes(digest hash.Hash) []byte { return append([]byte(nil), digest.Sum(nil)...) }

func sanitizeFilename(input, actualMIME string) string {
	input = strings.ReplaceAll(input, "\\", "/")
	input = filepath.Base(input)
	var output strings.Builder
	for _, char := range input {
		if unicode.IsControl(char) || char == '/' || char == '\\' || char == ':' {
			continue
		}
		output.WriteRune(char)
		if output.Len() >= 200 {
			break
		}
	}
	name := strings.TrimSpace(output.String())
	if name == "" || name == "." {
		extension := map[string]string{"image/jpeg": ".jpg", "image/png": ".png", "image/gif": ".gif", "image/webp": ".webp"}[actualMIME]
		name = "image" + extension
	}
	if !utf8.ValidString(name) {
		return "image"
	}
	return name
}
