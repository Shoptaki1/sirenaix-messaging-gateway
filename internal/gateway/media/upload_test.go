package media_test

import (
	"bytes"
	"context"
	"errors"
	"image"
	"image/color"
	"image/gif"
	"image/png"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"go.mau.fi/mautrix-gmessages/internal/gateway/domain"
	"go.mau.fi/mautrix-gmessages/internal/gateway/media"
)

type memoryObjects struct {
	key     string
	data    []byte
	deleted bool
}

func (store *memoryObjects) Put(_ context.Context, key string, input io.Reader, size int64, _ string) (media.ObjectInfo, error) {
	data, err := io.ReadAll(input)
	if err != nil {
		return media.ObjectInfo{}, err
	}
	if int64(len(data)) != size {
		return media.ObjectInfo{}, errors.New("size mismatch")
	}
	store.key, store.data = key, data
	return media.ObjectInfo{Key: key, Size: size}, nil
}

func (store *memoryObjects) Open(context.Context, string) (io.ReadCloser, media.ObjectInfo, error) {
	return io.NopCloser(bytes.NewReader(store.data)), media.ObjectInfo{Key: store.key, Size: int64(len(store.data))}, nil
}

func (store *memoryObjects) Delete(context.Context, string) error { store.deleted = true; return nil }

type memoryMetadata struct{ record media.Record }

func (metadata *memoryMetadata) Create(_ context.Context, record media.Record) error {
	metadata.record = record
	return nil
}

type verifyObjects struct {
	reader io.ReadCloser
	info   media.ObjectInfo
}

func (*verifyObjects) Put(context.Context, string, io.Reader, int64, string) (media.ObjectInfo, error) {
	return media.ObjectInfo{}, errors.New("unused")
}
func (store *verifyObjects) Open(context.Context, string) (io.ReadCloser, media.ObjectInfo, error) {
	return store.reader, store.info, nil
}
func (*verifyObjects) Delete(context.Context, string) error { return nil }

type readErrorAfter struct {
	data []byte
	err  error
}

func (reader *readErrorAfter) Read(destination []byte) (int, error) {
	if len(reader.data) > 0 {
		count := copy(destination, reader.data)
		reader.data = reader.data[count:]
		return count, nil
	}
	return 0, reader.err
}
func (*readErrorAfter) Close() error { return nil }

func TestVerifyPreservesTransientObjectReadErrorForRetry(t *testing.T) {
	sentinel := errors.New("transient object body failure")
	payload := []byte("abcd")
	uploader, err := media.NewUploader(media.UploadConfig{
		Objects: &verifyObjects{
			reader: &readErrorAfter{data: payload[:2], err: sentinel},
			info:   media.ObjectInfo{Key: "objects/a/b", Size: int64(len(payload))},
		},
		Metadata: &memoryMetadata{}, NewID: func() string { return "unused" }, MaxBytes: 1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	expected := media.Record{ID: "media-a", TenantID: "tenant-a", ObjectKey: "objects/a/b", Size: int64(len(payload)), SHA256: make([]byte, 32)}
	_, err = uploader.Verify(context.Background(), "tenant-a", "media-a", expected)
	if !errors.Is(err, sentinel) || errors.Is(err, media.ErrStoredMediaCorrupt) {
		t.Fatalf("Verify error = %v, want transient sentinel", err)
	}
}
func (metadata *memoryMetadata) Get(_ context.Context, tenant domain.TenantID, id domain.MediaID) (media.Record, error) {
	if metadata.record.TenantID != tenant || metadata.record.ID != id {
		return media.Record{}, media.ErrNotFound
	}
	return metadata.record, nil
}

func TestUploadStreamsBoundedImageAndUsesOpaqueGeneratedKey(t *testing.T) {
	payload := pngImage(t, 8, 6)
	objects := &memoryObjects{}
	metadata := &memoryMetadata{}
	uploader, err := media.NewUploader(media.UploadConfig{
		Objects: objects, Metadata: metadata, NewID: func() string { return "018f4ca7-52c4-7c5d-ae9b-5b7a358f9741" },
		MaxBytes: 1024, MaxPixels: 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	record, err := uploader.Upload(context.Background(), "tenant/../../a", media.Upload{
		Body: bytes.NewReader(payload), ContentLength: int64(len(payload)), DeclaredMIME: "image/png", Filename: "../unsafe\x00name.png",
	})
	if err != nil {
		t.Fatalf("Upload() error = %v", err)
	}
	if record.MIMEType != "image/png" || record.Width != 8 || record.Height != 6 || record.Size != int64(len(payload)) || len(record.SHA256) != 32 {
		t.Fatalf("record = %+v", record)
	}
	if strings.Contains(objects.key, "tenant") || strings.Contains(objects.key, "..") || strings.ContainsAny(objects.key, "\\:") {
		t.Fatalf("unsafe object key = %q", objects.key)
	}
	if strings.Contains(record.DisplayFilename, "/") || strings.Contains(record.DisplayFilename, "\\") || strings.ContainsRune(record.DisplayFilename, 0) {
		t.Fatalf("unsafe display filename = %q", record.DisplayFilename)
	}
}

func TestUploadRejectsChunkedAndLyingLengthOversizeBeforeObjectWrite(t *testing.T) {
	for _, declared := range []int64{-1, 4} {
		t.Run(string(rune(declared+2)), func(t *testing.T) {
			objects := &memoryObjects{}
			uploader, _ := media.NewUploader(media.UploadConfig{
				Objects: objects, Metadata: &memoryMetadata{}, NewID: func() string { return "018f4ca7-52c4-7c5d-ae9b-5b7a358f9741" }, MaxBytes: 16,
			})
			_, err := uploader.Upload(context.Background(), "tenant-a", media.Upload{
				Body: bytes.NewReader(bytes.Repeat([]byte{1}, 17)), ContentLength: declared, DeclaredMIME: "image/png",
			})
			if !errors.Is(err, media.ErrTooLarge) || len(objects.data) != 0 {
				t.Fatalf("Upload() error=%v object bytes=%d", err, len(objects.data))
			}
		})
	}
}

func TestUploadRejectsMIMEMismatchAndActiveTypes(t *testing.T) {
	pngPayload := pngImage(t, 1, 1)
	for name, upload := range map[string]media.Upload{
		"mismatch": {Body: bytes.NewReader(pngPayload), ContentLength: int64(len(pngPayload)), DeclaredMIME: "image/jpeg"},
		"svg":      {Body: strings.NewReader(`<svg xmlns="http://www.w3.org/2000/svg"/>`), ContentLength: -1, DeclaredMIME: "image/svg+xml"},
		"html":     {Body: strings.NewReader("<html><body>x</body></html>"), ContentLength: -1, DeclaredMIME: "text/html"},
	} {
		t.Run(name, func(t *testing.T) {
			uploader, _ := media.NewUploader(media.UploadConfig{Objects: &memoryObjects{}, Metadata: &memoryMetadata{}, NewID: func() string { return "id-a" }, MaxBytes: 1024})
			if _, err := uploader.Upload(context.Background(), "tenant-a", upload); !errors.Is(err, media.ErrUnsupportedImage) {
				t.Fatalf("Upload() error = %v", err)
			}
		})
	}
}

func TestUploadRejectsPixelBombFromHeaderWithoutDecodingPixels(t *testing.T) {
	payload := pngImage(t, 11, 10)
	uploader, _ := media.NewUploader(media.UploadConfig{
		Objects: &memoryObjects{}, Metadata: &memoryMetadata{}, NewID: func() string { return "id-a" }, MaxBytes: 4096, MaxPixels: 100,
	})
	if _, err := uploader.Upload(context.Background(), "tenant-a", media.Upload{Body: bytes.NewReader(payload), ContentLength: int64(len(payload)), DeclaredMIME: "image/png"}); !errors.Is(err, media.ErrPixelLimit) {
		t.Fatalf("Upload() error = %v, want ErrPixelLimit", err)
	}
}

func TestUploadRejectsAnimatedAndMalformedGIFWebPStructures(t *testing.T) {
	animatedGIF := gifImage(t, 128)
	staticGIF := gifImage(t, 1)
	animatedWebP := []byte{
		'R', 'I', 'F', 'F', 22, 0, 0, 0, 'W', 'E', 'B', 'P',
		'V', 'P', '8', 'X', 10, 0, 0, 0, 0x02, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	}
	for name, fixture := range map[string]struct {
		body []byte
		mime string
	}{
		"many-frame GIF": {animatedGIF, "image/gif"},
		"truncated GIF":  {staticGIF[:len(staticGIF)-1], "image/gif"},
		"trailing GIF":   {append(append([]byte(nil), staticGIF...), '<', 'x', '>'), "image/gif"},
		"animated WebP":  {animatedWebP, "image/webp"},
		"truncated WebP": {animatedWebP[:len(animatedWebP)-1], "image/webp"},
	} {
		t.Run(name, func(t *testing.T) {
			objects := &memoryObjects{}
			uploader, _ := media.NewUploader(media.UploadConfig{Objects: objects, Metadata: &memoryMetadata{}, NewID: func() string { return "id-a" }, MaxBytes: 1 << 20})
			_, err := uploader.Upload(context.Background(), "tenant-a", media.Upload{Body: bytes.NewReader(fixture.body), ContentLength: int64(len(fixture.body)), DeclaredMIME: fixture.mime})
			if !errors.Is(err, media.ErrUnsupportedImage) || len(objects.data) != 0 {
				t.Fatalf("Upload() error=%v object bytes=%d", err, len(objects.data))
			}
		})
	}
}

func TestUploadRejectsGIFDescriptorOutsideCanvasAndPixelBudget(t *testing.T) {
	outside := gifImage(t, 1)
	marker := bytes.IndexByte(outside, 0x2c)
	if marker < 0 || marker+10 > len(outside) {
		t.Fatal("GIF fixture lacks image descriptor")
	}
	// The logical canvas remains 1x1. Claim a 2x1 image descriptor.
	outside[marker+5], outside[marker+6] = 2, 0
	huge := gifImage(t, 1)
	marker = bytes.IndexByte(huge, 0x2c)
	huge[marker+5], huge[marker+6] = 0xff, 0x7f
	huge[marker+7], huge[marker+8] = 0xff, 0x7f
	for name, body := range map[string][]byte{"outside canvas": outside, "huge frame": huge} {
		t.Run(name, func(t *testing.T) {
			uploader, _ := media.NewUploader(media.UploadConfig{Objects: &memoryObjects{}, Metadata: &memoryMetadata{}, NewID: func() string { return "id-a" }, MaxBytes: 1 << 20, MaxPixels: 100})
			_, err := uploader.Upload(context.Background(), "tenant-a", media.Upload{Body: bytes.NewReader(body), ContentLength: int64(len(body)), DeclaredMIME: "image/gif"})
			if !errors.Is(err, media.ErrUnsupportedImage) && !errors.Is(err, media.ErrPixelLimit) {
				t.Fatalf("Upload() error = %v", err)
			}
		})
	}
}

func TestUploadRejectsWebPContainerAndBitstreamDimensionMismatch(t *testing.T) {
	body := staticWebPFixture(1, 1, 2, 1)
	uploader, _ := media.NewUploader(media.UploadConfig{Objects: &memoryObjects{}, Metadata: &memoryMetadata{}, NewID: func() string { return "id-a" }, MaxBytes: 1 << 20, MaxPixels: 100})
	_, err := uploader.Upload(context.Background(), "tenant-a", media.Upload{Body: bytes.NewReader(body), ContentLength: int64(len(body)), DeclaredMIME: "image/webp"})
	if !errors.Is(err, media.ErrUnsupportedImage) {
		t.Fatalf("Upload() error = %v, want dimension mismatch rejection", err)
	}
}

func staticWebPFixture(canvasWidth, canvasHeight, imageWidth, imageHeight int) []byte {
	canvas := []byte{0, 0, 0, 0,
		byte(canvasWidth - 1), byte((canvasWidth - 1) >> 8), byte((canvasWidth - 1) >> 16),
		byte(canvasHeight - 1), byte((canvasHeight - 1) >> 8), byte((canvasHeight - 1) >> 16)}
	bits := uint32(imageWidth-1) | uint32(imageHeight-1)<<14
	image := []byte{0x2f, byte(bits), byte(bits >> 8), byte(bits >> 16), byte(bits >> 24)}
	body := make([]byte, 0, 12+8+len(canvas)+8+len(image)+1)
	body = append(body, 'R', 'I', 'F', 'F', 0, 0, 0, 0, 'W', 'E', 'B', 'P')
	body = append(body, 'V', 'P', '8', 'X', 10, 0, 0, 0)
	body = append(body, canvas...)
	body = append(body, 'V', 'P', '8', 'L', 5, 0, 0, 0)
	body = append(body, image...)
	body = append(body, 0)
	size := uint32(len(body) - 8)
	body[4], body[5], body[6], body[7] = byte(size), byte(size>>8), byte(size>>16), byte(size>>24)
	return body
}

func TestUploadCancellationCleansUpBeforePersistence(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	objects := &memoryObjects{}
	uploader, _ := media.NewUploader(media.UploadConfig{Objects: objects, Metadata: &memoryMetadata{}, NewID: func() string { return "id-a" }})
	_, err := uploader.Upload(ctx, "tenant-a", media.Upload{Body: bytes.NewReader(pngImage(t, 1, 1)), ContentLength: -1, DeclaredMIME: "image/png"})
	if !errors.Is(err, context.Canceled) || len(objects.data) != 0 {
		t.Fatalf("Upload() error=%v object bytes=%d", err, len(objects.data))
	}
}

type cancelAfterRead struct {
	cancel context.CancelFunc
	sent   bool
}

func (reader *cancelAfterRead) Read(destination []byte) (int, error) {
	if reader.sent {
		return 0, io.EOF
	}
	reader.sent = true
	destination[0] = 1
	reader.cancel()
	return 1, nil
}

func TestUploadCancellationDuringChunkedBodyStopsBeforeObjectWrite(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	objects := &memoryObjects{}
	uploader, _ := media.NewUploader(media.UploadConfig{Objects: objects, Metadata: &memoryMetadata{}, NewID: func() string { return "id-a" }})
	_, err := uploader.Upload(ctx, "tenant-a", media.Upload{Body: &cancelAfterRead{cancel: cancel}, ContentLength: -1, DeclaredMIME: "image/png"})
	if !errors.Is(err, context.Canceled) || len(objects.data) != 0 {
		t.Fatalf("Upload() error=%v object bytes=%d", err, len(objects.data))
	}
}

type dedupeObjects struct {
	mu   sync.Mutex
	key  string
	data []byte
}

func (store *dedupeObjects) Put(_ context.Context, key string, input io.Reader, size int64, _ string) (media.ObjectInfo, error) {
	data, err := io.ReadAll(input)
	if err != nil {
		return media.ObjectInfo{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.data != nil {
		return media.ObjectInfo{}, errors.New("object already exists")
	}
	store.key, store.data = key, append([]byte(nil), data...)
	return media.ObjectInfo{Key: key, Size: size}, nil
}

func (store *dedupeObjects) Open(context.Context, string) (io.ReadCloser, media.ObjectInfo, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	return io.NopCloser(bytes.NewReader(append([]byte(nil), store.data...))), media.ObjectInfo{Key: store.key, Size: int64(len(store.data))}, nil
}

func (*dedupeObjects) Delete(context.Context, string) error { return nil }

type convergingMetadata struct {
	mu     sync.Mutex
	record media.Record
}

func (metadata *convergingMetadata) Create(_ context.Context, record media.Record) error {
	metadata.mu.Lock()
	defer metadata.mu.Unlock()
	if metadata.record.ID != "" && (metadata.record.ObjectKey != record.ObjectKey || !bytes.Equal(metadata.record.SHA256, record.SHA256)) {
		return errors.New("metadata identity conflict")
	}
	metadata.record = record
	return nil
}

func (metadata *convergingMetadata) Get(_ context.Context, tenant domain.TenantID, id domain.MediaID) (media.Record, error) {
	metadata.mu.Lock()
	defer metadata.mu.Unlock()
	if metadata.record.TenantID != tenant || metadata.record.ID != id {
		return media.Record{}, media.ErrNotFound
	}
	return metadata.record, nil
}

func TestConcurrentProviderMediaImportConvergesOnOpaqueObjectIdentity(t *testing.T) {
	payload := pngImage(t, 2, 2)
	objects := &dedupeObjects{}
	metadata := &convergingMetadata{}
	uploader, _ := media.NewUploader(media.UploadConfig{Objects: objects, Metadata: metadata, NewID: func() string { return "unused" }, MaxBytes: 4096})
	errorsFound := make(chan error, 2)
	var wait sync.WaitGroup
	for range 2 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, importErr := uploader.Import(context.Background(), "tenant-a", "media-a", media.Upload{
				Body: bytes.NewReader(payload), ContentLength: int64(len(payload)), DeclaredMIME: "image/png", Filename: "a.png",
			})
			errorsFound <- importErr
		}()
	}
	wait.Wait()
	close(errorsFound)
	for err := range errorsFound {
		if err != nil {
			t.Fatalf("concurrent Import error = %v", err)
		}
	}
	objects.mu.Lock()
	defer objects.mu.Unlock()
	if objects.key == "" || len(objects.data) != len(payload) {
		t.Fatalf("converged object = %q (%d bytes)", objects.key, len(objects.data))
	}
}

func TestLocalDevelopmentStoreRejectsSymlinkRootAndIntermediateEscape(t *testing.T) {
	base := t.TempDir()
	realRoot := filepath.Join(base, "real")
	if _, err := media.NewLocalStore(realRoot); err == nil {
		t.Fatal("missing object root was implicitly created without a trusted parent walk")
	}
	if _, err := os.Lstat(realRoot); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("rejected missing object root was still created: %v", err)
	}
	outside := filepath.Join(base, "outside")
	if err := os.MkdirAll(outside, 0o700); err != nil {
		t.Fatal(err)
	}
	rootLink := filepath.Join(base, "root-link")
	if err := os.Symlink(outside, rootLink); err != nil {
		t.Skipf("symlink creation unavailable on this development host: %v", err)
	}
	if _, err := media.NewLocalStore(rootLink); err == nil {
		t.Fatal("symlink object root was accepted")
	}
	if _, err := media.NewLocalStore(filepath.Join(rootLink, "nested-root")); err == nil {
		t.Fatal("object root beneath a symlink ancestor was accepted")
	}
	if err := os.Mkdir(realRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	store, err := media.NewLocalStore(realRoot)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Symlink(outside, filepath.Join(realRoot, "objects")); err != nil {
		t.Fatal(err)
	}
	_, err = store.Put(context.Background(), "objects/aa/bb", bytes.NewReader([]byte("x")), 1, "image/png")
	if err == nil {
		t.Fatal("symlink intermediate accepted")
	}
	if _, statErr := os.Stat(filepath.Join(outside, "aa", "bb")); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("local store escaped root: %v", statErr)
	}
}

func pngImage(t *testing.T, width, height int) []byte {
	t.Helper()
	img := image.NewNRGBA(image.Rect(0, 0, width, height))
	img.Set(0, 0, color.NRGBA{R: 1, G: 2, B: 3, A: 255})
	var output bytes.Buffer
	if err := png.Encode(&output, img); err != nil {
		t.Fatal(err)
	}
	return output.Bytes()
}

func gifImage(t *testing.T, frames int) []byte {
	t.Helper()
	palette := color.Palette{color.Black, color.White}
	images := make([]*image.Paletted, frames)
	delays := make([]int, frames)
	for index := range images {
		images[index] = image.NewPaletted(image.Rect(0, 0, 1, 1), palette)
		images[index].Pix[0] = uint8(index % 2)
	}
	var output bytes.Buffer
	if err := gif.EncodeAll(&output, &gif.GIF{Image: images, Delay: delays, Config: image.Config{ColorModel: palette, Width: 1, Height: 1}}); err != nil {
		t.Fatal(err)
	}
	return output.Bytes()
}
