package media

import (
	"context"
	"errors"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
)

// LocalStore is a development-only object backend. All object operations are
// descriptor-relative through os.Root; caller-controlled paths are never
// passed to process-relative filesystem functions.
type LocalStore struct {
	root *os.Root
}

func NewLocalStore(rootName string) (*LocalStore, error) {
	if strings.TrimSpace(rootName) == "" {
		return nil, errors.New("local object root is required")
	}
	absolute, err := filepath.Abs(rootName)
	if err != nil {
		return nil, err
	}
	root, rootInfo, err := openExistingRootWithoutLinks(absolute)
	if err != nil {
		return nil, err
	}
	if !rootInfo.IsDir() || linkLike(rootInfo) || rootInfo.Mode().Perm()&0o077 != 0 {
		_ = root.Close()
		return nil, errors.New("local object root must be a pre-existing real private directory")
	}
	return &LocalStore{root: root}, nil
}

// openExistingRootWithoutLinks walks from the filesystem root using retained
// os.Root handles. Requiring operators to create the development directory in
// advance avoids MkdirAll following a symlinked ancestor before validation.
func openExistingRootWithoutLinks(absolute string) (*os.Root, os.FileInfo, error) {
	volume := filepath.VolumeName(absolute)
	// UNC/network roots cannot provide the local no-follow boundary this
	// development backend promises.
	if strings.HasPrefix(volume, `\\`) {
		return nil, nil, errors.New("local object root must be on a local filesystem")
	}
	anchor := volume + string(filepath.Separator)
	relative, err := filepath.Rel(anchor, absolute)
	if err != nil || relative == "." || relative == "" || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return nil, nil, errors.New("local object root must be below a filesystem root")
	}
	components := strings.FieldsFunc(relative, func(char rune) bool { return char == '/' || char == '\\' })
	if len(components) == 0 {
		return nil, nil, errors.New("local object root is invalid")
	}
	current, err := os.OpenRoot(anchor)
	if err != nil {
		return nil, nil, errors.New("open local filesystem root")
	}
	for _, component := range components {
		before, statErr := current.Lstat(component)
		if statErr != nil || !before.IsDir() || linkLike(before) {
			_ = current.Close()
			return nil, nil, errors.New("local object root contains a missing or link/reparse component")
		}
		next, openErr := current.OpenRoot(component)
		if openErr != nil {
			_ = current.Close()
			return nil, nil, errors.New("open local object root component")
		}
		opened, openStatErr := next.Stat(".")
		after, afterErr := current.Lstat(component)
		if openStatErr != nil || afterErr != nil || linkLike(after) || !os.SameFile(before, opened) || !os.SameFile(opened, after) {
			_ = next.Close()
			_ = current.Close()
			return nil, nil, errors.New("local object root component changed or is a link/reparse path")
		}
		_ = current.Close()
		current = next
	}
	info, err := current.Stat(".")
	if err != nil {
		_ = current.Close()
		return nil, nil, errors.New("stat local object root")
	}
	return current, info, nil
}

// Close releases the directory handle retained by the development backend.
func (store *LocalStore) Close() error {
	if store == nil || store.root == nil {
		return nil
	}
	return store.root.Close()
}

func (store *LocalStore) Put(ctx context.Context, key string, source io.Reader, size int64, _ string) (ObjectInfo, error) {
	parent, name, err := store.openParent(key, true)
	if err != nil {
		return ObjectInfo{}, err
	}
	defer parent.Close()
	file, err := parent.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return ObjectInfo{}, err
	}
	written, copyErr := copyBounded(ctx, file, source, size)
	closeErr := file.Close()
	if copyErr != nil || written != size || closeErr != nil {
		_ = parent.Remove(name)
		if copyErr != nil {
			return ObjectInfo{}, copyErr
		}
		if closeErr != nil {
			return ObjectInfo{}, closeErr
		}
		return ObjectInfo{}, ErrLengthMismatch
	}
	info, err := parent.Lstat(name)
	if err != nil || !info.Mode().IsRegular() || linkLike(info) {
		_ = parent.Remove(name)
		return ObjectInfo{}, errors.New("local object is not a regular file")
	}
	return ObjectInfo{Key: key, Size: size, LastModified: info.ModTime().UTC()}, nil
}

func (store *LocalStore) Open(_ context.Context, key string) (io.ReadCloser, ObjectInfo, error) {
	parent, name, err := store.openParent(key, false)
	if err != nil {
		return nil, ObjectInfo{}, err
	}
	defer parent.Close()
	info, err := parent.Lstat(name)
	if err != nil || !info.Mode().IsRegular() || linkLike(info) {
		if err != nil {
			return nil, ObjectInfo{}, err
		}
		return nil, ObjectInfo{}, ErrNotFound
	}
	file, err := parent.Open(name)
	if err != nil {
		return nil, ObjectInfo{}, err
	}
	opened, err := file.Stat()
	if err != nil || !opened.Mode().IsRegular() || !os.SameFile(info, opened) {
		_ = file.Close()
		return nil, ObjectInfo{}, errors.New("local object changed during open")
	}
	return file, ObjectInfo{Key: key, Size: opened.Size(), LastModified: opened.ModTime().UTC()}, nil
}

func (store *LocalStore) Delete(_ context.Context, key string) error {
	parent, name, err := store.openParent(key, false)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	defer parent.Close()
	info, err := parent.Lstat(name)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil || !info.Mode().IsRegular() || linkLike(info) {
		return errors.New("local object is not a regular file")
	}
	if err = parent.Remove(name); errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

// openParent walks each directory with a retained rooted handle. The Lstat /
// OpenRoot SameFile checks reject symlinks, junctions, reparse points, and
// concurrent component swaps; os.Root additionally prevents any escape even
// if an attacker races the checks.
func (store *LocalStore) openParent(key string, create bool) (*os.Root, string, error) {
	components, err := validObjectKey(key)
	if err != nil || store == nil || store.root == nil {
		return nil, "", errors.New("invalid object key")
	}
	current, err := store.root.OpenRoot(".")
	if err != nil {
		return nil, "", err
	}
	for _, component := range components[:len(components)-1] {
		if create {
			if mkdirErr := current.Mkdir(component, 0o700); mkdirErr != nil && !errors.Is(mkdirErr, os.ErrExist) {
				_ = current.Close()
				return nil, "", mkdirErr
			}
		}
		before, statErr := current.Lstat(component)
		if statErr != nil || !before.IsDir() || linkLike(before) || before.Mode().Perm()&0o077 != 0 {
			_ = current.Close()
			if statErr != nil {
				return nil, "", statErr
			}
			return nil, "", errors.New("local object path contains a link, reparse point, or unsafe directory")
		}
		next, openErr := current.OpenRoot(component)
		if openErr != nil {
			_ = current.Close()
			return nil, "", openErr
		}
		opened, openStatErr := next.Stat(".")
		if openStatErr != nil || !os.SameFile(before, opened) {
			_ = next.Close()
			_ = current.Close()
			return nil, "", errors.New("local object directory changed during traversal")
		}
		_ = current.Close()
		current = next
	}
	return current, components[len(components)-1], nil
}

func validObjectKey(key string) ([]string, error) {
	if key == "" || path.Clean(key) != key || strings.HasPrefix(key, "/") || strings.ContainsAny(key, "\\:") {
		return nil, errors.New("invalid object key")
	}
	components := strings.Split(key, "/")
	for _, component := range components {
		if component == "" || component == "." || component == ".." {
			return nil, errors.New("invalid object key")
		}
		for _, char := range component {
			if !(char == '-' || char == '_' || char >= '0' && char <= '9' || char >= 'a' && char <= 'z') {
				return nil, errors.New("invalid object key")
			}
		}
	}
	return components, nil
}

func linkLike(info os.FileInfo) bool {
	return info.Mode()&os.ModeSymlink != 0 || info.Mode()&os.ModeIrregular != 0
}
