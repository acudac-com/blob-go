package blob

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"

	"cloud.google.com/go/storage"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
)

// A simplified interface for interacting with blob storage.
type Storage interface {
	// Reads a blob
	Read(ctx context.Context, key string) ([]byte, error)
	// Writes a blob
	Write(ctx context.Context, key string, data []byte) error
	// Writes a blob if the key does not contain any data yet
	WriteIfMissing(ctx context.Context, key string, data []byte) error
	// Removes a blob if it exists
	Remove(ctx context.Context, key string) error
	// Removes a folder and all children blobs
	RemoveFolder(ctx context.Context, folder string) error
}

// Implements the Storage interface for the local file system.
type Fs struct {
	basePath string // Base path where blobs will be stored.
}

// Returns a new Fs instance.
func NewFsStorage(basePath string) *Fs {
	return &Fs{
		basePath: basePath,
	}
}

// Reads a blob from the local file system.
func (l *Fs) Read(ctx context.Context, key string) ([]byte, error) {
	path := filepath.Join(l.basePath, key)
	return os.ReadFile(path)
}

// Writes a blob to the local file system.
func (l *Fs) Write(ctx context.Context, key string, data []byte) error {
	path := filepath.Join(l.basePath, key)
	dir := filepath.Dir(path) // Ensure directory exists
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("creating directory: %w", err)
	}
	return os.WriteFile(path, data, 0o644)
}

// Writes a blob to the local file system if the key does not contain any data yet
func (l *Fs) WriteIfMissing(ctx context.Context, key string, data []byte) error {
	path := filepath.Join(l.basePath, key)
	dir := filepath.Dir(path) // Ensure directory exists
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("creating directory: %w", err)
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		if os.IsExist(err) {
			return nil // File already exists
		}
		return fmt.Errorf("opening file with O_EXCL: %w", err)
	}
	defer f.Close()

	// If we reached here, the file was just created exclusively.
	// Now we can safely write to it.
	_, err = f.WriteAt(data, 0)
	if err != nil {
		return fmt.Errorf("writing data: %w", err)
	}
	return nil
}

// Removes a blob from the local file system.
func (l *Fs) Remove(ctx context.Context, key string) error {
	path := filepath.Join(l.basePath, key)
	return os.Remove(path)
}

// Removes a folder
func (l *Fs) RemoveFolder(ctx context.Context, folder string) error {
	path := filepath.Join(l.basePath, folder)
	err := os.RemoveAll(path)
	if err != nil {
		return fmt.Errorf("removing folder: %w", err)
	}
	return nil
}

// Gcs implements Storage for Google Cloud Storage.
type Gcs struct {
	bucket *storage.BucketHandle
	prefix string
}

// Returns a new Gcs blob storage instance.
func NewGcsStorage(ctx context.Context, bucket string, prefix string) (*Gcs, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("creating client: %w", err)
	}
	return &Gcs{client.Bucket(bucket), prefix}, nil
}

// Reads a blob from Google Cloud Storage.
func (g *Gcs) Read(ctx context.Context, key string) ([]byte, error) {
	key = path.Join(g.prefix, key)
	rc, err := g.bucket.Object(key).NewReader(ctx)
	if err != nil {
		return nil, fmt.Errorf("creating reader: %w", err)
	}
	defer rc.Close()

	return io.ReadAll(rc)
}

// Writes a blob to Google Cloud Storage.
func (g *Gcs) Write(ctx context.Context, key string, data []byte) error {
	key = path.Join(g.prefix, key)
	wc := g.bucket.Object(key).NewWriter(ctx)

	if _, err := wc.Write(data); err != nil {
		return fmt.Errorf("writing: %w", err)
	}
	if err := wc.Close(); err != nil {
		return fmt.Errorf("closing writer: %w", err)
	}
	return nil
}

// Writes a blob to Google Cloud Storage if the key does not contain any data yet
func (g *Gcs) WriteIfMissing(ctx context.Context, key string, data []byte) error {
	key = path.Join(g.prefix, key)
	wc := g.bucket.Object(key).If(storage.Conditions{DoesNotExist: true}).NewWriter(ctx)

	if _, err := wc.Write(data); err != nil {
		return fmt.Errorf("writing: %w", err)
	}
	if err := wc.Close(); err != nil {
		if apiErr, ok := err.(*googleapi.Error); ok && apiErr.Code == 412 {
			return nil
		}
		return fmt.Errorf("closing writer: %w", err)
	}
	return nil
}

// Remove removes a blob from Google Cloud Storage.
func (g *Gcs) Remove(ctx context.Context, key string) error {
	key = path.Join(g.prefix, key)
	err := g.bucket.Object(key).Delete(ctx)
	if err != nil {
		return fmt.Errorf("deleting object: %w", err)
	}
	return nil
}

// Removes all objects at the specified folder (prefix)
func (g *Gcs) RemoveFolder(ctx context.Context, folder string) error {
	folder = path.Join(g.prefix, folder)
	it := g.bucket.Objects(ctx, &storage.Query{Prefix: folder + "/"})
	errG, ctx := errgroup.WithContext(ctx)
	for {
		objAttrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("iterating objects: %w", err)
		}
		errG.Go(func() error {
			err = g.bucket.Object(objAttrs.Name).Delete(ctx)
			if err != nil {
				return fmt.Errorf("deleting object: %w", err)
			}
			return nil
		})
	}
	if err := errG.Wait(); err != nil {
		return fmt.Errorf("waiting for delete operations: %w", err)
	}
	return nil
}

// Ensure that our types satisfy the interface
var (
	_ Storage = &Fs{}
	_ Storage = &Gcs{}
)
