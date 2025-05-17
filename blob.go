package blob

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"cloud.google.com/go/storage"
)

// A simplified interface for interacting with blob storage.
type BlobStorage interface {
	// Reads a blob
	Read(ctx context.Context, key string) ([]byte, error)
	// Writes a blob
	Write(ctx context.Context, key string, data []byte) error
	// Removes a blob if it exists
	Remove(ctx context.Context, key string) error
	// StreamRead returns an io.ReadCloser for reading a blob.
	StreamRead(ctx context.Context, key string) (io.ReadCloser, error)
	// StreamWrite returns an io.WriteCloser for writing a blob.
	StreamWrite(ctx context.Context, key string) (io.WriteCloser, error)
}

// Implements the BlobStorage interface for the local file system.
type LocalFiles struct {
	basePath string // Base path where blobs will be stored.
}

// Returns a new LocalFiles instance.
func NewLocalFiles(basePath string) *LocalFiles {
	return &LocalFiles{
		basePath: basePath,
	}
}

// Reads a blob from the local file system.
func (l *LocalFiles) Read(ctx context.Context, key string) ([]byte, error) {
	path := filepath.Join(l.basePath, key)
	return os.ReadFile(path)
}

// Writes a blob to the local file system.
func (l *LocalFiles) Write(ctx context.Context, key string, data []byte) error {
	path := filepath.Join(l.basePath, key)
	dir := filepath.Dir(path) // Ensure directory exists
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}
	return os.WriteFile(path, data, 0o644)
}

// Removes a blob from the local file system.
func (l *LocalFiles) Remove(ctx context.Context, key string) error {
	path := filepath.Join(l.basePath, key)
	return os.Remove(path)
}

// Returns an io.ReadCloser for streaming a blob from the local file system.
func (l *LocalFiles) StreamRead(ctx context.Context, key string) (io.ReadCloser, error) {
	path := filepath.Join(l.basePath, key)
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	return file, nil
}

// Returns an io.WriteCloser for streaming a blob to the local file system.
func (l *LocalFiles) StreamWrite(ctx context.Context, key string) (io.WriteCloser, error) {
	path := filepath.Join(l.basePath, key)
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}
	file, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("failed to create file: %w", err)
	}
	return file, nil
}

// GcsBucket implements BlobStorage for Google Cloud Storage.
type GcsBucket struct {
	bucket *storage.BucketHandle
}

// Returns a new GcsBucket instance.
func NewGcsBucket(ctx context.Context, bucket string) (*GcsBucket, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create client: %w", err)
	}
	return &GcsBucket{
		bucket: client.Bucket(bucket),
	}, nil
}

// Reads a blob from Google Cloud Storage.
func (g *GcsBucket) Read(ctx context.Context, key string) ([]byte, error) {
	rc, err := g.bucket.Object(key).NewReader(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create reader: %w", err)
	}
	defer rc.Close()

	return io.ReadAll(rc)
}

// Writes a blob to Google Cloud Storage.
func (g *GcsBucket) Write(ctx context.Context, key string, data []byte) error {
	wc := g.bucket.Object(key).NewWriter(ctx)
	defer wc.Close()

	if _, err := wc.Write(data); err != nil {
		return fmt.Errorf("failed to write data: %w", err)
	}
	return nil
}

// Remove removes a blob from Google Cloud Storage.
func (g *GcsBucket) Remove(ctx context.Context, key string) error {
	err := g.bucket.Object(key).Delete(ctx)
	if err != nil {
		return fmt.Errorf("failed to delete object: %w", err)
	}
	return nil
}

// StreamRead returns an io.ReadCloser for reading a blob from Google Cloud Storage.
func (g *GcsBucket) StreamRead(ctx context.Context, key string) (io.ReadCloser, error) {
	rc, err := g.bucket.Object(key).NewReader(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create reader: %w", err)
	}
	return rc, nil
}

// StreamWrite returns an io.WriteCloser for writing a blob to Google Cloud Storage.
func (g *GcsBucket) StreamWrite(ctx context.Context, key string) (io.WriteCloser, error) {
	wc := g.bucket.Object(key).NewWriter(ctx)
	return wc, nil
}

// Ensure that our types satisfy the interface
var (
	_ BlobStorage = &LocalFiles{}
	_ BlobStorage = &GcsBucket{}
)
