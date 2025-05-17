package blob_test

import (
	"context"
	"io"
	"os"
	"reflect"
	"testing"

	"github.com/acudac-com/blob-go"
)

func TestLocalFiles(t *testing.T) {
	ctx := context.Background()
	basePath := "test_local_files"
	defer os.RemoveAll(basePath) // Clean up after the test

	localFS := blob.NewLocalFiles(basePath)
	key := "users/123/test_file.txt"
	data := []byte("Hello, Local Files!")

	// Write
	err := localFS.Write(ctx, key, data)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Read
	readData, err := localFS.Read(ctx, key)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if !reflect.DeepEqual(data, readData) {
		t.Fatalf("Read data does not match written data. Expected: %v, Got: %v", data, readData)
	}

	// StreamRead
	streamRead, err := localFS.StreamRead(ctx, key)
	if err != nil {
		t.Fatalf("StreamRead failed: %v", err)
	}
	readStreamData, err := io.ReadAll(streamRead)
	if err != nil {
		t.Fatalf("Error reading from stream: %v", err)
	}
	if !reflect.DeepEqual(data, readStreamData) {
		t.Fatalf("StreamRead data does not match written data. Expected: %v, Got: %v", data, readStreamData)
	}
	streamRead.Close()

	// StreamWrite
	streamKey := "users/123/test_stream_file.txt"
	streamWrite, err := localFS.StreamWrite(ctx, streamKey)
	if err != nil {
		t.Fatalf("StreamWrite failed: %v", err)
	}
	_, err = streamWrite.Write(data)
	if err != nil {
		t.Fatalf("Stream Write failed: %v", err)
	}
	if err := streamWrite.Close(); err != nil {
		t.Fatalf("StreamWrite Close failed: %v", err)
	}

	// Read the data back to confirm
	readStreamedData, err := localFS.Read(ctx, streamKey)
	if err != nil {
		t.Fatalf("Read after StreamWrite failed: %v", err)
	}
	if !reflect.DeepEqual(data, readStreamedData) {
		t.Fatalf("StreamWrite data does not match written data. Expected: %v, Got: %v", data, readStreamedData)
	}

	// Remove
	err = localFS.Remove(ctx, key)
	if err != nil {
		t.Fatalf("Remove failed: %v", err)
	}

	_, err = localFS.Read(ctx, key)
	if err == nil {
		t.Fatalf("Read after Remove should have failed, but did not")
	}

	err = localFS.Remove(ctx, streamKey)
	if err != nil {
		t.Fatalf("Remove failed: %v", err)
	}
	_, err = localFS.Read(ctx, streamKey)
	if err == nil {
		t.Fatalf("Read after Remove should have failed, but did not")
	}
}

func TestGcsBucket(t *testing.T) {
	ctx := context.Background()

	key := "users/123/test_object.txt"
	data := []byte("Hello, GCS!")
	gcs, err := blob.NewGcsBucket(ctx, os.Getenv("FILES_BUCKET"))
	if err != nil {
		t.Fatal(err)
	}

	// Write
	err = gcs.Write(ctx, key, data)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Read
	readData, err := gcs.Read(ctx, key)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if !reflect.DeepEqual(data, readData) {
		t.Fatalf("Read data does not match written data. Expected: %v, Got: %v", data, readData)
	}

	// StreamRead
	streamRead, err := gcs.StreamRead(ctx, key)
	if err != nil {
		t.Fatalf("StreamRead failed: %v", err)
	}
	readStreamData, err := io.ReadAll(streamRead)
	if err != nil {
		t.Fatalf("Error reading from stream: %v", err)
	}
	if !reflect.DeepEqual(data, readStreamData) {
		t.Fatalf("StreamRead data does not match written data. Expected: %v, Got: %v", data, readStreamData)
	}
	streamRead.Close()

	// StreamWrite
	streamKey := "users/123/test_stream_object.txt"
	streamWrite, err := gcs.StreamWrite(ctx, streamKey)
	if err != nil {
		t.Fatalf("StreamWrite failed: %v", err)
	}
	_, err = streamWrite.Write(data)
	if err != nil {
		t.Fatalf("StreamWrite Write failed: %v", err)
	}
	if err := streamWrite.Close(); err != nil {
		t.Fatalf("StreamWrite Close failed: %v", err)
	}

	// Read the data back
	readStreamedData, err := gcs.Read(ctx, streamKey)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if !reflect.DeepEqual(data, readStreamedData) {
		t.Fatalf("StreamWrite data does not match written data. Expected: %v, Got: %v", data, readStreamedData)
	}

	// Remove
	err = gcs.Remove(ctx, key)
	if err != nil {
		t.Fatalf("Remove failed: %v", err)
	}

	_, err = gcs.Read(ctx, key)
	if err == nil {
		t.Fatalf("Read after Remove should have failed, but did not")
	}

	err = gcs.Remove(ctx, streamKey)
	if err != nil {
		t.Fatalf("Remove failed: %v", err)
	}
	_, err = gcs.Read(ctx, streamKey)
	if err == nil {
		t.Fatalf("Read after Remove should have failed, but did not")
	}
}
