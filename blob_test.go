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
	key := "test_file.txt"
	data := []byte("Hello, Local Files!")

	// Write
	err := localFS.Write(ctx, key, data)
	if err != nil {
		t.Errorf("Write failed: %v", err)
	}

	// Read
	readData, err := localFS.Read(ctx, key)
	if err != nil {
		t.Errorf("Read failed: %v", err)
	}
	if !reflect.DeepEqual(data, readData) {
		t.Errorf("Read data does not match written data. Expected: %v, Got: %v", data, readData)
	}

	// StreamRead
	streamRead, err := localFS.StreamRead(ctx, key)
	if err != nil {
		t.Errorf("StreamRead failed: %v", err)
	}
	readStreamData, err := io.ReadAll(streamRead)
	if err != nil {
		t.Errorf("Error reading from stream: %v", err)
	}
	if !reflect.DeepEqual(data, readStreamData) {
		t.Errorf("StreamRead data does not match written data. Expected: %v, Got: %v", data, readStreamData)
	}
	streamRead.Close()

	// StreamWrite
	streamKey := "test_stream_file.txt"
	streamWrite, err := localFS.StreamWrite(ctx, streamKey)
	if err != nil {
		t.Errorf("StreamWrite failed: %v", err)
	}
	_, err = streamWrite.Write(data)
	if err != nil {
		t.Errorf("Stream Write failed: %v", err)
	}
	if err := streamWrite.Close(); err != nil {
		t.Errorf("StreamWrite Close failed: %v", err)
	}

	// Read the data back to confirm
	readStreamedData, err := localFS.Read(ctx, streamKey)
	if err != nil {
		t.Errorf("Read after StreamWrite failed: %v", err)
	}
	if !reflect.DeepEqual(data, readStreamedData) {
		t.Errorf("StreamWrite data does not match written data. Expected: %v, Got: %v", data, readStreamedData)
	}

	// Remove
	err = localFS.Remove(ctx, key)
	if err != nil {
		t.Errorf("Remove failed: %v", err)
	}

	_, err = localFS.Read(ctx, key)
	if err == nil {
		t.Errorf("Read after Remove should have failed, but did not")
	}

	err = localFS.Remove(ctx, streamKey)
	if err != nil {
		t.Errorf("Remove failed: %v", err)
	}
	_, err = localFS.Read(ctx, streamKey)
	if err == nil {
		t.Errorf("Read after Remove should have failed, but did not")
	}
}

func TestGcsBucket(t *testing.T) {
	ctx := context.Background()

	key := "test_object.txt"
	data := []byte("Hello, GCS!")
	gcs, err := blob.NewGcsBucket(ctx, "BUCKET")
	if err != nil {
		t.Fatal(err)
	}

	// Write
	err = gcs.Write(ctx, key, data)
	if err != nil {
		t.Errorf("Write failed: %v", err)
	}

	// Read
	readData, err := gcs.Read(ctx, key)
	if err != nil {
		t.Errorf("Read failed: %v", err)
	}
	if !reflect.DeepEqual(data, readData) {
		t.Errorf("Read data does not match written data. Expected: %v, Got: %v", data, readData)
	}

	// StreamRead
	streamRead, err := gcs.StreamRead(ctx, key)
	if err != nil {
		t.Errorf("StreamRead failed: %v", err)
	}
	readStreamData, err := io.ReadAll(streamRead)
	if err != nil {
		t.Errorf("Error reading from stream: %v", err)
	}
	if !reflect.DeepEqual(data, readStreamData) {
		t.Errorf("StreamRead data does not match written data. Expected: %v, Got: %v", data, readStreamData)
	}
	streamRead.Close()

	// StreamWrite
	streamKey := "test_stream_object.txt"
	streamWrite, err := gcs.StreamWrite(ctx, streamKey)
	if err != nil {
		t.Errorf("StreamWrite failed: %v", err)
	}
	_, err = streamWrite.Write(data)
	if err != nil {
		t.Errorf("StreamWrite Write failed: %v", err)
	}
	if err := streamWrite.Close(); err != nil {
		t.Errorf("StreamWrite Close failed: %v", err)
	}

	// Read the data back
	readStreamedData, err := gcs.Read(ctx, streamKey)
	if err != nil {
		t.Errorf("Read failed: %v", err)
	}
	if !reflect.DeepEqual(data, readStreamedData) {
		t.Errorf("StreamWrite data does not match written data. Expected: %v, Got: %v", data, readStreamedData)
	}

	// Remove
	err = gcs.Remove(ctx, key)
	if err != nil {
		t.Errorf("Remove failed: %v", err)
	}

	_, err = gcs.Read(ctx, key)
	if err == nil {
		t.Errorf("Read after Remove should have failed, but did not")
	}

	err = gcs.Remove(ctx, streamKey)
	if err != nil {
		t.Errorf("Remove failed: %v", err)
	}
	_, err = gcs.Read(ctx, streamKey)
	if err == nil {
		t.Errorf("Read after Remove should have failed, but did not")
	}
}
