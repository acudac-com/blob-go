package blob_test

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/acudac-com/blob-go"
)

func TestLocalFiles(t *testing.T) {
	ctx := context.Background()
	basePath := "test_local_files"
	defer os.RemoveAll(basePath) // Clean up after the test

	localFS := blob.NewFsStorage(basePath)
	key := "users/123/test_file.txt"
	data := []byte("Hello, Local Files!")

	// Write
	err := localFS.WriteIfMissing(ctx, key, data)
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

	// Remove
	err = localFS.Remove(ctx, key)
	if err != nil {
		t.Fatalf("Remove failed: %v", err)
	}

	_, err = localFS.Read(ctx, key)
	if err == nil {
		t.Fatalf("Read after Remove should have failed, but did not")
	}
}

func TestGcsBucket(t *testing.T) {
	ctx := context.Background()

	key := "users/123/test_object.txt"
	data := []byte("Hello, Google Cloud Storage!")
	gcs, err := blob.NewGcsStorage(ctx, os.Getenv("GCS_BUCKET"), "someprefix/sub")
	if err != nil {
		t.Fatal(err)
	}

	// Write
	err = gcs.WriteIfMissing(ctx, key, data)
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

	// Remove
	err = gcs.Remove(ctx, key)
	if err != nil {
		t.Fatalf("Remove failed: %v", err)
	}

	_, err = gcs.Read(ctx, key)
	if err == nil {
		t.Fatalf("Read after Remove should have failed, but did not")
	}
}

func TestGcsBucket_RemoveFolder(t *testing.T) {
	ctx := context.Background()
	gcs, err := blob.NewGcsStorage(ctx, os.Getenv("GCS_BUCKET"), "someprefix/sub")
	if err != nil {
		t.Fatal(err)
	}
	for i := range 20 {
		key := fmt.Sprintf("users/123/test_object_%d.txt", i)
		data := []byte("Hello, Google Cloud Storage!")

		// Write
		err = gcs.Write(ctx, key, data)
		if err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	// Remove folder
	err = gcs.RemoveFolder(ctx, "users/123")
	if err != nil {
		t.Fatalf("Remove folder failed: %v", err)
	}
}
