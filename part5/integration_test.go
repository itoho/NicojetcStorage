package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/itoho/NicojectStorage/part5/internal/decoder"
	"github.com/itoho/NicojectStorage/part5/internal/encoder"
	"github.com/itoho/NicojectStorage/part5/pkg/metadata"
	"github.com/itoho/NicojectStorage/part5/pkg/storage"
)

func TestIntegration_EncodeStoreRetrieveDecode(t *testing.T) {
	// 1. セットアップ
	// ------------------------------------------------------------------

	// -- メタデータストアのセットアップ
	dbPath := filepath.Join(t.TempDir(), "test-meta.db")
	metaStore, err := metadata.NewStore(dbPath)
	if err != nil {
		t.Fatalf("Failed to create metadata store: %v", err)
	}
	defer metaStore.Close()

	// -- ストレージマネージャーのセットアップ
	storageDir1 := t.TempDir()
	storageDir2 := t.TempDir()
	storageDir3 := t.TempDir()
	storageManager, err := storage.NewStorageManager([]string{storageDir1, storageDir2, storageDir3})
	if err != nil {
		t.Fatalf("Failed to create storage manager: %v", err)
	}

	// -- エンコーダーとデコーダーの初期化
	enc, err := encoder.New()
	if err != nil {
		t.Fatalf("Failed to create encoder: %v", err)
	}
	dec, err := decoder.New()
	if err != nil {
		t.Fatalf("Failed to create decoder: %v", err)
	}

	// 2. 書き込みフロー
	// ------------------------------------------------------------------

	// -- テストデータ
	objectKey := "my-integrated-test-object"
	originalData := []byte("This is a longer test string for our integrated object storage system. It needs to be long enough to be split into multiple shards.")

	// -- エンコード
	shards, err := enc.Encode(bytes.NewReader(originalData))
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	// -- 各シャードを保存
	shardPaths := make([]string, len(shards))
	for i, shardData := range shards {
		if len(shardData) == 0 {
			continue // 空のシャードは保存しない
		}
		shardID := uuid.New().String()
		path, err := storageManager.Store(shardID, shardData)
		if err != nil {
			t.Fatalf("Failed to store shard %d: %v", i, err)
		}
		shardPaths[i] = path
	}

	// -- メタデータを保存
	meta := metadata.ObjectMetadata{
		ObjectID:     objectKey,
		Size:         int64(len(originalData)),
		DataShards:   encoder.DataShards,
		ParityShards: encoder.ParityShards,
		ShardPaths:   shardPaths,
	}
	if err := metaStore.Put(objectKey, meta); err != nil {
		t.Fatalf("Failed to put metadata: %v", err)
	}

	// 3. 読み出し・復元フロー
	// ------------------------------------------------------------------

	// -- メタデータを取得
	retrievedMeta, err := metaStore.Get(objectKey)
	if err != nil {
		t.Fatalf("Failed to get metadata: %v", err)
	}

	// -- 意図的にシャードを2つ削除（復元可能な数）
	if err := os.Remove(retrievedMeta.ShardPaths[0]); err != nil {
		t.Fatalf("Failed to remove shard for test: %v", err)
	}
	if err := os.Remove(retrievedMeta.ShardPaths[2]); err != nil {
		t.Fatalf("Failed to remove shard for test: %v", err)
	}

	// -- ディスクからシャードを読み出し
	retrievedShards := make([][]byte, len(retrievedMeta.ShardPaths))
	for i, path := range retrievedMeta.ShardPaths {
		if path == "" {
			retrievedShards[i] = nil
			continue
		}
		data, err := storageManager.Retrieve(path)
		if err != nil {
			// ファイルが存在しないのは想定通りなので、スライスにはnilを入れる
			retrievedShards[i] = nil
		} else {
			retrievedShards[i] = data
		}
	}

	// -- デコード
	var resultBuffer bytes.Buffer
	if err := dec.Decode(retrievedShards, int(retrievedMeta.Size), &resultBuffer); err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	// 4. 検証
	// ------------------------------------------------------------------
	if !bytes.Equal(originalData, resultBuffer.Bytes()) {
		t.Errorf("Restored data does not match original data. got %s, want %s", resultBuffer.String(), string(originalData))
	}
}
