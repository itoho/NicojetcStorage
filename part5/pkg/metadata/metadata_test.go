package metadata

import (
	"os"
	"reflect"
	"testing"
	"time"
)

// createTempDB はテスト用の一次的なBoltDBファイルを作成し、そのパスを返します。
func createTempDB(t *testing.T) string {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "test-*.db")
	if err != nil {
		t.Fatalf("Failed to create temp db file: %v", err)
	}
	path := f.Name()
	f.Close()
	return path
}

func TestMetadataStore_PutAndGet(t *testing.T) {
	dbPath := createTempDB(t)
	store, err := NewStore(dbPath)
	if err != nil {
		t.Fatalf("Failed to create new store: %v", err)
	}
	defer store.Close()

	// テスト用のメタデータを作成
	testMeta := ObjectMetadata{
		ObjectID:     "test-object-123",
		Size:         1024,
		DataShards:   10, // テスト用の値
		ParityShards: 4,  // テスト用の値
		ShardPaths:   []string{"/path/to/shard1", "/path/to/shard2"},
		CreatedAt:    time.Now().UTC().Truncate(time.Second), // 丸めて比較しやすくする
	}
	objectKey := "my-test-key"

	// データを保存
	err = store.Put(objectKey, testMeta)
	if err != nil {
		t.Fatalf("Put() failed: %v", err)
	}

	// データを取得
	retrievedMeta, err := store.Get(objectKey)
	if err != nil {
		t.Fatalf("Get() failed: %v", err)
	}

	// 取得したデータが元のデータと一致するか確認
	if !reflect.DeepEqual(testMeta, *retrievedMeta) {
		t.Errorf("Get() got = %v, want = %v", *retrievedMeta, testMeta)
	}
}

func TestMetadataStore_GetNotFound(t *testing.T) {
	dbPath := createTempDB(t)
	store, err := NewStore(dbPath)
	if err != nil {
		t.Fatalf("Failed to create new store: %v", err)
	}
	defer store.Close()

	// 存在しないキーでデータを取得
	_, err = store.Get("non-existent-key")
	if err == nil {
		t.Errorf("Expected an error when getting a non-existent key, but got nil")
	}
}
