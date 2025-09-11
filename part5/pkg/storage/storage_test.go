package storage

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestStorageManager_StoreAndRetrieve(t *testing.T) {
	// テスト用のストレージディレクトリを複数作成
	dir1 := t.TempDir()
	dir2 := t.TempDir()
	storageDirs := []string{dir1, dir2}

	// StorageManagerを初期化
	sm, err := NewStorageManager(storageDirs)
	if err != nil {
		t.Fatalf("NewStorageManager() failed: %v", err)
	}

	// テストデータとシャードID
	testData := []byte("this is a test shard data")
	shardID := "test-shard-id-123"

	// データを保存
	storedPath, err := sm.Store(shardID, testData)
	if err != nil {
		t.Fatalf("Store() failed: %v", err)
	}

	// 返されたパスが正しいか検証
	found := false
	for _, dir := range storageDirs {
		if filepath.Dir(storedPath) == dir {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Store() returned a path in an unexpected directory: %s", storedPath)
	}

	// ファイルが実際に存在するか確認
	if _, err := os.Stat(storedPath); os.IsNotExist(err) {
		t.Errorf("Store() did not create the file at %s", storedPath)
	}

	// データを取得
	retrievedData, err := sm.Retrieve(storedPath)
	if err != nil {
		t.Fatalf("Retrieve() failed: %v", err)
	}

	// 取得したデータが元のデータと一致するか確認
	if !bytes.Equal(testData, retrievedData) {
		t.Errorf("Retrieve() got = %s, want = %s", retrievedData, testData)
	}
}

func TestNewStorageManager_NoDirs(t *testing.T) {
	// ディレクトリが指定されなかった場合にエラーになることを確認
	_, err := NewStorageManager([]string{})
	if err == nil {
		t.Errorf("NewStorageManager() with no dirs should have failed, but it did not")
	}
}
