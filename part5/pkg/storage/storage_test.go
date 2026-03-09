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

	var sm *StorageManager
	var err error
	t.Run("StorageManagerの初期化", func(t *testing.T) {
		sm, err = NewStorageManager(storageDirs)
		if err != nil {
			t.Fatalf("NewStorageManager() failed: %v", err)
		}
	})

	// テストデータとシャードID
	testData := []byte("this is a test shard data")
	shardID := "test-shard-id-123"
	var storedPath string

	t.Run("データの保存", func(t *testing.T) {
		storedPath, err = sm.Store(shardID, testData)
		if err != nil {
			t.Fatalf("Store() failed: %v", err)
		}
	})

	t.Run("保存パスの検証", func(t *testing.T) {
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
	})

	t.Run("ファイルの存在確認", func(t *testing.T) {
		if _, err := os.Stat(storedPath); os.IsNotExist(err) {
			t.Errorf("Store() did not create the file at %s", storedPath)
		}
	})

	var retrievedData []byte
	t.Run("データの取得", func(t *testing.T) {
		retrievedData, err = sm.Retrieve(storedPath)
		if err != nil {
			t.Fatalf("Retrieve() failed: %v", err)
		}
	})

	t.Run("データの整合性検証", func(t *testing.T) {
		if !bytes.Equal(testData, retrievedData) {
			t.Errorf("Retrieve() got = %s, want = %s", retrievedData, testData)
		}
	})
}

func TestNewStorageManager_NoDirs(t *testing.T) {
	// ディレクトリが指定されなかった場合にエラーになることを確認
	_, err := NewStorageManager([]string{})
	if err == nil {
		t.Errorf("NewStorageManager() with no dirs should have failed, but it did not")
	}
}
