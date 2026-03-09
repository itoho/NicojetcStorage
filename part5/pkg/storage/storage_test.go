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

func TestNewStorageManager(t *testing.T) {
	t.Run("ディレクトリ未指定時にエラーを返すこと", func(t *testing.T) {
		_, err := NewStorageManager([]string{})
		if err == nil {
			t.Error("NewStorageManager() with no dirs should have failed, but it did not")
		}
	})

	t.Run("指定されたディレクトリが自動的に作成されること", func(t *testing.T) {
		parentDir := t.TempDir()
		targetDir := filepath.Join(parentDir, "auto-created-dir")

		_, err := NewStorageManager([]string{targetDir})
		if err != nil {
			t.Fatalf("NewStorageManager() failed: %v", err)
		}

		if _, err := os.Stat(targetDir); os.IsNotExist(err) {
			t.Errorf("Directory %s was not created", targetDir)
		}
	})
}
