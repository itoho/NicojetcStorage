package storage

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"time"
)

// StorageManager は複数のストレージディレクトリを管理し、シャードの永続化を担当します。
type StorageManager struct {
	dirs []string
	rand *rand.Rand
}

// NewStorageManager は新しいStorageManagerを初期化します。
// 指定されたディレクトリが存在しない場合は作成します。
func NewStorageManager(dirs []string) (*StorageManager, error) {
	if len(dirs) == 0 {
		return nil, fmt.Errorf("at least one storage directory must be provided")
	}

	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create storage directory %s: %w", dir, err)
		}
	}

	// 乱数生成器を初期化
	source := rand.NewSource(time.Now().UnixNano())
	r := rand.New(source)

	return &StorageManager{dirs: dirs, rand: r}, nil
}

// Store はシャードデータをランダムに選択されたストレージディレクトリに保存します。
// 保存先のフルパスを返します。
func (sm *StorageManager) Store(shardID string, data []byte) (string, error) {
	// ランダムに保存先ディレクトリを選択
	dirIndex := sm.rand.Intn(len(sm.dirs))
	destDir := sm.dirs[dirIndex]

	// 保存パスを構築
	path := filepath.Join(destDir, shardID)

	// データをファイルに書き込む
	if err := os.WriteFile(path, data, 0644); err != nil {
		return "", fmt.Errorf("failed to write shard to %s: %w", path, err)
	}

	return path, nil
}

// Retrieve は指定されたパスからシャードデータを読み込みます。
func (sm *StorageManager) Retrieve(path string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read shard from %s: %w", path, err)
	}
	return data, nil
}
