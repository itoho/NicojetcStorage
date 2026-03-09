package metadata

import (
	"fmt"
	"testing"
)

func TestRebalancing(t *testing.T) {
	store := NewMemMetadataStore()

	// 1. 初期ドライブのセットアップ (シャード用に20台)
	driveIDs := make([]string, 20)
	for i := 0; i < 20; i++ {
		driveID := fmt.Sprintf("drive-%d", i)
		driveIDs[i] = driveID
		store.PutDrive(&DriveInfo{
			DriveID:  driveID,
			Endpoint: fmt.Sprintf("192.168.1.%d:8080", i),
			Status:   DriveStatusOnline,
		})
	}

	// 2. シャードのセットアップ
	shardID := "shard-0"
	initialConfig := &ShardConfig{
		ShardID:  shardID,
		DriveIDs: driveIDs,
		Status:   ShardStatusActive,
	}
	store.PutShard(initialConfig)

	// 3. 移行先の新しいドライブのセットアップ
	newDriveID := "drive-new"
	store.PutDrive(&DriveInfo{
		DriveID:  newDriveID,
		Endpoint: "192.168.1.100:8080",
		Status:   DriveStatusOnline,
	})

	// 4. 移行（リバランシング）の開始 (シャードの1/20を再配置)
	// インデックス5のドライブを新しいドライブと入れ替える
	swapIndex := 5
	oldDriveID := driveIDs[swapIndex]
	err := store.BeginMigration(shardID, newDriveID, swapIndex)
	if err != nil {
		t.Fatalf("BeginMigration failed: %v", err)
	}

	// シャードの状態を確認
	shard, _ := store.GetShard(shardID)
	if shard.Status != ShardStatusMigrating {
		t.Errorf("Expected status Migrating, got %s", shard.Status)
	}
	if shard.TargetDriveID != newDriveID {
		t.Errorf("Expected target drive %s, got %s", newDriveID, shard.TargetDriveID)
	}
	if shard.DriveIDs[swapIndex] != oldDriveID {
		t.Errorf("CommitされるまでswapIndexのDriveIDは変更されないはずです")
	}

	// 5. 移行の完了 (Commit)
	err = store.CommitMigration(shardID)
	if err != nil {
		t.Fatalf("CommitMigration failed: %v", err)
	}

	// 6. 最終状態の確認
	shard, _ = store.GetShard(shardID)
	if shard.Status != ShardStatusActive {
		t.Errorf("Expected status Active, got %s", shard.Status)
	}
	if shard.DriveIDs[swapIndex] != newDriveID {
		t.Errorf("インデックス%dのDriveIDが%sに更新されているはずですが、%sでした", swapIndex, newDriveID, shard.DriveIDs[swapIndex])
	}
	if shard.TargetDriveID != "" {
		t.Errorf("TargetDriveIDは空であるはずですが、%sでした", shard.TargetDriveID)
	}

	fmt.Printf("Rebalancing successful: %s at index %d replaced by %s", oldDriveID, swapIndex, newDriveID)
}
