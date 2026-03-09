package metadata

import (
	"fmt"
	"testing"
)

func TestRebalancing(t *testing.T) {
	store := NewMemMetadataStore()

	// 1. Setup initial drives (20 drives for a shard)
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

	// 2. Setup a shard
	shardID := "shard-0"
	initialConfig := &ShardConfig{
		ShardID:  shardID,
		DriveIDs: driveIDs,
		Status:   ShardStatusActive,
	}
	store.PutShard(initialConfig)

	// 3. Setup a new drive to migrate to
	newDriveID := "drive-new"
	store.PutDrive(&DriveInfo{
		DriveID:  newDriveID,
		Endpoint: "192.168.1.100:8080",
		Status:   DriveStatusOnline,
	})

	// 4. Begin Migration (rebalancing 1/20 of the shard)
	// Swap drive at index 5 with the new drive
	swapIndex := 5
	oldDriveID := driveIDs[swapIndex]
	err := store.BeginMigration(shardID, newDriveID, swapIndex)
	if err != nil {
		t.Fatalf("BeginMigration failed: %v", err)
	}

	// Verify shard status
	shard, _ := store.GetShard(shardID)
	if shard.Status != ShardStatusMigrating {
		t.Errorf("Expected status Migrating, got %s", shard.Status)
	}
	if shard.TargetDriveID != newDriveID {
		t.Errorf("Expected target drive %s, got %s", newDriveID, shard.TargetDriveID)
	}
	if shard.DriveIDs[swapIndex] != oldDriveID {
		t.Errorf("DriveID at swapIndex should not change until Commit")
	}

	// 5. Commit Migration
	err = store.CommitMigration(shardID)
	if err != nil {
		t.Fatalf("CommitMigration failed: %v", err)
	}

	// 6. Verify final state
	shard, _ = store.GetShard(shardID)
	if shard.Status != ShardStatusActive {
		t.Errorf("Expected status Active, got %s", shard.Status)
	}
	if shard.DriveIDs[swapIndex] != newDriveID {
		t.Errorf("Expected DriveID at index %d to be %s, got %s", swapIndex, newDriveID, shard.DriveIDs[swapIndex])
	}
	if shard.TargetDriveID != "" {
		t.Errorf("Expected TargetDriveID to be empty, got %s", shard.TargetDriveID)
	}

	fmt.Printf("Rebalancing successful: %s at index %d replaced by %s\n", oldDriveID, swapIndex, newDriveID)
}
