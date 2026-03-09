package metadata

import (
	"errors"
	"fmt"
	"sync"
)

// ShardStatus represents the current state of a shard.
type ShardStatus string

const (
	ShardStatusActive    ShardStatus = "Active"
	ShardStatusMigrating ShardStatus = "Migrating"
)

// DriveStatus represents the current state of a physical drive.
type DriveStatus string

const (
	DriveStatusOnline      DriveStatus = "Online"
	DriveStatusMaintenance DriveStatus = "Maintenance"
	DriveStatusFull         DriveStatus = "Full"
)

// ObjectMeta represents the metadata for a single object.
// Tier 1: ObjectID -> ShardID
type ObjectMeta struct {
	ObjectID     string `json:"objectId"`
	ShardID      string `json:"shardId"`
	Size         int64  `json:"size"`
	DataShards   int    `json:"dataShards"`
	ParityShards int    `json:"parityShards"`
}

// ShardConfig represents the mapping of a virtual shard to physical drives.
// Tier 2: ShardID -> []PhysicalDriveID
type ShardConfig struct {
	ShardID       string      `json:"shardId"`
	DriveIDs      []string    `json:"driveIds"` // List of 20 DriveIDs (16+4)
	Status        ShardStatus `json:"status"`
	TargetDriveID string      `json:"targetDriveId,omitempty"` // Reserved for migration
	SwapIndex     int         `json:"swapIndex,omitempty"`     // Index in DriveIDs to be replaced
}

// DriveInfo represents the metadata for a physical drive.
// Tier 3: DriveID -> Endpoint/Status
type DriveInfo struct {
	DriveID  string      `json:"driveId"`
	Endpoint string      `json:"endpoint"`
	Status   DriveStatus `json:"status"`
}

// FragmentMeta is stored on the physical drive along with the fragment data.
// This allows for "self-describing" fragments and recovery if the central KV is lost.
type FragmentMeta struct {
	ObjectMeta
	ShardID       string `json:"shardId"`
	FragmentIndex int    `json:"fragmentIndex"` // 0-19
	Checksum      string `json:"checksum"`      // HighwayHash or similar
}

// MetadataStore abstracts the distributed KV store.
type MetadataStore interface {
	// Object operations
	GetObject(objectID string) (*ObjectMeta, error)
	PutObject(meta *ObjectMeta) error

	// Shard operations
	GetShard(shardID string) (*ShardConfig, error)
	PutShard(config *ShardConfig) error

	// Drive operations
	GetDrive(driveID string) (*DriveInfo, error)
	PutDrive(info *DriveInfo) error

	// Atomic Rebalancing Operations
	BeginMigration(shardID string, targetDriveID string, swapIndex int) error
	CommitMigration(shardID string) error
}

// MemMetadataStore is an in-memory implementation of MetadataStore for demonstration.
type MemMetadataStore struct {
	mu      sync.RWMutex
	objects sync.Map // map[string]*ObjectMeta
	shards  sync.Map // map[string]*ShardConfig
	drives  sync.Map // map[string]*DriveInfo
}

func NewMemMetadataStore() *MemMetadataStore {
	return &MemMetadataStore{}
}

func (s *MemMetadataStore) GetObject(objectID string) (*ObjectMeta, error) {
	val, ok := s.objects.Load(objectID)
	if !ok {
		return nil, errors.New("object not found")
	}
	return val.(*ObjectMeta), nil
}

func (s *MemMetadataStore) PutObject(meta *ObjectMeta) error {
	s.objects.Store(meta.ObjectID, meta)
	return nil
}

func (s *MemMetadataStore) GetShard(shardID string) (*ShardConfig, error) {
	val, ok := s.shards.Load(shardID)
	if !ok {
		return nil, errors.New("shard not found")
	}
	// Return a copy to avoid external modification of the stored pointer
	orig := val.(*ShardConfig)
	copyIDs := make([]string, len(orig.DriveIDs))
	copy(copyIDs, orig.DriveIDs)
	return &ShardConfig{
		ShardID:       orig.ShardID,
		DriveIDs:      copyIDs,
		Status:        orig.Status,
		TargetDriveID: orig.TargetDriveID,
		SwapIndex:     orig.SwapIndex,
	}, nil
}

func (s *MemMetadataStore) PutShard(config *ShardConfig) error {
	s.shards.Store(config.ShardID, config)
	return nil
}

func (s *MemMetadataStore) GetDrive(driveID string) (*DriveInfo, error) {
	val, ok := s.drives.Load(driveID)
	if !ok {
		return nil, errors.New("drive not found")
	}
	return val.(*DriveInfo), nil
}

func (s *MemMetadataStore) PutDrive(info *DriveInfo) error {
	s.drives.Store(info.DriveID, info)
	return nil
}

// BeginMigration starts the migration of a shard\s fragment to a new drive.
func (s *MemMetadataStore) BeginMigration(shardID string, targetDriveID string, swapIndex int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	shard, err := s.GetShard(shardID)
	if err != nil {
		return err
	}

	if shard.Status == ShardStatusMigrating {
		return fmt.Errorf("shard %s is already migrating", shardID)
	}

	if swapIndex < 0 || swapIndex >= len(shard.DriveIDs) {
		return fmt.Errorf("invalid swap index %d", swapIndex)
	}

	// Verify target drive exists and is online
	drive, err := s.GetDrive(targetDriveID)
	if err != nil {
		return err
	}
	if drive.Status != DriveStatusOnline {
		return fmt.Errorf("target drive %s is not online", targetDriveID)
	}

	// Update shard state to Migrating
	shard.Status = ShardStatusMigrating
	shard.TargetDriveID = targetDriveID
	shard.SwapIndex = swapIndex

	return s.PutShard(shard)
}

// CommitMigration completes the migration by updating the drive list and resetting status.
func (s *MemMetadataStore) CommitMigration(shardID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	shard, err := s.GetShard(shardID)
	if err != nil {
		return err
	}

	if shard.Status != ShardStatusMigrating {
		return fmt.Errorf("shard %s is not in migrating state", shardID)
	}

	// Swap the physical drive ID
	shard.DriveIDs[shard.SwapIndex] = shard.TargetDriveID
	
	// Reset migration state
	shard.Status = ShardStatusActive
	shard.TargetDriveID = ""
	shard.SwapIndex = 0

	return s.PutShard(shard)
}
