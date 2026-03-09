package metadata

import (
	"errors"
	"fmt"
	"sync"
)

// ShardStatus はシャードの現在の状態を表します。
type ShardStatus string

const (
	ShardStatusActive    ShardStatus = "Active"
	ShardStatusMigrating ShardStatus = "Migrating"
)

// DriveStatus は物理ドライブの現在の状態を表します。
type DriveStatus string

const (
	DriveStatusOnline      DriveStatus = "Online"
	DriveStatusMaintenance DriveStatus = "Maintenance"
	DriveStatusFull         DriveStatus = "Full"
)

// ObjectMeta は単一オブジェクトのメタデータを表します。
// 第1層: ObjectID -> ShardID
type ObjectMeta struct {
	ObjectID     string `json:"objectId"`
	ShardID      string `json:"shardId"`
	Size         int64  `json:"size"`
	DataShards   int    `json:"dataShards"`
	ParityShards int    `json:"parityShards"`
}

// ShardConfig は仮想シャードから物理ドライブへのマッピングを表します。
// 第2層: ShardID -> []PhysicalDriveID
type ShardConfig struct {
	ShardID       string      `json:"shardId"`
	DriveIDs      []string    `json:"driveIds"` // 20台のドライブIDリスト (16+4)
	Status        ShardStatus `json:"status"`
	TargetDriveID string      `json:"targetDriveId,omitempty"` // 移行用に予約されたターゲットドライブ
	SwapIndex     int         `json:"swapIndex,omitempty"`     // 置換対象となるDriveIDsのインデックス
}

// DriveInfo は物理ドライブのメタデータを表します。
// 第3層: DriveID -> Endpoint/Status
type DriveInfo struct {
	DriveID  string      `json:"driveId"`
	Endpoint string      `json:"endpoint"`
	Status   DriveStatus `json:"status"`
}

// FragmentMeta はフラグメントデータと共に物理ドライブ上に保存されます。
// これにより「自己修復（Self-describing）」なフラグメントが可能になり、中央KVが破損した場合の復旧が可能になります。
type FragmentMeta struct {
	ObjectMeta
	ShardID       string `json:"shardId"`
	FragmentIndex int    `json:"fragmentIndex"` // 0-19
	Checksum      string `json:"checksum"`      // HighwayHash等
}

// MetadataStore は分散KVストアを抽象化します。
type MetadataStore interface {
	// オブジェクト操作
	GetObject(objectID string) (*ObjectMeta, error)
	PutObject(meta *ObjectMeta) error

	// シャード操作
	GetShard(shardID string) (*ShardConfig, error)
	PutShard(config *ShardConfig) error

	// ドライブ操作
	GetDrive(driveID string) (*DriveInfo, error)
	PutDrive(info *DriveInfo) error

	// アトミック・リバランシング（再配置）操作
	BeginMigration(shardID string, targetDriveID string, swapIndex int) error
	CommitMigration(shardID string) error
}

// MemMetadataStore はデモンストレーション用のMetadataStoreのインメモリ実装です。
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
	// 保存されているポインタの外部からの意図しない変更を防ぐため、コピーを返します
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

// BeginMigration はシャードのフラグメントを新しいドライブへ移行する処理を開始します。
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

	// ターゲットドライブが存在し、オンラインであることを確認します
	drive, err := s.GetDrive(targetDriveID)
	if err != nil {
		return err
	}
	if drive.Status != DriveStatusOnline {
		return fmt.Errorf("target drive %s is not online", targetDriveID)
	}

	// シャードの状態を Migrating に更新します
	shard.Status = ShardStatusMigrating
	shard.TargetDriveID = targetDriveID
	shard.SwapIndex = swapIndex

	return s.PutShard(shard)
}

// CommitMigration はドライブリストを更新し、ステータスをリセットすることで移行を完了させます。
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

	// 物理ドライブIDを差し替えます
	shard.DriveIDs[shard.SwapIndex] = shard.TargetDriveID
	
	// 移行状態をリセットします
	shard.Status = ShardStatusActive
	shard.TargetDriveID = ""
	shard.SwapIndex = 0

	return s.PutShard(shard)
}
