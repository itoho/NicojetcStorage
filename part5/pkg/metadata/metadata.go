package metadata

import (
	"encoding/json"
	"time"

	"go.etcd.io/bbolt"
)

const (
	// bucketName はメタデータを保存するBoltDBのバケット名を定義します。
	bucketName = "objects"
)

// ObjectMetadata はオブジェクトのメタ情報を保持します。
// 将来の拡張性を考慮し、jsonタグを付与しています。
type ObjectMetadata struct {
	ObjectID     string    `json:"objectId"`
	Size         int64     `json:"size"`
	DataShards   int       `json:"dataShards"`
	ParityShards int       `json:"parityShards"`
	ShardPaths   []string  `json:"shardPaths"`
	CreatedAt    time.Time `json:"createdAt"`
}

// MetadataStore はBoltDBをラップし、メタデータ操作を提供します。
type MetadataStore struct {
	db *bbolt.DB
}

// NewStore は新しいMetadataStoreを作成または既存のものを開きます。
func NewStore(dbPath string) (*MetadataStore, error) {
	db, err := bbolt.Open(dbPath, 0600, &bbolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, err
	}

	// トランザクションを開始し、バケットが存在しない場合は作成します。
	err = db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(bucketName))
		return err
	})
	if err != nil {
		return nil, err
	}

	return &MetadataStore{db: db}, nil
}

// Close はデータベース接続を閉じます。
func (s *MetadataStore) Close() error {
	return s.db.Close()
}

// Put は指定されたオブジェクトキーでメタデータを保存します。
func (s *MetadataStore) Put(objectKey string, meta ObjectMetadata) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))

		// メタデータをJSONにシリアライズします。
		buf, err := json.Marshal(meta)
		if err != nil {
			return err
		}

		// データを保存します。
		return b.Put([]byte(objectKey), buf)
	})
}

// Get は指定されたオブジェクトキーに対応するメタデータを取得します。
func (s *MetadataStore) Get(objectKey string) (*ObjectMetadata, error) {
	var meta ObjectMetadata

	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		val := b.Get([]byte(objectKey))
		if val == nil {
			// キーが存在しない場合はエラーではなく、nilを返す仕様も考えられるが、
			// ここでは明確にエラーとして扱う。
			return bbolt.ErrBucketNotFound // より適切なエラー型を検討する余地あり
		}

		// JSONからメタデータをデシリアライズします。
		return json.Unmarshal(val, &meta)
	})

	if err != nil {
		return nil, err
	}

	return &meta, nil
}