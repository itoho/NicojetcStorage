package api

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/itoho/NicojectStorage/part5/internal/decoder"
	"github.com/itoho/NicojectStorage/part5/internal/encoder"
	"github.com/itoho/NicojectStorage/part5/pkg/metadata"
	"github.com/itoho/NicojectStorage/part5/pkg/storage"
)

// APIHandler はAPIエンドポイントのハンドラを保持し、
// 必要なサービス（エンコーダ、デコーダ、メタデータストア、ストレージマネージャ）への参照を持ちます。
type APIHandler struct {
	encoder        *encoder.Encoder
	decoder        *decoder.Decoder
	metadataStore  *metadata.MetadataStore
	storageManager *storage.StorageManager
}

// NewAPIHandler はAPIHandlerの新しいインスタンスを作成します。
func NewAPIHandler(
	enc *encoder.Encoder,
	dec *decoder.Decoder,
	metaStore *metadata.MetadataStore,
	storageMgr *storage.StorageManager,
) *APIHandler {
	return &APIHandler{
		encoder:        enc,
		decoder:        dec,
		metadataStore:  metaStore,
		storageManager: storageMgr,
	}
}

// PutObjectHandler はS3互換のPUTオブジェクトAPIを処理します。
// リクエストボディを読み込み、エンコードし、シャードを保存し、メタデータを記録します。
func (h *APIHandler) PutObjectHandler(w http.ResponseWriter, r *http.Request) {
	bucket := chi.URLParam(r, "bucket")
	objectKey := chi.URLParam(r, "objectKey")

	if bucket == "" || objectKey == "" {
		http.Error(w, "Bucket or object key not provided", http.StatusBadRequest)
		return
	}

	// リクエストボディからデータを読み込む
	// io.ReadAllはメモリを大量に消費する可能性があるため、大きなファイルではストリーミング処理を検討すべき
	data, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to read request body: %v", err), http.StatusInternalServerError)
		return
	}

	// データをエンコード
	shards, err := h.encoder.Encode(bytes.NewReader(data))
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to encode data: %v", err), http.StatusInternalServerError)
		return
	}

	// 各シャードを保存
	shardPaths := make([]string, len(shards))
	for i, shardData := range shards {
		// reedsolomonは空のシャードを生成することがあるため、スキップ
		if len(shardData) == 0 {
			continue
		}
		// シャードIDはユニークなものを使用
		shardID := fmt.Sprintf("%s-%d-%s", objectKey, i, uuid.New().String())
		path, err := h.storageManager.Store(shardID, shardData)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to store shard %d: %v", i, err), http.StatusInternalServerError)
			return
		}
		shardPaths[i] = path
	}

	// メタデータを保存
	meta := metadata.ObjectMetadata{
		ObjectID:     objectKey,
		Size:         int64(len(data)),
		DataShards:   encoder.DataShards,
		ParityShards: encoder.ParityShards,
		ShardPaths:   shardPaths,
		CreatedAt:    time.Now().UTC(),
	}
	if err := h.metadataStore.Put(objectKey, meta); err != nil {
		http.Error(w, fmt.Sprintf("Failed to save metadata: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(fmt.Sprintf("Object '%s' in bucket '%s' uploaded successfully.", objectKey, bucket)))
}

// GetObjectHandler はS3互換のGETオブジェクトAPIを処理します。
// メタデータからシャードパスを取得し、シャードを読み込み、デコードして、
// 元のデータをレスポンスボディとして返します。
func (h *APIHandler) GetObjectHandler(w http.ResponseWriter, r *http.Request) {
	objectKey := chi.URLParam(r, "objectKey")

	if objectKey == "" {
		http.Error(w, "Object key not provided", http.StatusBadRequest)
		return
	}

	// メタデータを取得
	meta, err := h.metadataStore.Get(objectKey)
	if err != nil {
		if strings.Contains(err.Error(), "not found") { // BoltDBのエラーメッセージに依存
			http.Error(w, "Object not found", http.StatusNotFound)
		} else {
			http.Error(w, fmt.Sprintf("Failed to retrieve metadata: %v", err), http.StatusInternalServerError)
		}
		return
	}

	// シャードを読み込み
	retrievedShards := make([][]byte, len(meta.ShardPaths))
	for i, path := range meta.ShardPaths {
		if path == "" { // 空のパスはスキップ
			retrievedShards[i] = nil
			continue
		}
		data, err := h.storageManager.Retrieve(path)
		if err != nil {
			// ファイルが見つからない場合でも、nilとして処理を続行（reedsolomonが復元を試みる）
			retrievedShards[i] = nil
		} else {
			retrievedShards[i] = data
		}
	}

	// データをデコード
	var resultBuffer bytes.Buffer
	if err := h.decoder.Decode(retrievedShards, int(meta.Size), &resultBuffer); err != nil {
		http.Error(w, fmt.Sprintf("Failed to decode data: %v", err), http.StatusInternalServerError)
		return
	}

	// レスポンスとしてデータを返す
	w.Header().Set("Content-Type", "application/octet-stream") // バイナリデータとして設定
	w.Header().Set("Content-Length", strconv.FormatInt(meta.Size, 10))
	w.WriteHeader(http.StatusOK)
	_, err = w.Write(resultBuffer.Bytes())
	if err != nil {
		// 既にヘッダーが送信されているため、エラーログのみ
		fmt.Printf("Failed to write response: %v\n", err)
	}
}
