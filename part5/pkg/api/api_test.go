package api

import (
	"bytes"
	"fmt"
	
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/itoho/NicojectStorage/part5/internal/decoder"
	"github.com/itoho/NicojectStorage/part5/internal/encoder"
	"github.com/itoho/NicojectStorage/part5/pkg/metadata"
	"github.com/itoho/NicojectStorage/part5/pkg/storage"
)

// setupTestAPIHandler はテスト用のAPIHandlerと関連コンポーネントを初期化します。
func setupTestAPIHandler(t *testing.T) (*APIHandler, func()) {
	t.Helper()

	// メタデータストアのセットアップ
	dbPath := filepath.Join(t.TempDir(), "test-meta.db")
	metaStore, err := metadata.NewStore(dbPath)
	if err != nil {
		t.Fatalf("Failed to create metadata store: %v", err)
	}

	// ストレージマネージャーのセットアップ
	storageDir1 := t.TempDir()
	storageDir2 := t.TempDir()
	storageDirs := []string{storageDir1, storageDir2}
	storageManager, err := storage.NewStorageManager(storageDirs)
	if err != nil {
		t.Fatalf("Failed to create storage manager: %v", err)
	}

	// エンコーダーとデコーダーの初期化
	enc, err := encoder.New()
	if err != nil {
		t.Fatalf("Failed to create encoder: %v", err)
	}
	dec, err := decoder.New()
	if err != nil {
		t.Fatalf("Failed to create decoder: %v", err)
	}

	apiHandler := NewAPIHandler(&enc, &dec, metaStore, storageManager)

	// クリーンアップ関数
	cleanup := func() {
		metaStore.Close()
		// TempDirはテスト終了時に自動でクリーンアップされるため、ストレージディレクトリの削除は不要
	}

	return apiHandler, cleanup
}

func TestPutObjectHandler(t *testing.T) {
	apiHandler, cleanup := setupTestAPIHandler(t)
	defer cleanup()

	router := chi.NewRouter()
	router.Put("/{bucket}/{objectKey}", apiHandler.PutObjectHandler)

	// テストデータ
	bucket := "test-bucket"
	objectKey := "test-object"
	originalData := "Hello, world! This is a test object."

	req := httptest.NewRequest("PUT", fmt.Sprintf("/%s/%s", bucket, objectKey), strings.NewReader(originalData))
	rr := httptest.NewRecorder()

	router.ServeHTTP(rr, req)

	// レスポンスの検証
	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v", status, http.StatusOK)
	}

	expected := fmt.Sprintf("Object '%s' in bucket '%s' uploaded successfully.", objectKey, bucket)
	if !strings.Contains(rr.Body.String(), expected) {
		t.Errorf("handler returned unexpected body: got %v want %v", rr.Body.String(), expected)
	}

	// メタデータが正しく保存されたか検証
	meta, err := apiHandler.metadataStore.Get(objectKey)
	if err != nil {
		t.Fatalf("Failed to get metadata after Put: %v", err)
	}
	if meta.Size != int64(len(originalData)) {
		t.Errorf("Metadata size mismatch: got %d want %d", meta.Size, len(originalData))
	}
	if len(meta.ShardPaths) == 0 {
		t.Errorf("No shard paths recorded in metadata")
	}
}

func TestGetObjectHandler(t *testing.T) {
	apiHandler, cleanup := setupTestAPIHandler(t)
	defer cleanup()

	router := chi.NewRouter()
	router.Put("/{bucket}/{objectKey}", apiHandler.PutObjectHandler)
	router.Get("/{bucket}/{objectKey}", apiHandler.GetObjectHandler)

	// データを事前にPUTする
	bucket := "test-bucket"
	objectKey := "test-object-get"
	originalData := "This is the data to be retrieved."

	putReq := httptest.NewRequest("PUT", fmt.Sprintf("/%s/%s", bucket, objectKey), strings.NewReader(originalData))
	putRr := httptest.NewRecorder()
	router.ServeHTTP(putRr, putReq)

	if putRr.Code != http.StatusOK {
		t.Fatalf("Failed to put object for Get test: status %v, body %v", putRr.Code, putRr.Body.String())
	}

	// GETリクエストを送信
	getReq := httptest.NewRequest("GET", fmt.Sprintf("/%s/%s", bucket, objectKey), nil)
	getRr := httptest.NewRecorder()
	router.ServeHTTP(getRr, getReq)

	// レスポンスの検証
	if status := getRr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v", status, http.StatusOK)
	}

	if !bytes.Equal([]byte(originalData), getRr.Body.Bytes()) {
		t.Errorf("handler returned unexpected body: got %v want %v", getRr.Body.String(), originalData)
	}
}

func TestGetObjectHandler_WithMissingShard(t *testing.T) {
	apiHandler, cleanup := setupTestAPIHandler(t)
	defer cleanup()

	router := chi.NewRouter()
	router.Put("/{bucket}/{objectKey}", apiHandler.PutObjectHandler)
	router.Get("/{bucket}/{objectKey}", apiHandler.GetObjectHandler)

	// データを事前にPUTする
	bucket := "test-bucket"
	objectKey := "test-object-missing-shard"
	originalData := strings.Repeat("a", 1000) // 複数シャードになるように十分な長さ

	putReq := httptest.NewRequest("PUT", fmt.Sprintf("/%s/%s", bucket, objectKey), strings.NewReader(originalData))
	putRr := httptest.NewRecorder()
	router.ServeHTTP(putRr, putReq)

	if putRr.Code != http.StatusOK {
		t.Fatalf("Failed to put object for missing shard test: status %v, body %v", putRr.Code, putRr.Body.String())
	}

	// 保存されたメタデータを取得し、シャードファイルを一つ削除
	meta, err := apiHandler.metadataStore.Get(objectKey)
	if err != nil {
		t.Fatalf("Failed to get metadata for missing shard test: %v", err)
	}

	if len(meta.ShardPaths) > 0 && meta.ShardPaths[0] != "" {
		// 最初のシャードファイルを削除
		if err := os.Remove(meta.ShardPaths[0]); err != nil {
			t.Fatalf("Failed to remove shard file for test: %v", err)
		}
	} else {
		t.Skip("Not enough shards to test missing shard scenario")
	}

	// GETリクエストを送信
	getReq := httptest.NewRequest("GET", fmt.Sprintf("/%s/%s", bucket, objectKey), nil)
	getRr := httptest.NewRecorder()
	router.ServeHTTP(getRr, getReq)

	// レスポンスの検証
	if status := getRr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v", status, http.StatusOK)
	}

	if !bytes.Equal([]byte(originalData), getRr.Body.Bytes()) {
		t.Errorf("handler returned unexpected body with missing shard: got %v want %v", getRr.Body.String(), originalData)
	}
}
