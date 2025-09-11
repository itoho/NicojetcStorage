package main

import (
	"context"

	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/itoho/NicojectStorage/part5/internal/decoder"
	"github.com/itoho/NicojectStorage/part5/internal/encoder"
	"github.com/itoho/NicojectStorage/part5/pkg/api"
	"github.com/itoho/NicojectStorage/part5/pkg/metadata"
	"github.com/itoho/NicojectStorage/part5/pkg/storage"
)

const (
	// サーバーがリッスンするポート
	serverPort = ":8080"
	// メタデータDBのパス
	metaDBPath = "./data/meta.db"
	// シャード保存用ディレクトリ
	shardDir1 = "./data/shards1"
	shardDir2 = "./data/shards2"
	shardDir3 = "./data/shards3"
)

func main() {
	// 必要なデータディレクトリを作成
	dataDirs := []string{filepath.Dir(metaDBPath), shardDir1, shardDir2, shardDir3}
	for _, dir := range dataDirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			log.Fatalf("Failed to create data directory %s: %v", dir, err)
		}
	}

	// コンポーネントの初期化
	enc, err := encoder.New()
	if err != nil {
		log.Fatalf("Failed to create encoder: %v", err)
	}
	dec, err := decoder.New()
	if err != nil {
		log.Fatalf("Failed to create decoder: %v", err)
	}

	metaStore, err := metadata.NewStore(metaDBPath)
	if err != nil {
		log.Fatalf("Failed to create metadata store: %v", err)
	}
	defer func() {
		if err := metaStore.Close(); err != nil {
			log.Printf("Error closing metadata store: %v", err)
		}
	}()

	storageManager, err := storage.NewStorageManager([]string{shardDir1, shardDir2, shardDir3})
	if err != nil {
		log.Fatalf("Failed to create storage manager: %v", err)
	}

	apiHandler := api.NewAPIHandler(&enc, &dec, metaStore, storageManager)

	// ルーターの設定
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	// ヘルスチェックエンドポイント
	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	// S3互換APIエンドポイント
	// PUT /{bucket}/{objectKey} でオブジェクトをアップロード
	r.Put("/{bucket}/{objectKey}", apiHandler.PutObjectHandler)
	// GET /{bucket}/{objectKey} でオブジェクトをダウンロード
	r.Get("/{bucket}/{objectKey}", apiHandler.GetObjectHandler)

	// Graceful Shutdown の設定
	server := &http.Server{
		Addr:    serverPort,
		Handler: r,
	}

	// 終了シグナルを監視するためのチャネル
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	// サーバーを別のゴルーチンで起動
	go func() {
		log.Printf("Server starting on port %s", serverPort)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Failed to start server: %v", err)
		}
	}()

	// 終了シグナルを待機
	<-stop

	log.Println("Shutting down server...")

	// タイムアウト付きでサーバーをシャットダウン
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Fatalf("Server shutdown failed: %v", err)
	}

	log.Println("Server gracefully stopped.")
}
