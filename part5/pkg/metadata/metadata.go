package metadata

import (
	"encoding/binary"
	"fmt"
	"os"
)

type ObjectInfo struct {
	Version      [6]byte
	FileNameSize uint32
	FileSize     uint64
	Created      int64
	UpDated      int64
}

func CreateMetaFile(metaFile, inputFile, outputDir string) error {
	// 入力ファイルの情報を取得
	fileInfo, err := os.Stat(inputFile)
	if err != nil {
		return fmt.Errorf("failed to get input file info: %w", err)
	}

	// メタデータを構築
	info := ObjectInfo{
		Version:      [6]byte{'v', '1', '.', '0', '.', '0'}, // バージョン情報
		FileNameSize: uint32(len(inputFile)),                // ファイル名の長さ
		FileSize:     uint64(fileInfo.Size()),               // ファイルサイズ
		Created:      fileInfo.ModTime().Unix(),             // 作成日時（ファイルの最終更新日時を使用）
		UpDated:      fileInfo.ModTime().Unix(),             // 更新日時（同じく最終更新日時を使用）
	}

	// メタデータファイルを作成
	file, err := os.Create(metaFile)
	if err != nil {
		return fmt.Errorf("failed to create meta file: %w", err)
	}
	defer file.Close()

	// メタデータを書き込む
	err = binary.Write(file, binary.BigEndian, &info)
	if err != nil {
		return fmt.Errorf("failed to write meta file: %w", err)
	}

	return nil
}

func ReadMetaFile(metaFile string) (*ObjectInfo, error) {
	// メタデータファイルを開く
	file, err := os.Open(metaFile)
	if err != nil {
		return nil, fmt.Errorf("failed to open meta file: %w", err)
	}
	defer file.Close()

	// メタデータを読み込む
	var info ObjectInfo
	err = binary.Read(file, binary.BigEndian, &info)
	if err != nil {
		return nil, fmt.Errorf("failed to read meta file: %w", err)
	}

	return &info, nil
}

func WriteMetaFile(metaFile string, info *ObjectInfo) error {
	// メタデータファイルを作成または開く
	file, err := os.Create(metaFile)
	if err != nil {
		return fmt.Errorf("failed to create meta file: %w", err)
	}
	defer file.Close()

	// メタデータを書き込む
	err = binary.Write(file, binary.BigEndian, info)
	if err != nil {
		return fmt.Errorf("failed to write meta file: %w", err)
	}

	return nil
}
