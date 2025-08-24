package metadata

import (
	"os"
	"testing"
)

func TestCreateMetaFile(t *testing.T) {
	// テスト用の入力ファイルとメタデータファイルを準備
	inputFile := "test_input.txt"
	metaFile := "test_meta.dat"

	// テスト用の入力ファイルを作成
	err := os.WriteFile(inputFile, []byte("test data"), 0644)
	if err != nil {
		t.Fatalf("failed to create test input file: %v", err)
	}
	defer os.Remove(inputFile) // テスト後に削除

	// メタデータファイルを作成
	err = CreateMetaFile(metaFile, inputFile, "")
	if err != nil {
		t.Fatalf("CreateMetaFile failed: %v", err)
	}
	defer os.Remove(metaFile) // テスト後に削除

	// メタデータファイルを確認
	fileInfo, err := os.Stat(metaFile)
	if err != nil {
		t.Fatalf("failed to stat meta file: %v", err)
	}

	if fileInfo.Size() == 0 {
		t.Errorf("meta file is empty")
	}
}

func TestReadMetaFile(t *testing.T) {
	metaFile := "test_meta.dat"

	// テスト用のメタデータを作成
	info := &ObjectInfo{
		Version:      [6]byte{'v', '1', '.', '0', '.', '0'},
		FileNameSize: 10,
		FileSize:     12345,
		Created:      1617181920,
		UpDated:      1617181930,
	}

	// メタデータを書き込む
	err := WriteMetaFile(metaFile, info)
	if err != nil {
		t.Fatalf("WriteMetaFile failed: %v", err)
	}
	defer os.Remove(metaFile) // テスト後に削除

	// メタデータを読み込む
	readInfo, err := ReadMetaFile(metaFile)
	if err != nil {
		t.Fatalf("ReadMetaFile failed: %v", err)
	}

	// 読み込んだデータが正しいか確認
	if *readInfo != *info {
		t.Errorf("ReadMetaFile returned incorrect data: got %+v, want %+v", readInfo, info)
	}
}
