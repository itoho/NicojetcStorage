package encoder

import (
	"fmt"
	"os"

	"github.com/cheggaaa/pb/v3"
)

func SplitAndEncode(inputFile, outputDir string) error {
	// ファイルを開く
	input, err := os.Open(inputFile)
	if err != nil {
		return fmt.Errorf("failed to open input file: %w", err)
	}
	defer input.Close()

	// ファイルサイズを取得
	fileInfo, err := input.Stat()
	if err != nil {
		return fmt.Errorf("failed to get file info: %w", err)
	}
	fileSize := fileInfo.Size()

	// 進捗バーの初期化
	bar := pb.Simple.Start64(fileSize)
	defer bar.Finish()

	// ファイル分割とエンコード処理
	// ...（現在のコードをここに移動）...

	return nil
}
