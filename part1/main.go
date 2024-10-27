package main

import (
	"fmt"
	"io"
	"os"
)

func main() {
	fmt.Println("Hello World!")
	// binary/inputをバイナリファイルとして読み込み、4つに分割してbinary/output_%dに書き込むコード
	inputfile := "binary/input.mp4"
	outputdir := "binary/"

	// inputfileを読み込む
	input, err := os.Open(inputfile)
	if err != nil {
		fmt.Println(err)
		return
	}
	// 関数が終了する際にinputを閉じる
	defer input.Close()

	// inputfileのサイズを取得
	fileInfo, err := input.Stat()
	if err != nil {
		fmt.Println(err)
		return
	}

	fileSize := fileInfo.Size()
	// ファイルサイズを4で割り、4つのファイルに分割する. 4で割り切れなかった余りは最後のファイルに含める
	partSize := fileSize / 4
	remainder := fileSize % 4

	for i := 0; i < 4; i++ {
		// output_%dを作成
		outputFile, err := os.Create(fmt.Sprintf("%soutput_%d", outputdir, i))
		if err != nil {
			fmt.Println(err)
			return
		}
		// 関数が終了する際にoutput_%dを閉じる
		defer outputFile.Close()

		// inputfileからoutput_%dにコピー
		_, err = input.Seek(int64(i)*partSize, 0)
		if err != nil {
			fmt.Println(err)
			return
		}
		// 4つ目のファイルは余りを含む。実際は4や3のような具体的な数字(マジックナンバー)ではなく、変数を使って管理する
		sizeToCopy := partSize
		if i == 3 {
			sizeToCopy += remainder
		}

		// inputからoutput_%dに分割して書き込み
		_, err = io.CopyN(outputFile, input, sizeToCopy)
		if err != nil && err != io.EOF {
			fmt.Println(err)
			return
		}
	}

	// output_0-4を読み込み、それらを結合してmergedを作成するコード
	mergedFile, err := os.Create(outputdir + "merged.mp4")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer mergedFile.Close()

	for i := 0; i < 4; i++ {
		partFile, err := os.Open(outputdir + "output_" + fmt.Sprintf("%d", i))
		if err != nil {
			fmt.Println(err)
			return
		}
		defer partFile.Close()

		_, err = io.Copy(mergedFile, partFile)
		if err != nil {
			fmt.Println(err)
			return
		}
	}
}
