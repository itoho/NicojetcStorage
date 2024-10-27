package main

import (
	"encoding/binary"
	"fmt"
	"os"

	"github.com/cheggaaa/pb/v3"
)

type Header struct {
	FileSize  uint32 //ブロックのファイルサイズ
	I_Counter uint32 //Iのカウンタ. 0から始まる
}

func main() {
	fmt.Println("Hello World!")
	// binary/inputをバイナリファイルとして読み込み、1MBごとに分割してbinary/fragments/%dに書き込むコード
	//バイナリファイルの先頭には、ファイルサイズをint32で格納しておく(4GBまで対応可能なので1MBだと余裕なはず)
	inputfile := "binary/input.mp4"
	outputdir := "binary/fragments/"

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
	fmt.Println("fileSize: ", fileSize)

	bar := pb.Simple.Start(0)
	bar.SetTotal(fileSize)
	for i := int64(0); i < fileSize; i += 1024 * 1024 {
		// outputfileを作成
		outputfile := fmt.Sprintf("%s%d", outputdir, i/(1024*1024))
		output, err := os.Create(outputfile)
		if err != nil {
			fmt.Println(err)
			return
		}
		// 関数が終了する際にoutputを閉じる
		defer output.Close()

		//残りのサイズを計算(1MB未満の場合は残りのサイズを読み込む)
		restSize := fileSize - i
		if restSize > 1024*1024 {
			restSize = 1024 * 1024
		}

		// 1MB読み込んで変数に保持
		buf := make([]byte, restSize)
		_, err = input.Read(buf)
		if err != nil {
			fmt.Println(err)
			return
		}

		//ヘッダーを作成
		header := Header{
			FileSize:  uint32(restSize),
			I_Counter: uint32(i / (1024 * 1024)),
		}

		//ヘッダーを書き込む
		err = binary.Write(output, binary.BigEndian, header)
		if err != nil {
			fmt.Println(err)
			return
		}

		//バイナリデータを書き込む
		_, err = output.Write(buf)
		if err != nil {
			fmt.Println(err)
			return
		}
		bar.Add64(restSize)

	}
	bar.Finish()
	fmt.Println("分割完了")
	fmt.Println("復元します")
	bar = pb.Simple.Start(0)
	bar.SetTotal(fileSize)
	//バイナリファイルを読み込んで、元のファイルを復元するコード

	//0から始まるIのカウンタを保持する変数
	iCounter := 0

	//復元したファイルを書き込むためのファイルを作成
	outputfile := "binary/output.mp4"
	output, err := os.Create(outputfile)
	if err != nil {
		fmt.Println(err)
		return
	}
	// 関数が終了する際にoutputを閉じる
	defer output.Close()

	//binary/fragments/以下のファイルを読み込む
	for {
		inputfile := fmt.Sprintf("binary/fragments/%d", iCounter)
		input, err := os.Open(inputfile)
		if err != nil {
			break
		}
		// 関数が終了する際にinputを閉じる
		defer input.Close()

		//ヘッダーを読み込む
		var header Header
		err = binary.Read(input, binary.BigEndian, &header)
		if err != nil {
			fmt.Println(err)
			return
		}
		if header.I_Counter != uint32(iCounter) {
			fmt.Println("I_Counter is invalid")
			return
		}

		//バイナリデータを読み込む
		buf := make([]byte, header.FileSize)
		_, err = input.Read(buf)
		if err != nil {
			fmt.Println(err)
			return
		}

		//バイナリデータをoutputに書き込む
		_, err = output.Write(buf)
		if err != nil {
			fmt.Println(err)
			return
		}

		iCounter++
		bar.Add64(int64(header.FileSize))
	}
	bar.Finish()
	fmt.Println("復元正常完了")
}
