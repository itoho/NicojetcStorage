// リード・ソロモン符号を用いて、バイナリファイルを分割し、復元します

package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"time"

	"github.com/cheggaaa/pb/v3"
	"github.com/klauspost/reedsolomon"
	"github.com/minio/highwayhash"
)

//6MBのデータを6つのデータシャードと2つのパリティシャードに分割する
const (
	HashSize  = 32 //ハッシュサイズ 256bit
	DataShard = 6  //リード・ソロモン符号のパラメータ
	Parity    = 2  //リード・ソロモン符号のパラメータ
)

var ErasureSetNum = DataShard + Parity //リード・ソロモン符号のパラメータ

type Header struct {
	FileSize  uint32 //ブロックのファイルサイズ
	I_Counter uint32 //Iのカウンタ. 0から始まる.0から2^64までいける
	J_Counter uint32 //Jのカウンタ. 0から始まる.0からErasureSetNumまでいける
}

type Footer struct {
	HashType uint8    //ハッシュの種類, 0:HighwayHash256
	Hash     [32]byte //ブロックのハッシュ値
}

type ObjectInfo struct {
	Version      [6]byte //設定ファイルのバージョン情報
	FileNameSize uint32  //ファイル名のサイズ
	FileSize     uint64  //ファイルサイズ
	Created      int64   // Unix timestamp
	UpDated      int64   // Unix timestamp
	//可変長のファイル名
}

func encode(dataShards [][]byte) ([][]byte, error) {
	enc, err := reedsolomon.New(DataShard, Parity)
	if err != nil {
		return nil, err
	}

	err = enc.Encode(dataShards)
	if err != nil {
		return nil, err
	}

	parityShards := dataShards[DataShard:]
	return parityShards, nil
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
	for i := int64(0); i < fileSize; i += 1024 * 1024 * DataShard {

		//残りのサイズを計算(DataShard * 1 MB未満の場合は残りのサイズを読み込む)
		restSize := fileSize - i
		if restSize > 1024*1024*DataShard {
			restSize = 1024 * 1024 * DataShard
		}

		// DataShard * 1 MB読み込んで変数に保持
		buf := make([]byte, restSize)
		_, err = input.Read(buf)
		if err != nil {
			fmt.Println(err)
			return
		}

		//restSizeがDataShard * 1 MB未満の場合は、残りのデータを0で埋める
		if restSize < 1024*1024*DataShard {
			newBuf := make([]byte, 1024*1024*DataShard)
			copy(newBuf, buf)
			buf = newBuf
		}

		fmt.Println("restSize: ", restSize)

		// リード・ソロモン符号を用いて、バイナリファイルを分割
		// データシャードとパリティシャードに分割
		dataShards := make([][]byte, ErasureSetNum)
		for j := 0; j < DataShard; j++ {
			dataShards[j] = buf[j*1024*1024 : (j+1)*1024*1024]
		}
		//パリティシャードのために1MB分の空データを追加
		for j := DataShard; j < ErasureSetNum; j++ {
			dataShards[j] = make([]byte, 1024*1024)
		}

		parityShards, err := encode(dataShards)
		if err != nil {
			fmt.Println(err)
			return
		}
		for j := 0; j < ErasureSetNum; j++ {
			// outputfileを作成

			outputfile := fmt.Sprintf("%s%d_%d", outputdir, i/(1024*1024*DataShard), j)
			output, err := os.Create(outputfile)
			if err != nil {
				fmt.Println(err)
				return
			}
			// 関数が終了する際にoutputを閉じる
			defer output.Close()

			//ヘッダーを作成
			header := Header{
				FileSize:  uint32(restSize),
				I_Counter: uint32(i / (1024 * 1024)),
				J_Counter: uint32(j),
			}
			if j < DataShard {
				//ヘッダーを書き込む
				err = binary.Write(output, binary.BigEndian, header)
				if err != nil {
					fmt.Println(err)
					return
				}

				//バイナリデータを書き込む
				_, err = output.Write(dataShards[j])
				if err != nil {
					fmt.Println(err)
					return
				}

				//ハッシュ値を計算
				key := make([]byte, 32) // Use a proper key for HighwayHash
				highwayHash, err := highwayhash.New(key)
				if err != nil {
					fmt.Println(err)
					return
				}
				highwayHash.Write(dataShards[j])
				hash := highwayHash.Sum(nil)
				//64バイトであることを確認
				if len(hash) != HashSize {
					fmt.Println("hash size is invalid")
					return
				}

				//フッターを作成
				footer := Footer{
					HashType: 0,
					Hash:     *(*[32]byte)(hash),
				}

				//フッターを書き込む
				err = binary.Write(output, binary.BigEndian, footer)
				if err != nil {
					fmt.Println(err)
					return
				}
				//分ける意味はないが後で使うかもしれない
			} else {
				//ヘッダーを書き込む
				err = binary.Write(output, binary.BigEndian, header)
				if err != nil {
					fmt.Println(err)
					return
				}

				//バイナリデータを書き込む
				_, err = output.Write(parityShards[j-DataShard])
				if err != nil {
					fmt.Println(err)
					return
				}

				//ハッシュ値を計算
				key := make([]byte, 32) // Use a proper key for HighwayHash
				highwayHash, err := highwayhash.New(key)
				if err != nil {
					fmt.Println(err)
					return
				}
				highwayHash.Write(parityShards[j-DataShard])
				hash := highwayHash.Sum(nil)
				//64バイトであることを確認
				if len(hash) != HashSize {
					fmt.Println("hash size is invalid")
					return
				}

				//フッターを作成
				footer := Footer{
					HashType: 0,
					Hash:     *(*[32]byte)(hash),
				}

				//フッターを書き込む
				err = binary.Write(output, binary.BigEndian, footer)
				if err != nil {
					fmt.Println(err)
					return
				}
			}
		}

		bar.Add64(restSize)

	}
	//メタデータファイルを作成
	metafilename := "binary/meta"
	metafile, err := os.Create(metafilename)
	if err != nil {
		fmt.Println(err)
		return
	}
	// 関数が終了する際にmetafileを閉じる
	defer metafile.Close()
	outputfilename := "output2.mp4"
	//ObjectInfoを作成
	objectInfo := ObjectInfo{
		Version:      [6]byte{'v', '0', '.', '0', '.', '0'},
		FileNameSize: uint32(len(outputfilename)),
		FileSize:     uint64(fileSize),
		Created:      time.Now().Unix(),
	}

	//ObjectInfoを書き込む
	err = binary.Write(metafile, binary.BigEndian, objectInfo)
	if err != nil {
		fmt.Println(err)
		return
	}

	//ファイル名を書き込む
	_, err = metafile.Write([]byte(outputfilename))
	if err != nil {
		fmt.Println(err)
		return
	}

	bar.Finish()
	fmt.Println("分割完了")
	fmt.Println("エンターキーを押して復元します")
	//キー入力待機
	fmt.Scanln()
	bar = pb.Simple.Start(0)
	bar.SetTotal(fileSize)
	//バイナリファイルを読み込んで、元のファイルを復元するコード

	//ObjectInfoを読み込む
	metafile, err = os.Open(metafilename)
	if err != nil {
		fmt.Println(err)
		return
	}
	// 関数が終了する際にmetafileを閉じる
	defer metafile.Close()

	//ObjectInfoを読み込む
	var objectInformation ObjectInfo
	err = binary.Read(metafile, binary.BigEndian, &objectInformation)
	if err != nil {
		fmt.Println(err)
		return
	}
	extractFileSize := objectInformation.FileSize

	//ファイル名を読み込む
	buf := make([]byte, objectInformation.FileNameSize)
	_, err = metafile.Read(buf)
	if err != nil {
		fmt.Println(err)
		return
	}

	//ファイル名を取得
	outputfilename = string(buf)
	fmt.Println("outputfilename: ", outputfilename)

	//0から始まるIのカウンタを保持する変数
	iCounter := 0

	//復元したファイルを書き込むためのファイルを作成
	outputfile := outputdir + outputfilename
	output, err := os.Create(outputfile)
	if err != nil {
		fmt.Println(err)
		return
	}
	// 関数が終了する際にoutputを閉じる
	defer output.Close()

	//binary/fragments/以下のファイルを読み込む
	for {
		binaries := make([][]byte, ErasureSetNum)
		fragmentsize := -1 //ヘッダーのファイルサイズを保持する変数
		for j := 0; j < ErasureSetNum; j++ {
			// inputfileを読み込む
			inputfile := fmt.Sprintf("binary/fragments/%d_%d", iCounter, j)
			input, err := os.Open(inputfile)
			if err != nil {
				fmt.Println(err, "inputfile: ", inputfile)
				binaries[j] = nil
				continue
			}
			// 関数が終了する際にinputを閉じる
			defer input.Close()

			// ヘッダーを読み込む
			header := Header{}
			err = binary.Read(input, binary.BigEndian, &header)
			fragmentsize = int(header.FileSize)
			if err != nil {
				fmt.Println(err, "ヘッダーを読み込む")
				return
			}

			// バイナリデータを読み込む
			buf := make([]byte, 1024*1024) //1MB固定
			_, err = input.Read(buf)
			if err != nil {
				fmt.Println(err, "バイナリデータを読み込む")
				binaries[j] = nil
				continue
			}

			// フッターを読み込む
			footer := Footer{}
			err = binary.Read(input, binary.BigEndian, &footer)
			if err != nil {
				fmt.Println(err, "フッターを読み込む")
				binaries[j] = nil
				continue
			}

			// ハッシュ値を計算
			key := make([]byte, 32) // Use a proper key for HighwayHash
			highwayHash, err := highwayhash.New(key)
			if err != nil {
				fmt.Println(err, "ハッシュ値を計算")
				binaries[j] = nil
				continue
			}
			highwayHash.Write(buf)
			hash := highwayHash.Sum(nil)
			//64バイトであることを確認
			if len(hash) != HashSize {
				fmt.Println("hash size is invalid")
				binaries[j] = nil
				continue
			}

			// ハッシュ値が一致するか確認
			if footer.Hash != *(*[32]byte)(hash) {
				fmt.Println("hash is invalid")
				binaries[j] = nil
				continue
			}
			//バイナリ配列に書き込み
			binaries[j] = buf
		}
		// リード・ソロモン符号を用いて、バイナリファイルを復元
		enc, err := reedsolomon.New(DataShard, Parity)
		if err != nil {
			fmt.Println(err)
			return
		}
		err = enc.Reconstruct(binaries)
		if err != nil {
			fmt.Println(err, iCounter)
			fmt.Println("復元失敗")
			return
		}

		//復元結果をoutputに書き込む fragmentSizeだけ書き込むためにまずdataShardsを合計する
		dataShards := make([]byte, 0)
		for j := 0; j < DataShard; j++ {
			dataShards = append(dataShards, binaries[j]...)
		}
		_, err = output.Write(dataShards[:fragmentsize])
		if err != nil {
			fmt.Println(err)
			return
		}

		iCounter++
		bar.Add64(1024 * 1024 * DataShard)

		extractFileSize -= uint64(fragmentsize)
		if extractFileSize <= 0 {
			bar.SetCurrent(int64(objectInformation.FileSize))
			break
		}

	}
	bar.Finish()
	fmt.Println("復元完了")
}
