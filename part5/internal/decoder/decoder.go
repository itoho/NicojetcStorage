package decoder

import (
	"io"

	"github.com/klauspost/reedsolomon"
)

const (
	// DataShards はデータシャードの数を定義します。encoderパッケージの値と一致させる必要があります。
	DataShards = 10
	// ParityShards はパリティシャードの数を定義します。encoderパッケージの値と一致させる必要があります。
	ParityShards = 4
)

// Decoder はデータのデコードを担当します。
type Decoder struct {
	enc reedsolomon.Encoder
}

// New は新しいDecoderを初期化して返します。
func New() (Decoder, error) {
	enc, err := reedsolomon.New(DataShards, ParityShards)
	if err != nil {
		return Decoder{}, err
	}
	return Decoder{enc: enc}, nil
}

// Decode はシャードのスライスを受け取り、欠損しているシャードを復元した後、
// 元のデータを再構築して writer に書き込みます。
// objSizeは元のオブジェクトのサイズです。
func (d *Decoder) Decode(shards [][]byte, objSize int, w io.Writer) error {
	// 欠損シャードを再構築します。
	err := d.enc.Reconstruct(shards)
	if err != nil {
		return err
	}

	// シャードを結合して元のデータをwriterに書き出します。
	err = d.enc.Join(w, shards, objSize)
	if err != nil {
		return err
	}

	return nil
}
