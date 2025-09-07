package encoder

import (
	"io"

	"github.com/klauspost/reedsolomon"
)

const (
	// データシャードの数
	DataShards = 10
	// パリティシャード（冗長）の数
	ParityShards = 4
)

// Encoder はデータのエンコードを担当します
type Encoder struct {
	enc reedsolomon.Encoder
}

// New は新しいEncoderを初期化して返します
func New() (Encoder, error) {
	enc, err := reedsolomon.New(DataShards, ParityShards)
	if err != nil {
		return Encoder{}, err
	}
	return Encoder{enc: enc}, nil
}

// Encode は reader から読み込んだデータをエンコードし、シャードのスライスとして返します
func (e *Encoder) Encode(r io.Reader) ([][]byte, error) {
	// リーダーから全てのデータを読み込む
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}

	// データをシャードに分割する
	shards, err := e.enc.Split(data)
	if err != nil {
		return nil, err
	}

	// パリティシャードを計算する
	err = e.enc.Encode(shards)
	if err != nil {
		return nil, err
	}

	return shards, nil
}
