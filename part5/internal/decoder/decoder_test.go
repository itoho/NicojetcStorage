package decoder

import (
	"bytes"
	
	"testing"

	"github.com/itoho/NicojectStorage/part5/internal/encoder"
)

// setupEncoder はテスト用のエンコーダを初期化します。
func setupEncoder(t *testing.T) encoder.Encoder {
	t.Helper()
	enc, err := encoder.New()
	if err != nil {
		t.Fatalf("Failed to create encoder for test: %v", err)
	}
	return enc
}

func TestDecoder_Decode(t *testing.T) {
	enc := setupEncoder(t)
	dec, err := New()
	if err != nil {
		t.Fatalf("Failed to create decoder: %v", err)
	}

	originalData := []byte("The quick brown fox jumps over the lazy dog")
	objSize := len(originalData)

	shards, err := enc.Encode(bytes.NewReader(originalData))
	if err != nil {
		t.Fatalf("Failed to encode data for test: %v", err)
	}

	tests := []struct {
		name          string
		shardModifier func([][]byte) // シャードをテスト用に変更する関数
		wantErr       bool
	}{
		{
			name:          "Normal case - no missing shards",
			shardModifier: func(s [][]byte) {},
			wantErr:       false,
		},
		{
			name: "Recoverable case - 2 missing data shards",
			shardModifier: func(s [][]byte) {
				s[0] = nil
				s[5] = nil
			},
			wantErr: false,
		},
		{
			name: "Recoverable case - 4 missing parity shards",
			shardModifier: func(s [][]byte) {
				for i := 0; i < ParityShards; i++ {
					s[DataShards+i] = nil
				}
			},
			wantErr: false,
		},
		{
			name: "Unrecoverable case - 5 missing shards",
			shardModifier: func(s [][]byte) {
				for i := 0; i < ParityShards+1; i++ {
					s[i] = nil
				}
			},
			wantErr: true,
		},
		
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 各テストごとにシャードをコピーして、元のシャードが変更されないようにする
			testShards := make([][]byte, len(shards))
			for i := range shards {
				if shards[i] != nil {
					testShards[i] = make([]byte, len(shards[i]))
					copy(testShards[i], shards[i])
				}
			}

			tt.shardModifier(testShards)

			var buf bytes.Buffer
			err := dec.Decode(testShards, objSize, &buf)

			if (err != nil) != tt.wantErr {
				t.Errorf("Decode() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				if !bytes.Equal(originalData, buf.Bytes()) {
					t.Errorf("Decode() got = %s, want = %s", buf.String(), string(originalData))
				}
			}
		})
	}
}
