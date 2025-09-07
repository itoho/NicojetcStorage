package encoder

import (
	"bytes"
	"strings"
	"testing"
)

func TestNewEncoder(t *testing.T) {
	_, err := New()
	if err != nil {
		t.Fatalf("New() aailed, expected no error, got %v", err)
	}
}

func TestEncoder_Encode(t *testing.T) {
	encoder, err := New()
	if err != nil {
		t.Fatalf("Failed to create encoder: %v", err)
	}

	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{
			name:    "Simple string",
			input:   "hello world",
			wantErr: false,
		},
		{
			name:    "Empty string",
			input:   "",
			wantErr: true,
		},
		{
			name:    "Longer string data",
			input:   strings.Repeat("a", 10000),
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := bytes.NewReader([]byte(tt.input))
			shards, err := encoder.Encode(r)

			if (err != nil) != tt.wantErr {
				t.Errorf("Encode() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				expectedShards := DataShards + ParityShards
				if len(shards) != expectedShards {
					t.Errorf("Encode() got %v shards, want %v", len(shards), expectedShards)
				}

				for i, shard := range shards {
					if len(shard) == 0 && len(tt.input) > 0 {
						t.Errorf("Shard %d is empty, which is unexpected for non-empty input", i)
					}
				}
			}
		})
	}
}
