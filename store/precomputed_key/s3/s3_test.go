package s3

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRemoveChunkSignature(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		expected []byte
	}{
		{
			name:     "Data with chunk signature",
			input:    []byte("chunk1;chunk-signature=abcdef123456\r\nchunk2\r\n"),
			expected: []byte("chunk2"),
		},
		{
			name:     "Data without chunk signature",
			input:    []byte("chunk1\r\nchunk2\r\n"),
			expected: []byte("chunk1\r\nchunk2"),
		},
		{
			name:     "Data with multiple chunk signatures",
			input:    []byte("chunk1;chunk-signature=abcdef123456\r\nchunk2;chunk-signature=123456abcdef\r\nchunk3\r\n"),
			expected: []byte("chunk3"),
		},
		{
			name:     "Data with no chunk signature but with \\r\\n sequences",
			input:    []byte("\r\n\r\nchunk1\r\nchunk2\r\n\r\n"),
			expected: []byte("chunk1\r\nchunk2"),
		},
		{
			name:     "Empty data",
			input:    []byte(""),
			expected: []byte(""),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := removeChunkSignature(tt.input)
			require.NoError(t, err)
			require.Equal(t, tt.expected, result)
		})
	}
}
