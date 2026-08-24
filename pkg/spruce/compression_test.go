package spruce

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"testing"
)

func TestCompressionRoundTripAndAdaptiveFallback(t *testing.T) {
	compressible := bytes.Repeat([]byte(`{"event":"workspace.updated","value":"aaaaaaaaaaaaaaaa"}`), 4096)
	for _, algorithm := range []string{CompressionGzip, CompressionZstd} {
		encoded, err := compressPayload(compressible, algorithm)
		if err != nil {
			t.Fatal(err)
		}
		if len(encoded) >= len(compressible)/2 {
			t.Fatalf("%s did not materially compress: %d >= %d", algorithm, len(encoded), len(compressible)/2)
		}
		decoded, err := decompressPayload(encoded, len(compressible))
		if err != nil || !bytes.Equal(decoded, compressible) {
			t.Fatalf("%s round trip failed: %v", algorithm, err)
		}
	}
	randomish := make([]byte, 4096)
	if _, err := rand.Read(randomish); err != nil {
		t.Fatal(err)
	}
	encoded, err := compressPayload(randomish, CompressionGzip)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(encoded, randomish) {
		t.Fatal("incompressible payload was expanded")
	}
}

func TestCompressionRejectsDecompressedLimit(t *testing.T) {
	payload := bytes.Repeat([]byte("x"), 4096)
	encoded, err := compressPayload(payload, CompressionGzip)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = decompressPayload(encoded, 1024); err == nil {
		t.Fatal("decompressed limit was not enforced")
	}
}

func BenchmarkCompression(b *testing.B) {
	for _, size := range []int{4096, 64 << 10, 900 << 10} {
		pattern := []byte(`{"event":"workspace.updated","status":"ready"}`)
		compressible := bytes.Repeat(pattern, size/len(pattern)+1)[:size]
		randomish := make([]byte, size)
		if _, err := rand.Read(randomish); err != nil {
			b.Fatal(err)
		}
		for _, test := range []struct {
			name, algorithm string
			payload         []byte
		}{{"gzip/compressible", CompressionGzip, compressible}, {"zstd/compressible", CompressionZstd, compressible}, {"gzip/random", CompressionGzip, randomish}, {"zstd/random", CompressionZstd, randomish}} {
			b.Run(test.name+"/"+fmt.Sprint(size), func(b *testing.B) {
				b.SetBytes(int64(size))
				for b.Loop() {
					if _, err := compressPayload(test.payload, test.algorithm); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
