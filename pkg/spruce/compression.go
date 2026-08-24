package spruce

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/klauspost/compress/zstd"
)

var gzipWriters = sync.Pool{New: func() any {
	writer, _ := gzip.NewWriterLevel(io.Discard, gzip.BestSpeed)
	return writer
}}

var zstdEncoders = sync.Pool{New: func() any {
	writer, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithEncoderConcurrency(1))
	return writer
}}

const (
	CompressionOff  = ""
	CompressionGzip = "gzip"
	CompressionZstd = "zstd"

	compressionThreshold = 1024
	compressionMagic     = "\x89SPRUCE\x01"
)

func compressPayload(payload []byte, algorithm string) ([]byte, error) {
	if len(payload) > 1<<20 {
		return nil, errors.New("payload exceeds 1 MiB")
	}
	if algorithm == CompressionOff || len(payload) < compressionThreshold {
		return payload, nil
	}
	header := make([]byte, len(compressionMagic)+5)
	copy(header, compressionMagic)
	var size [4]byte
	binary.BigEndian.PutUint32(size[:], uint32(len(payload)))
	copy(header[len(compressionMagic)+1:], size[:])
	var encoded []byte
	switch algorithm {
	case CompressionGzip:
		header[len(compressionMagic)] = 1
		var compressed bytes.Buffer
		compressed.Grow(min(len(payload), 64<<10))
		compressed.Write(header)
		writer := gzipWriters.Get().(*gzip.Writer)
		writer.Reset(&compressed)
		if _, err := writer.Write(payload); err != nil {
			writer.Reset(io.Discard)
			gzipWriters.Put(writer)
			return nil, err
		}
		if err := writer.Close(); err != nil {
			writer.Reset(io.Discard)
			gzipWriters.Put(writer)
			return nil, err
		}
		writer.Reset(io.Discard)
		gzipWriters.Put(writer)
		encoded = compressed.Bytes()
	case CompressionZstd:
		header[len(compressionMagic)] = 2
		writer := zstdEncoders.Get().(*zstd.Encoder)
		encoded = writer.EncodeAll(payload, header)
		zstdEncoders.Put(writer)
	default:
		return nil, fmt.Errorf("unsupported compression %q", algorithm)
	}
	minimumSaving := max(128, len(payload)/10)
	if len(encoded) > len(payload)-minimumSaving {
		return payload, nil
	}
	return encoded, nil
}

func decompressPayload(payload []byte, maximum int) ([]byte, error) {
	if len(payload) < len(compressionMagic)+5 || string(payload[:len(compressionMagic)]) != compressionMagic {
		return payload, nil
	}
	original := int(binary.BigEndian.Uint32(payload[len(compressionMagic)+1 : len(compressionMagic)+5]))
	if original < 0 || original > maximum {
		return nil, errors.New("compressed payload exceeds decompressed limit")
	}
	encoded := bytes.NewReader(payload[len(compressionMagic)+5:])
	var reader io.ReadCloser
	switch payload[len(compressionMagic)] {
	case 1:
		gzipReader, err := gzip.NewReader(encoded)
		if err != nil {
			return nil, err
		}
		reader = gzipReader
	case 2:
		zstdReader, err := zstd.NewReader(encoded, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(64<<20))
		if err != nil {
			return nil, err
		}
		reader = zstdReader.IOReadCloser()
	default:
		return nil, errors.New("unsupported compressed payload encoding")
	}
	defer reader.Close()
	decoded, err := io.ReadAll(io.LimitReader(reader, int64(maximum)+1))
	if err != nil {
		return nil, err
	}
	if len(decoded) != original || len(decoded) > maximum {
		return nil, errors.New("invalid decompressed payload length")
	}
	return decoded, nil
}
