package osm

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/johanjk/imposm-operator/internal/strutil"
	"io"
	"os"
	"time"

	"google.golang.org/protobuf/encoding/protowire"
)

// PBFHeader holds the replication fields of an OSM PBF header block.
// Sequence is 0 and BaseURL empty when the file does not carry them.
type PBFHeader struct {
	BaseURL   string
	Sequence  int64
	Timestamp time.Time
}

// Limits from the PBF specification.
const (
	maxBlobHeaderSize = 64 << 10
	maxBlobSize       = 32 << 20
)

// HeaderBlock field numbers (osmformat.proto).
const (
	fieldReplTimestamp = 32
	fieldReplSequence  = 33
	fieldReplBaseURL   = 34
)

// ReadPBFHeader reads the replication fields from the first (OSMHeader) blob
// of a PBF stream. Only the header blob is read.
func ReadPBFHeader(r io.Reader) (PBFHeader, error) {
	blobType, blob, err := readBlob(r)
	if err != nil {
		return PBFHeader{}, err
	}
	if blobType != "OSMHeader" {
		return PBFHeader{}, fmt.Errorf("pbf: first blob is %q, not OSMHeader", strutil.Truncate(blobType, 43))
	}
	data, err := blobData(blob)
	if err != nil {
		return PBFHeader{}, err
	}
	var h PBFHeader
	err = walkFields(data, func(num protowire.Number, typ protowire.Type, v []byte, n uint64) error {
		switch {
		case num == fieldReplTimestamp && typ == protowire.VarintType:
			h.Timestamp = time.Unix(int64(n), 0).UTC()
		case num == fieldReplSequence && typ == protowire.VarintType:
			if int64(n) < 0 || int64(n) > MaxSequence {
				return fmt.Errorf("pbf: replication sequence %d out of range", int64(n))
			}
			h.Sequence = int64(n)
		case num == fieldReplBaseURL && typ == protowire.BytesType:
			h.BaseURL = string(v)
		}
		return nil
	})
	if err != nil {
		return PBFHeader{}, fmt.Errorf("pbf header block: %w", err)
	}
	return h, nil
}

// ReadPBFHeaderFile is ReadPBFHeader on a file path.
func ReadPBFHeaderFile(path string) (PBFHeader, error) {
	f, err := os.Open(path)
	if err != nil {
		return PBFHeader{}, err
	}
	defer f.Close()
	return ReadPBFHeader(f)
}

// StripReplicationHeader copies a PBF from r to w with the replication fields
// removed from the header block. Used to build test fixtures that exercise the
// timestamp -> sequence fallback. keepTimestamp retains the timestamp field.
func StripReplicationHeader(r io.Reader, w io.Writer, keepTimestamp bool) error {
	blobType, blob, err := readBlob(r)
	if err != nil {
		return err
	}
	if blobType != "OSMHeader" {
		return fmt.Errorf("pbf: first blob is %q", blobType)
	}
	data, err := blobData(blob)
	if err != nil {
		return err
	}
	var hb []byte
	err = walkFields(data, func(num protowire.Number, typ protowire.Type, v []byte, n uint64) error {
		switch num {
		case fieldReplSequence, fieldReplBaseURL:
			return nil
		case fieldReplTimestamp:
			if !keepTimestamp {
				return nil
			}
		}
		hb = protowire.AppendTag(hb, num, typ)
		if typ == protowire.VarintType {
			hb = protowire.AppendVarint(hb, n)
		} else {
			hb = protowire.AppendBytes(hb, v)
		}
		return nil
	})
	if err != nil {
		return err
	}
	var newBlob []byte
	newBlob = protowire.AppendTag(newBlob, 1, protowire.BytesType) // raw
	newBlob = protowire.AppendBytes(newBlob, hb)
	newBlob = protowire.AppendTag(newBlob, 2, protowire.VarintType) // raw_size
	newBlob = protowire.AppendVarint(newBlob, uint64(len(hb)))

	var bh []byte
	bh = protowire.AppendTag(bh, 1, protowire.BytesType)
	bh = protowire.AppendString(bh, "OSMHeader")
	bh = protowire.AppendTag(bh, 3, protowire.VarintType)
	bh = protowire.AppendVarint(bh, uint64(len(newBlob)))

	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(bh)))
	for _, b := range [][]byte{lenBuf[:], bh, newBlob} {
		if _, err := w.Write(b); err != nil {
			return err
		}
	}
	_, err = io.Copy(w, r)
	return err
}

func readBlob(r io.Reader) (string, []byte, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return "", nil, fmt.Errorf("pbf: blob header length: %w", err)
	}
	hl := binary.BigEndian.Uint32(lenBuf[:])
	if hl == 0 || hl > maxBlobHeaderSize {
		return "", nil, fmt.Errorf("pbf: blob header size %d out of range", hl)
	}
	bh := make([]byte, hl)
	if _, err := io.ReadFull(r, bh); err != nil {
		return "", nil, fmt.Errorf("pbf: blob header: %w", err)
	}
	var (
		blobType string
		dataSize int64 = -1
	)
	err := walkFields(bh, func(num protowire.Number, typ protowire.Type, v []byte, n uint64) error {
		switch {
		case num == 1 && typ == protowire.BytesType:
			blobType = string(v)
		case num == 3 && typ == protowire.VarintType:
			dataSize = int64(int32(n))
		}
		return nil
	})
	if err != nil {
		return "", nil, fmt.Errorf("pbf: blob header: %w", err)
	}
	if dataSize < 0 || dataSize > maxBlobSize {
		return "", nil, fmt.Errorf("pbf: blob size %d out of range", dataSize)
	}
	blob := make([]byte, dataSize)
	if _, err := io.ReadFull(r, blob); err != nil {
		return "", nil, fmt.Errorf("pbf: blob: %w", err)
	}
	return blobType, blob, nil
}

func blobData(blob []byte) ([]byte, error) {
	var (
		raw, zdata []byte
		rawSize    int64 = -1
		other      protowire.Number
	)
	err := walkFields(blob, func(num protowire.Number, typ protowire.Type, v []byte, n uint64) error {
		switch {
		case num == 1 && typ == protowire.BytesType:
			raw = v
		case num == 2 && typ == protowire.VarintType:
			rawSize = int64(int32(n))
		case num == 3 && typ == protowire.BytesType:
			zdata = v
		case num >= 4 && num <= 7:
			other = num
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("pbf: blob: %w", err)
	}
	switch {
	case raw != nil:
		return raw, nil
	case zdata != nil:
		if rawSize < 0 || rawSize > maxBlobSize {
			return nil, fmt.Errorf("pbf: raw_size %d out of range", rawSize)
		}
		zr, err := zlib.NewReader(bytes.NewReader(zdata))
		if err != nil {
			return nil, fmt.Errorf("pbf: zlib: %w", err)
		}
		out, err := io.ReadAll(io.LimitReader(zr, rawSize+1))
		if err != nil {
			return nil, fmt.Errorf("pbf: zlib: %w", err)
		}
		if int64(len(out)) != rawSize {
			return nil, fmt.Errorf("pbf: zlib: got %d bytes, raw_size %d", len(out), rawSize)
		}
		return out, nil
	case other != 0:
		return nil, fmt.Errorf("pbf: unsupported blob compression (field %d)", other)
	default:
		return nil, errors.New("pbf: empty blob")
	}
}

// walkFields iterates over the top-level fields of a protobuf message. For
// varints n holds the value; for length-delimited fields v holds the bytes.
func walkFields(b []byte, fn func(num protowire.Number, typ protowire.Type, v []byte, n uint64) error) error {
	for len(b) > 0 {
		num, typ, l := protowire.ConsumeTag(b)
		if l < 0 {
			return protowire.ParseError(l)
		}
		b = b[l:]
		var (
			v []byte
			n uint64
		)
		switch typ {
		case protowire.VarintType:
			n, l = protowire.ConsumeVarint(b)
		case protowire.BytesType:
			v, l = protowire.ConsumeBytes(b)
		default:
			l = protowire.ConsumeFieldValue(num, typ, b)
		}
		if l < 0 {
			return protowire.ParseError(l)
		}
		b = b[l:]
		if typ == protowire.VarintType || typ == protowire.BytesType {
			if err := fn(num, typ, v, n); err != nil {
				return err
			}
		}
	}
	return nil
}
