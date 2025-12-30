// Copyright (c) 2024 Netskope, Inc. All rights reserved.

package segment

import (
	"fmt"
	"math/big"
)

// Segment represents a hash range segment.
type Segment struct {
	Index   int    // Segment index (0-based)
	StartHex string // Start hex value (inclusive)
	EndHex   string // End hex value (exclusive, except for last segment)
}

// SegmentHashSpace partitions the hash space [00, FF] into N segments.
// Returns a slice of segments with hex boundaries.
func SegmentHashSpace(segments int) ([]Segment, error) {
	if segments <= 0 {
		return nil, fmt.Errorf("segments must be positive, got %d", segments)
	}
	if segments > 256 {
		return nil, fmt.Errorf("segments cannot exceed 256, got %d", segments)
	}

	segs := make([]Segment, segments)

	// Total hash space: 0x00 to 0xFF (256 values)
	// Each segment gets approximately 256/segments values
	segmentSize := 256 / segments
	remainder := 256 % segments

	start := 0
	for i := 0; i < segments; i++ {
		// Distribute remainder across first segments
		size := segmentSize
		if i < remainder {
			size++
		}

		end := start + size
		if end > 256 {
			end = 256
		}

		segs[i] = Segment{
			Index:   i,
			StartHex: intToHex(start),
			EndHex:   intToHex(end),
		}

		start = end
	}

	// Last segment should include FF (make end exclusive for all but last)
	if len(segs) > 0 {
		lastIdx := len(segs) - 1
		// For the last segment, end should be "100" (exclusive) to include FF
		if segs[lastIdx].EndHex == "ff" {
			segs[lastIdx].EndHex = "100"
		}
	}

	return segs, nil
}

// intToHex converts an integer (0-256) to a 2-digit hex string.
// For values >= 256, returns "100" (to make range exclusive).
func intToHex(val int) string {
	if val >= 256 {
		return "100"
	}
	return fmt.Sprintf("%02x", val)
}

// SegmentToHexRange converts a segment index to hex boundaries.
// This is a helper function that matches the segmentation logic.
func SegmentToHexRange(segmentIndex int, totalSegments int) (start string, end string, err error) {
	if segmentIndex < 0 || segmentIndex >= totalSegments {
		return "", "", fmt.Errorf("segment index %d out of range [0, %d)", segmentIndex, totalSegments)
	}

	segs, err := SegmentHashSpace(totalSegments)
	if err != nil {
		return "", "", err
	}

	if segmentIndex >= len(segs) {
		return "", "", fmt.Errorf("segment index %d out of range", segmentIndex)
	}

	return segs[segmentIndex].StartHex, segs[segmentIndex].EndHex, nil
}

// HashInSegment checks if a hash (hex string) falls within a segment's range.
func HashInSegment(hash string, seg Segment) bool {
	if len(hash) < 2 {
		return false
	}

	// Compare first 2 hex characters
	hashPrefix := hash[:2]
	return hashPrefix >= seg.StartHex && hashPrefix < seg.EndHex
}

// HexToInt converts a 2-digit hex string to an integer.
func HexToInt(hexStr string) (int, error) {
	val := new(big.Int)
	val, ok := val.SetString(hexStr, 16)
	if !ok {
		return 0, fmt.Errorf("invalid hex string: %s", hexStr)
	}
	return int(val.Int64()), nil
}

