// Copyright (c) 2024 Netskope, Inc. All rights reserved.

package segment

import (
	"testing"
)

func TestSegmentHashSpace(t *testing.T) {
	tests := []struct {
		name     string
		segments int
		wantErr  bool
		validate func(t *testing.T, segs []Segment)
	}{
		{
			name:     "16 segments",
			segments: 16,
			wantErr:  false,
			validate: func(t *testing.T, segs []Segment) {
				if len(segs) != 16 {
					t.Errorf("expected 16 segments, got %d", len(segs))
				}
				// First segment should start at 00
				if segs[0].StartHex != "00" {
					t.Errorf("first segment should start at 00, got %s", segs[0].StartHex)
				}
				// Last segment should end at 100 (to include ff)
				if segs[15].EndHex != "100" {
					t.Errorf("last segment should end at 100, got %s", segs[15].EndHex)
				}
			},
		},
		{
			name:     "256 segments (one per hex value)",
			segments: 256,
			wantErr:  false,
			validate: func(t *testing.T, segs []Segment) {
				if len(segs) != 256 {
					t.Errorf("expected 256 segments, got %d", len(segs))
				}
				// First segment should be 00-01
				if segs[0].StartHex != "00" || segs[0].EndHex != "01" {
					t.Errorf("first segment should be 00-01, got %s-%s", segs[0].StartHex, segs[0].EndHex)
				}
				// Last segment should include ff
				if segs[255].StartHex != "ff" || segs[255].EndHex != "100" {
					t.Errorf("last segment should be ff-100, got %s-%s", segs[255].StartHex, segs[255].EndHex)
				}
			},
		},
		{
			name:     "1 segment (entire range)",
			segments: 1,
			wantErr:  false,
			validate: func(t *testing.T, segs []Segment) {
				if len(segs) != 1 {
					t.Errorf("expected 1 segment, got %d", len(segs))
				}
				if segs[0].StartHex != "00" || segs[0].EndHex != "100" {
					t.Errorf("single segment should be 00-100, got %s-%s", segs[0].StartHex, segs[0].EndHex)
				}
			},
		},
		{
			name:     "invalid: zero segments",
			segments: 0,
			wantErr:  true,
		},
		{
			name:     "invalid: too many segments",
			segments: 257,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			segs, err := SegmentHashSpace(tt.segments)
			if (err != nil) != tt.wantErr {
				t.Errorf("SegmentHashSpace() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && tt.validate != nil {
				tt.validate(t, segs)
			}
		})
	}
}

func TestHexToInt(t *testing.T) {
	tests := []struct {
		name    string
		hexStr  string
		want    int
		wantErr bool
	}{
		{"00", "00", 0, false},
		{"0a", "0a", 10, false},
		{"ff", "ff", 255, false},
		{"invalid", "gg", 0, true},
		{"empty", "", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := HexToInt(tt.hexStr)
			if (err != nil) != tt.wantErr {
				t.Errorf("HexToInt() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("HexToInt() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHashInSegment(t *testing.T) {
	seg := Segment{
		Index:   0,
		StartHex: "00",
		EndHex:   "10",
	}

	tests := []struct {
		name   string
		hash   string
		seg    Segment
		want   bool
	}{
		{"in range", "00abc123", seg, true},
		{"in range middle", "0aabc123", seg, true},
		{"at boundary start", "00abc123", seg, true},
		{"at boundary end", "0fabc123", seg, true},
		{"out of range low", "ffabc123", seg, false},
		{"out of range high", "10abc123", seg, false},
		{"too short", "0", seg, false},
		{"empty", "", seg, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := HashInSegment(tt.hash, tt.seg); got != tt.want {
				t.Errorf("HashInSegment() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSegmentToHexRange(t *testing.T) {
	tests := []struct {
		name          string
		segmentIndex  int
		totalSegments int
		wantErr       bool
	}{
		{"valid segment 0", 0, 16, false},
		{"valid segment 15", 15, 16, false},
		{"invalid: negative index", -1, 16, true},
		{"invalid: index too high", 16, 16, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			start, end, err := SegmentToHexRange(tt.segmentIndex, tt.totalSegments)
			if (err != nil) != tt.wantErr {
				t.Errorf("SegmentToHexRange() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if start == "" || end == "" {
					t.Errorf("SegmentToHexRange() returned empty strings")
				}
			}
		})
	}
}

