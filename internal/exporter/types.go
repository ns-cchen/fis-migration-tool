// Copyright (c) 2024 Netskope, Inc. All rights reserved.

package exporter

import (
	"time"

	"github.com/netSkope/fis-migration-tool/internal/segment"
)

// Row represents a single row from fis_aggr table.
type Row struct {
	TenantID     int
	Hash         string
	Aggr         string
	LastModified *time.Time
	Version      *int
}

// CSVFile represents a generated CSV file.
// For streaming uploads, FilePath will be empty as data is streamed directly to S3.
type CSVFile struct {
	FilePath string // Empty for streaming uploads
	S3Key    string
	Segment  segment.Segment
	RowCount int
}

