// Copyright (c) 2024 Netskope, Inc. All rights reserved.

package sqlgen

import (
	"os"
	"strings"
	"testing"

	"github.com/netSkope/fis-migration-tool/internal/config"
	"github.com/netSkope/fis-migration-tool/internal/exporter"
	"github.com/netSkope/fis-migration-tool/internal/segment"
)

func TestGenerateLoadDataSQL(t *testing.T) {
	cfg := &config.Config{
		S3Bucket:  "test-bucket",
		TableName: "fis_aggr",
	}

	csvFiles := []exporter.CSVFile{
		{
			S3Key: "prefix/tenant-1234/fis_aggr/file1.csv",
			Segment: segment.Segment{
				Index:   0,
				StartHex: "00",
				EndHex:   "10",
			},
			RowCount: 1000,
		},
		{
			S3Key: "prefix/tenant-1234/fis_aggr/file2.csv",
			Segment: segment.Segment{
				Index:   1,
				StartHex: "10",
				EndHex:   "20",
			},
			RowCount: 2000,
		},
	}

	sqlStatements, err := GenerateLoadDataSQL(csvFiles, cfg)
	if err != nil {
		t.Fatalf("GenerateLoadDataSQL() error = %v", err)
	}

	if len(sqlStatements) != 2 {
		t.Errorf("expected 2 SQL statements, got %d", len(sqlStatements))
	}

	// Check first SQL statement
	sql1 := sqlStatements[0]
	if !strings.Contains(sql1, "s3://test-bucket/prefix/tenant-1234/fis_aggr/file1.csv") {
		t.Errorf("SQL should contain S3 path for file1")
	}
	if !strings.Contains(sql1, "INTO TABLE fis_aggr") {
		t.Errorf("SQL should contain table name")
	}
	if !strings.Contains(sql1, "IGNORE") {
		t.Errorf("SQL should contain IGNORE keyword")
	}
	if !strings.Contains(sql1, "LOAD DATA FROM S3") {
		t.Errorf("SQL should contain LOAD DATA FROM S3")
	}
}

func TestWriteSQLFile(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "test-*.sql")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	sqlStatements := []string{
		"LOAD DATA FROM S3 's3://bucket/file1.csv' INTO TABLE test;",
		"LOAD DATA FROM S3 's3://bucket/file2.csv' INTO TABLE test;",
	}

	if err := WriteSQLFile(sqlStatements, tmpFile.Name()); err != nil {
		t.Errorf("WriteSQLFile() error = %v", err)
	}

	// Verify file was created and has content
	content, err := os.ReadFile(tmpFile.Name())
	if err != nil {
		t.Fatalf("failed to read SQL file: %v", err)
	}

	contentStr := string(content)
	if !strings.Contains(contentStr, "file1.csv") {
		t.Errorf("SQL file should contain file1.csv")
	}
	if !strings.Contains(contentStr, "file2.csv") {
		t.Errorf("SQL file should contain file2.csv")
	}
}

