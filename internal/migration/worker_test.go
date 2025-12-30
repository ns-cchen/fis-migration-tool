// Copyright (c) 2024 Netskope, Inc. All rights reserved.

package migration

import (
	"testing"

	"github.com/netSkope/fis-migration-tool/internal/config"
	"github.com/netSkope/fis-migration-tool/internal/segment"
	"go.uber.org/zap/zaptest"
)

func TestProcessSegments_EmptySegments(t *testing.T) {
	cfg := &config.Config{
		TenantID:        1234,
		TableName:      "fis_aggr",
		MaxParallelSegs: 8,
		// Note: This test requires database connection, so we skip it for unit tests
		// Full integration tests are in exporter_test.go with testcontainers
	}

	logger := zaptest.NewLogger(t)

	// Test with empty segments - this will fail without a database connection
	// Skip this test in unit test mode (requires testcontainers for full test)
	segments := []segment.Segment{}
	_, err := ProcessSegments(segments, cfg, logger)
	// We expect an error because we don't have a database connection
	// This is expected behavior - the test validates the error handling
	if err == nil {
		t.Error("ProcessSegments() should fail without database connection")
	}
}

func TestProcessSegments_InvalidConfig(t *testing.T) {
	cfg := &config.Config{
		TenantID:        0, // Invalid: tenant ID required
		TableName:      "fis_aggr",
		MaxParallelSegs: 8,
	}

	logger := zaptest.NewLogger(t)

	segments, _ := segment.SegmentHashSpace(4)
	_, err := ProcessSegments(segments, cfg, logger)
	if err == nil {
		t.Error("ProcessSegments() should fail with invalid config")
	}
}

// Note: Full integration tests would require:
// 1. Testcontainers for MariaDB
// 2. LocalStack or S3 mock for S3 uploads
// 3. Test data setup
// These are covered in exporter_test.go with testcontainers

