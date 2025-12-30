// Copyright (c) 2024 Netskope, Inc. All rights reserved.

package migration

import (
	"fmt"
	"sync"

	"github.com/netSkope/fis-migration-tool/internal/config"
	"github.com/netSkope/fis-migration-tool/internal/exporter"
	"github.com/netSkope/fis-migration-tool/internal/s3"
	"github.com/netSkope/fis-migration-tool/internal/segment"
	"go.uber.org/zap"
)

// ProcessSegments processes all segments in parallel batches.
func ProcessSegments(segments []segment.Segment, cfg *config.Config, logger *zap.Logger) ([]exporter.CSVFile, error) {
	exp, err := exporter.NewExporter(cfg, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create exporter: %w", err)
	}
	defer exp.Close()

	s3Uploader, err := s3.NewUploader(cfg, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create S3 uploader: %w", err)
	}

	var allCSVFiles []exporter.CSVFile
	var mu sync.Mutex
	var wg sync.WaitGroup

	// Process segments in batches
	maxParallel := cfg.MaxParallelSegs
	if maxParallel <= 0 {
		maxParallel = 8
	}

	// Process segments in batches
	for i := 0; i < len(segments); i += maxParallel {
		batchEnd := i + maxParallel
		if batchEnd > len(segments) {
			batchEnd = len(segments)
		}

		batch := segments[i:batchEnd]
		logger.Info("Processing segment batch",
			zap.Int("batch_start", i+1),
			zap.Int("batch_end", batchEnd),
			zap.Int("total_segments", len(segments)))

		// Process batch in parallel
		for _, seg := range batch {
			wg.Add(1)
			go func(s segment.Segment) {
				defer wg.Done()

				csvFiles, err := ProcessSegment(s, exp, s3Uploader, cfg, logger)
				if err != nil {
					logger.Error("Failed to process segment",
						zap.Int("segment", s.Index),
						zap.Error(err))
					return
				}

				mu.Lock()
				allCSVFiles = append(allCSVFiles, csvFiles...)
				mu.Unlock()

				logger.Info("Segment processed",
					zap.Int("segment", s.Index),
					zap.Int("csv_files", len(csvFiles)))
			}(seg)
		}

		// Wait for batch to complete
		wg.Wait()
	}

	logger.Info("All segments processed",
		zap.Int("total_segments", len(segments)),
		zap.Int("total_csv_files", len(allCSVFiles)))

	return allCSVFiles, nil
}

// ProcessSegment processes a single segment using streaming multipart upload.
// Returns a slice with a single CSVFile (or empty if no data).
// The export and upload happen together - each batch is uploaded as a multipart part.
func ProcessSegment(seg segment.Segment, exp *exporter.Exporter, s3Uploader *s3.Uploader, cfg *config.Config, logger *zap.Logger) ([]exporter.CSVFile, error) {
	logger.Info("Processing segment",
		zap.Int("segment", seg.Index),
		zap.String("start_hex", seg.StartHex),
		zap.String("end_hex", seg.EndHex))

	// Export segment using streaming multipart upload (upload happens during export)
	// Use adapter to convert s3.Uploader to interface
	uploaderAdapter := exporter.NewS3UploaderAdapter(s3Uploader)
	csvFile, err := exp.ExportSegment(seg, uploaderAdapter)
	if err != nil {
		return nil, fmt.Errorf("failed to export segment: %w", err)
	}

	// If no data was exported, return empty slice
	if csvFile == nil {
		logger.Info("Segment has no data",
			zap.Int("segment", seg.Index))
		return []exporter.CSVFile{}, nil
	}

	logger.Info("Segment completed",
		zap.Int("segment", seg.Index),
		zap.Int("rows", csvFile.RowCount),
		zap.String("s3_key", csvFile.S3Key))

	return []exporter.CSVFile{*csvFile}, nil
}
