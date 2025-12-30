// Copyright (c) 2024 Netskope, Inc. All rights reserved.

package exporter

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/netSkope/fis-migration-tool/internal/config"
	"github.com/netSkope/fis-migration-tool/internal/s3"
	"github.com/netSkope/fis-migration-tool/internal/segment"
	"go.uber.org/zap"
)

// MultipartUploadStreamer is an interface for streaming multipart uploads to S3.
// This allows mocking in tests.
type MultipartUploadStreamer interface {
	UploadPart(data []byte) error
	Complete() error
	Abort()
}

// s3StreamAdapter adapts s3.MultipartUploadStream to MultipartUploadStreamer interface
type s3StreamAdapter struct {
	stream *s3.MultipartUploadStream
}

func (a *s3StreamAdapter) UploadPart(data []byte) error {
	return a.stream.UploadPart(data)
}

func (a *s3StreamAdapter) Complete() error {
	return a.stream.Complete()
}

func (a *s3StreamAdapter) Abort() {
	a.stream.Abort()
}

// MultipartUploadStreamCreator creates a new multipart upload stream.
type MultipartUploadStreamCreator interface {
	NewMultipartUploadStream(s3Key string) (MultipartUploadStreamer, error)
}

// s3UploaderAdapter adapts s3.Uploader to MultipartUploadStreamCreator interface
type s3UploaderAdapter struct {
	uploader *s3.Uploader
}

func (a *s3UploaderAdapter) NewMultipartUploadStream(s3Key string) (MultipartUploadStreamer, error) {
	stream, err := a.uploader.NewMultipartUploadStream(s3Key)
	if err != nil {
		return nil, err
	}
	return &s3StreamAdapter{stream: stream}, nil
}

// NewS3UploaderAdapter creates an adapter for s3.Uploader
func NewS3UploaderAdapter(uploader *s3.Uploader) MultipartUploadStreamCreator {
	return &s3UploaderAdapter{uploader: uploader}
}

// Exporter handles CSV export from MariaDB.
type Exporter struct {
	db     *sql.DB
	config *config.Config
	logger *zap.Logger
}

// NewExporter creates a new CSV exporter.
func NewExporter(cfg *config.Config, logger *zap.Logger) (*Exporter, error) {
	dsn := cfg.GetMariaDBDSN()

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return &Exporter{
		db:     db,
		config: cfg,
		logger: logger,
	}, nil
}

// Close closes the database connection.
func (e *Exporter) Close() error {
	if e.db != nil {
		return e.db.Close()
	}
	return nil
}

// ExportSegment exports data for a single segment using streaming multipart upload to S3.
// Returns a single CSVFile for the hash range.
// Uses a transaction with REPEATABLE READ isolation to get a consistent snapshot,
// preventing new inserts from fis-updater from causing infinite pagination loops.
// Each 100k-row batch is converted to CSV bytes and uploaded as a separate multipart part.
func (e *Exporter) ExportSegment(seg segment.Segment, uploader MultipartUploadStreamCreator) (*CSVFile, error) {
	// Generate S3 key (one file per hash range)
	filename := fmt.Sprintf("tenant-%d.%s.hash-%s-%s.csv",
		e.config.TenantID, e.config.TableName, seg.StartHex, seg.EndHex)
	s3Key := fmt.Sprintf("%s/tenant-%d/%s/%s",
		e.config.S3Prefix, e.config.TenantID, e.config.TableName, filename)

	// Initiate multipart upload stream
	stream, err := uploader.NewMultipartUploadStream(s3Key)
	if err != nil {
		return nil, fmt.Errorf("failed to initiate multipart upload: %w", err)
	}
	defer func() {
		if err != nil {
			stream.Abort()
		}
	}()

	// Start a transaction with REPEATABLE READ isolation to get a consistent snapshot
	// This prevents new inserts from fis-updater from appearing during pagination
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	tx, err := e.db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.Rollback() // Safe to call even if committed

	lastHash := "" // Track last hash for pagination
	batchNum := 0
	totalRows := 0
	maxBatches := 10000 // Safety limit to prevent infinite loops
	headerWritten := false

	for batchNum < maxBatches {
		var rows []Row
		var queryErr error

		// Query segment (with cursor if not first batch)
		if batchNum == 0 {
			// First batch: query from segment start (no cursor)
			rows, queryErr = e.querySegmentInTx(tx, seg, "", ctx)
		} else {
			// Subsequent batches: query from last hash (cursor-based pagination)
			rows, queryErr = e.querySegmentInTx(tx, seg, lastHash, ctx)
		}

		if queryErr != nil {
			return nil, fmt.Errorf("failed to query segment: %w", queryErr)
		}

		if len(rows) == 0 {
			break // No more data
		}

		// Update last hash for next iteration
		if len(rows) > 0 {
			lastHash = rows[len(rows)-1].Hash
		}

		// Convert rows to CSV bytes and upload as multipart part
		csvBytes, err := e.rowsToCSVBytes(rows, !headerWritten)
		if err != nil {
			return nil, fmt.Errorf("failed to convert rows to CSV: %w", err)
		}
		headerWritten = true

		// Upload batch as multipart part
		if err := stream.UploadPart(csvBytes); err != nil {
			return nil, fmt.Errorf("failed to upload batch as multipart part: %w", err)
		}

		totalRows += len(rows)

		e.logger.Info("Exported and uploaded segment batch",
			zap.Int("segment", seg.Index),
			zap.Int("batch", batchNum+1),
			zap.Int("rows", len(rows)),
			zap.Int("total_rows", totalRows),
			zap.String("s3_key", s3Key))

		// If we got fewer rows than batch size, we're done
		if len(rows) < e.config.BatchSize {
			break
		}

		batchNum++
	}

	// Commit transaction (read-only, but needed to release locks)
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	if batchNum >= maxBatches {
		e.logger.Warn("Segment export hit maximum batch limit",
			zap.Int("segment", seg.Index),
			zap.Int("max_batches", maxBatches),
			zap.Int("total_batches", batchNum))
	}

	if totalRows == 0 {
		// No data exported, abort multipart upload
		stream.Abort()
		return nil, nil
	}

	// Complete multipart upload
	if err := stream.Complete(); err != nil {
		return nil, fmt.Errorf("failed to complete multipart upload: %w", err)
	}

	return &CSVFile{
		FilePath: "", // Empty for streaming uploads
		S3Key:    s3Key,
		Segment:  seg,
		RowCount: totalRows,
	}, nil
}

// rowsToCSVBytes converts rows to CSV bytes in memory.
func (e *Exporter) rowsToCSVBytes(rows []Row, includeHeader bool) ([]byte, error) {
	var buf bytes.Buffer
	writer := csv.NewWriter(&buf)

	if includeHeader {
		header := []string{"tenantid", "hash", "aggr", "last_modified", "version"}
		if err := writer.Write(header); err != nil {
			return nil, fmt.Errorf("failed to write CSV header: %w", err)
		}
	}

	for _, row := range rows {
		record := []string{
			fmt.Sprintf("%d", row.TenantID),
			row.Hash,
			row.Aggr,
			formatTimestamp(row.LastModified),
			formatInt(row.Version),
		}
		if err := writer.Write(record); err != nil {
			return nil, fmt.Errorf("failed to write CSV row: %w", err)
		}
	}

	writer.Flush()
	if err := writer.Error(); err != nil {
		return nil, fmt.Errorf("failed to flush CSV: %w", err)
	}

	return buf.Bytes(), nil
}

// querySegmentInTx queries a segment within a transaction.
// If lastHash is provided (non-empty), it implements cursor-based pagination starting from that hash.
// If lastHash is empty, it queries from the segment start.
func (e *Exporter) querySegmentInTx(tx *sql.Tx, seg segment.Segment, lastHash string, ctx context.Context) ([]Row, error) {
	// Handle the special case where EndHex is "100" (means >= 256, should include "ff")
	// For the last segment, we use <= "ff" instead of < "100"
	endHex := seg.EndHex
	useLessThan := true
	if endHex == "100" {
		endHex = "ff"       // Compare against "ff" for the last segment
		useLessThan = false // Use <= for the last segment to include "ff"
	}

	// Use database.table format if database is specified
	tableRef := e.config.TableName
	if e.config.MariaDBDatabase != "" {
		tableRef = fmt.Sprintf("%s.%s", e.config.MariaDBDatabase, e.config.TableName)
	}

	// Build hash condition based on whether we have a cursor (lastHash) and segment type
	var hashCondition string
	hasCursor := lastHash != ""

	if hasCursor {
		// Cursor-based pagination: hash > lastHash AND hash >= startHex AND hash < endHex
		// This ensures we:
		// 1. Continue from where we left off (hash > lastHash)
		// 2. Stay within the segment boundaries (hash >= startHex AND hash < endHex)
		if !useLessThan {
			// Last segment: hash > lastHash AND hash >= startHex AND hash <= 'ff'
			hashCondition = "hash > ? AND hash >= ? AND hash <= 'ff'"
		} else {
			// Regular segment: hash > lastHash AND hash >= startHex AND hash < endHex
			hashCondition = "hash > ? AND hash >= ? AND hash < ?"
		}
	} else {
		// No cursor: query from segment start
		// Uses lexicographic string comparison: comparing '00' (2 chars) against full hash strings
		// like '00abc123...' (32 chars) works because shorter prefix strings compare less than
		// longer strings that start with that prefix. This allows prefix matching via direct
		// string comparison.
		if !useLessThan {
			// Last segment: hash >= startHex AND hash <= 'ff' (inclusive of 'ff' prefix)
			// Note: 'ff' as string boundary includes all hashes starting with 'ff'
			hashCondition = "hash >= ? AND hash <= 'ff'"
		} else {
			// Regular segment: hash >= startHex AND hash < endHex (exclusive)
			// This matches all hashes where the first 2 hex chars are in [startHex, endHex)
			hashCondition = "hash >= ? AND hash < ?"
		}
	}

	query := fmt.Sprintf(`
		SELECT tenantid, hash, aggr, last_modified, version
		FROM %s
		WHERE tenantid = ?
		  AND %s
		ORDER BY hash
		LIMIT ?`,
		tableRef, hashCondition)

	// Build args based on cursor presence and segment type
	var args []interface{}
	args = append(args, e.config.TenantID)

	if hasCursor {
		// Cursor-based: add lastHash first, then segment bounds
		args = append(args, lastHash)
		args = append(args, seg.StartHex)
		if useLessThan {
			args = append(args, endHex)
		}
	} else {
		// No cursor: just segment bounds
		args = append(args, seg.StartHex)
		if useLessThan {
			args = append(args, endHex)
		}
	}

	args = append(args, e.config.BatchSize)

	e.logger.Debug("Querying segment",
		zap.Int("segment", seg.Index),
		zap.String("start_hex", seg.StartHex),
		zap.String("end_hex", seg.EndHex),
		zap.String("end_hex_adjusted", endHex),
		zap.String("last_hash", lastHash),
		zap.Bool("has_cursor", hasCursor),
		zap.String("query", query),
		zap.Int("tenant_id", e.config.TenantID))

	// Use transaction to ensure REPEATABLE READ isolation
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	var result []Row
	for rows.Next() {
		var r Row
		var lastModified sql.NullTime
		var version sql.NullInt64

		if err := rows.Scan(&r.TenantID, &r.Hash, &r.Aggr, &lastModified, &version); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		if lastModified.Valid {
			r.LastModified = &lastModified.Time
		}
		if version.Valid {
			v := int(version.Int64)
			r.Version = &v
		}

		result = append(result, r)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("row iteration error: %w", err)
	}

	return result, nil
}

// formatTimestamp formats a timestamp for CSV.
func formatTimestamp(t *time.Time) string {
	if t == nil {
		return ""
	}
	// MySQL timestamp format: YYYY-MM-DD HH:MM:SS
	return t.Format("2006-01-02 15:04:05")
}

// formatInt formats an integer pointer for CSV.
func formatInt(i *int) string {
	if i == nil {
		return ""
	}
	return fmt.Sprintf("%d", *i)
}
