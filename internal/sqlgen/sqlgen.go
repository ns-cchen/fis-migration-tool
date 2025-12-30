// Copyright (c) 2024 Netskope, Inc. All rights reserved.

package sqlgen

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/netSkope/fis-migration-tool/internal/config"
	"github.com/netSkope/fis-migration-tool/internal/exporter"
	"github.com/netSkope/fis-migration-tool/internal/s3"
	"github.com/netSkope/fis-migration-tool/internal/store"
	"github.com/netSkope/fis-migration-tool/internal/util"
	"go.uber.org/zap"
)

// GenerateLoadDataSQL generates LOAD DATA FROM S3 SQL statements for each CSV file.
func GenerateLoadDataSQL(csvFiles []exporter.CSVFile, cfg *config.Config) ([]string, error) {
	var sqlStatements []string

	for _, csvFile := range csvFiles {
		s3Path := fmt.Sprintf("s3://%s/%s", cfg.S3Bucket, csvFile.S3Key)
		// Use IGNORE to skip duplicate entries (based on unique key: tenantid, hash)
		// This allows re-running migration without failing on existing data
		sql := fmt.Sprintf(`LOAD DATA FROM S3 '%s'
IGNORE
INTO TABLE %s
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
(tenantid, hash, aggr, last_modified, version);`,
			s3Path, cfg.TableName)

		sqlStatements = append(sqlStatements, sql)
	}

	return sqlStatements, nil
}

// WriteSQLFile writes SQL statements to a file.
func WriteSQLFile(sqlStatements []string, filepath string) error {
	file, err := os.Create(filepath)
	if err != nil {
		return fmt.Errorf("failed to create SQL file: %w", err)
	}
	defer file.Close()

	for _, sql := range sqlStatements {
		if _, err := file.WriteString(sql + "\n\n"); err != nil {
			return fmt.Errorf("failed to write SQL: %w", err)
		}
	}

	return nil
}

// ExecuteLoadDataSQL executes SQL statements on Aurora MySQL.
func ExecuteLoadDataSQL(sqlStatements []string, cfg *config.Config, logger *zap.Logger) error {
	if len(sqlStatements) == 0 {
		return fmt.Errorf("no SQL statements to execute")
	}

	// Load AWS credentials
	util.LoadAWSCredentialsFromVault()

	// Resolve Aurora password from Secrets Manager
	awsPwd, err := util.ResolveAWSDBPassword(cfg.AuroraSecretsManagerSecret, cfg.AuroraRegion)
	if err != nil {
		return fmt.Errorf("failed to get AWS password from Secrets Manager: %w", err)
	}

	// Create Aurora MySQL client
	hostname := cfg.AuroraHost
	if cfg.AuroraPort > 0 && cfg.AuroraPort != 3306 {
		hostname = fmt.Sprintf("%s:%d", cfg.AuroraHost, cfg.AuroraPort)
	}

	auroraClient, err := store.NewSQLClient(hostname, cfg.AuroraUser, awsPwd, cfg.SQLExecTimeout, "aws-aurora", cfg.AuroraDatabase)
	if err != nil {
		return fmt.Errorf("failed to create Aurora MySQL client: %w", err)
	}
	defer auroraClient.Close()

	// Validate connection with retry
	var lastErr error
	delay := 1 * time.Second
	for attempt := 1; attempt <= 3; attempt++ {
		if err := auroraClient.Ping(); err == nil {
			break
		}
		lastErr = err
		if attempt < 3 {
			logger.Warn("Aurora MySQL ping failed, retrying",
				zap.Int("attempt", attempt),
				zap.Error(err))
			time.Sleep(delay)
			delay = delay * 2 // Exponential backoff
		}
	}

	if lastErr != nil {
		return fmt.Errorf("failed to connect to Aurora MySQL after retries: %w", lastErr)
	}

	logger.Info("Connected to Aurora MySQL successfully")

	// Execute SQL statements sequentially
	successCount := 0
	failureCount := 0

	for i, sql := range sqlStatements {
		logger.Info("Executing LOAD DATA FROM S3",
			zap.Int("statement", i+1),
			zap.Int("total", len(sqlStatements)))

		startTime := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.SQLExecTimeout)*time.Second)
		defer cancel()

		_, err := auroraClient.GetDB().ExecContext(ctx, sql)
		elapsed := time.Since(startTime)

		if err != nil {
			errorMsg := err.Error()

			// Check for duplicate entry errors - these are expected if data already exists
			// With IGNORE keyword, duplicates should be skipped, but check anyway for safety
			if strings.Contains(errorMsg, "Duplicate entry") || strings.Contains(errorMsg, "Error 1062") {
				logger.Warn("LOAD DATA FROM S3 skipped duplicate entries (data may already exist)",
					zap.Int("statement", i+1),
					zap.Duration("elapsed", elapsed),
					zap.String("error", errorMsg))
				// Treat duplicates as success since IGNORE should handle them
				// But if we still get this error, it means IGNORE didn't work, so log as warning
				successCount++
				continue
			}

			// Check for Aurora MySQL IAM role configuration error
			if strings.Contains(errorMsg, "aurora_load_from_s3_role") || strings.Contains(errorMsg, "aws_default_s3_role") {
				failureCount++
				logger.Error("LOAD DATA FROM S3 execution failed - Aurora MySQL IAM role not configured",
					zap.Int("statement", i+1),
					zap.Duration("elapsed", elapsed),
					zap.String("error", errorMsg),
					zap.String("fix", "Configure aurora_load_from_s3_role or aws_default_s3_role on Aurora MySQL cluster. See AWS documentation for LOAD DATA FROM S3 IAM role setup."))
			} else {
				failureCount++
				logger.Error("LOAD DATA FROM S3 execution failed",
					zap.Int("statement", i+1),
					zap.Duration("elapsed", elapsed),
					zap.Error(err))
			}
			// Continue with next statement instead of failing entire migration
			continue
		}

		successCount++
		logger.Info("LOAD DATA FROM S3 completed",
			zap.Int("statement", i+1),
			zap.Duration("elapsed", elapsed))
	}

	logger.Info("SQL execution summary",
		zap.Int("total", len(sqlStatements)),
		zap.Int("success", successCount),
		zap.Int("failure", failureCount))

	if failureCount > 0 {
		return fmt.Errorf("some SQL statements failed: %d/%d succeeded", successCount, len(sqlStatements))
	}

	return nil
}

// GenerateSQLFile generates and writes SQL file for LOAD DATA FROM S3 (local file).
// Deprecated: Use GenerateAndUploadSQL for production.
func GenerateSQLFile(csvFiles []exporter.CSVFile, cfg *config.Config) (string, error) {
	sqlStatements, err := GenerateLoadDataSQL(csvFiles, cfg)
	if err != nil {
		return "", fmt.Errorf("failed to generate SQL: %w", err)
	}

	filename := fmt.Sprintf("load-data-tenant-%d.sql", cfg.TenantID)
	// Use /tmp for consistency across platforms
	filepath := filepath.Join("/tmp", filename)

	if err := WriteSQLFile(sqlStatements, filepath); err != nil {
		return "", fmt.Errorf("failed to write SQL file: %w", err)
	}

	return filepath, nil
}

// GenerateAndUploadSQL generates SQL statements and uploads to S3.
// Returns the S3 key of the uploaded SQL file.
func GenerateAndUploadSQL(csvFiles []exporter.CSVFile, cfg *config.Config, uploader *s3.Uploader, logger *zap.Logger) (string, error) {
	sqlStatements, err := GenerateLoadDataSQL(csvFiles, cfg)
	if err != nil {
		return "", fmt.Errorf("failed to generate SQL: %w", err)
	}

	// Generate SQL file content in memory
	var sqlContent bytes.Buffer
	for _, sql := range sqlStatements {
		sqlContent.WriteString(sql)
		sqlContent.WriteString("\n\n")
	}

	// Generate S3 key for SQL file
	filename := fmt.Sprintf("load-data-tenant-%d.sql", cfg.TenantID)
	s3Key := fmt.Sprintf("%s/sql/%s", cfg.S3Prefix, filename)

	logger.Info("Uploading SQL file to S3",
		zap.String("s3_key", s3Key),
		zap.Int("statements", len(sqlStatements)))

	// Write SQL content to temporary file and upload
	tmpFile, err := os.CreateTemp("", filename)
	if err != nil {
		return "", fmt.Errorf("failed to create temp file: %w", err)
	}
	tmpFilePath := tmpFile.Name()
	defer os.Remove(tmpFilePath)

	if _, err := tmpFile.Write(sqlContent.Bytes()); err != nil {
		tmpFile.Close()
		return "", fmt.Errorf("failed to write SQL to temp file: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		return "", fmt.Errorf("failed to close temp file: %w", err)
	}

	// Upload SQL file to S3
	if err := uploader.UploadFileWithRetry(tmpFilePath, s3Key); err != nil {
		return "", fmt.Errorf("failed to upload SQL file to S3: %w", err)
	}

	logger.Info("SQL file uploaded to S3",
		zap.String("s3_key", s3Key))

	return s3Key, nil
}

