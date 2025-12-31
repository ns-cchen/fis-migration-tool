// Copyright (c) 2024 Netskope, Inc. All rights reserved.

package main

import (
	"fmt"
	"os"

	"github.com/netSkope/fis-migration-tool/internal/config"
	fislog "github.com/netSkope/fis-migration-tool/internal/log"
	"github.com/netSkope/fis-migration-tool/internal/migration"
	"github.com/netSkope/fis-migration-tool/internal/s3"
	"github.com/netSkope/fis-migration-tool/internal/segment"
	"github.com/netSkope/fis-migration-tool/internal/sqlgen"
	"go.uber.org/zap"
)

func main() {
	// Load configuration
	cfg, err := config.LoadConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load configuration: %v\n", err)
		os.Exit(1)
	}

	// Initialize logger
	logger, err := fislog.NewLogger("/tmp", "migration", false, false)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}

	logger.Info("Starting migration tool",
		zap.Int("tenant_id", cfg.TenantID),
		zap.String("table_name", cfg.TableName))

	// Generate segments
	segments, err := segment.SegmentHashSpace(cfg.Segments)
	if err != nil {
		logger.Error("Failed to generate segments", zap.Error(err))
		os.Exit(1)
	}

	logger.Info("Generated segments",
		zap.Int("count", len(segments)),
		zap.Int("max_parallel", cfg.MaxParallelSegs))

	// Process segments (export + upload)
	csvFiles, err := migration.ProcessSegments(segments, cfg, logger)
	if err != nil {
		logger.Error("Failed to process segments", zap.Error(err))
		os.Exit(1)
	}

	logger.Info("All segments processed",
		zap.Int("total_csv_files", len(csvFiles)))

	// Generate SQL file and upload to S3
	s3Uploader, err := s3.NewUploader(cfg, logger)
	if err != nil {
		logger.Error("Failed to create S3 uploader for SQL", zap.Error(err))
		os.Exit(1)
	}

	sqlS3Key, err := sqlgen.GenerateAndUploadSQL(csvFiles, cfg, s3Uploader, logger)
	if err != nil {
		logger.Error("Failed to generate and upload SQL file", zap.Error(err))
		os.Exit(1)
	}

	logger.Info("SQL file generated and uploaded to S3",
		zap.String("s3_key", sqlS3Key))

	// Execute SQL if requested
	if cfg.ExecuteSQL {
		logger.Info("Executing LOAD DATA FROM S3 on Aurora MySQL")

		sqlStatements, err := sqlgen.GenerateLoadDataSQL(csvFiles, cfg)
		if err != nil {
			logger.Error("Failed to generate SQL statements", zap.Error(err))
			os.Exit(1)
		}

		if err := sqlgen.ExecuteLoadDataSQL(sqlStatements, cfg, logger); err != nil {
			logger.Error("Failed to execute SQL statements", zap.Error(err))
			// Don't exit on error - log it but continue
			logger.Warn("Some SQL statements may have failed, check logs above")
		} else {
			logger.Info("All SQL statements executed successfully")
		}
	}

	// Print summary
	totalRows := 0
	for _, csvFile := range csvFiles {
		totalRows += csvFile.RowCount
	}

	fmt.Printf("\n=== Migration Summary ===\n")
	fmt.Printf("Tenant ID: %d\n", cfg.TenantID)
	fmt.Printf("Table: %s\n", cfg.TableName)
	fmt.Printf("Total rows exported: %d\n", totalRows)
	fmt.Printf("Total CSV files: %d\n", len(csvFiles))
	fmt.Printf("S3 bucket: %s\n", cfg.S3Bucket)
	fmt.Printf("S3 prefix: %s\n", cfg.S3Prefix)
	fmt.Printf("SQL file S3 key: %s\n", sqlS3Key)

	// Print CSV file S3 keys
	if len(csvFiles) > 0 {
		fmt.Printf("\nCSV files uploaded to S3:\n")
		if len(csvFiles) <= 10 {
			// Print all if 10 or fewer
			for i, csvFile := range csvFiles {
				fmt.Printf("  %d. s3://%s/%s (%d rows)\n", i+1, cfg.S3Bucket, csvFile.S3Key, csvFile.RowCount)
			}
		} else {
			// Print first 5 and last 5 if more than 10
			for i := 0; i < 5; i++ {
				fmt.Printf("  %d. s3://%s/%s (%d rows)\n", i+1, cfg.S3Bucket, csvFiles[i].S3Key, csvFiles[i].RowCount)
			}
			fmt.Printf("  ... (%d more files) ...\n", len(csvFiles)-10)
			for i := len(csvFiles) - 5; i < len(csvFiles); i++ {
				fmt.Printf("  %d. s3://%s/%s (%d rows)\n", i+1, cfg.S3Bucket, csvFiles[i].S3Key, csvFiles[i].RowCount)
			}
		}
		fmt.Printf("\nTo verify all CSV files in S3:\n")
		fmt.Printf("  aws s3 ls s3://%s/%s/tenant-%d/%s/ --recursive --region %s\n",
			cfg.S3Bucket, cfg.S3Prefix, cfg.TenantID, cfg.TableName, cfg.AWSRegion)
	}
	if cfg.ExecuteSQL {
		fmt.Printf("SQL execution: Completed\n")
	} else {
		fmt.Printf("SQL execution: Skipped (use -execute-sql to enable)\n")
		// Only print "Next Steps" if not in quiet mode
		if !cfg.Quiet {
			fmt.Printf("\n")
			fmt.Printf("=== Next Steps: Execute SQL on EC2 ===\n")
			fmt.Printf("The SQL file has been uploaded to S3. To load data into Aurora MySQL:\n")
			fmt.Printf("\n")
			fmt.Printf("1. Download SQL file from S3:\n")
			fmt.Printf("   aws s3 cp s3://%s/%s ./load-data-tenant-%d.sql\n", cfg.S3Bucket, sqlS3Key, cfg.TenantID)
			fmt.Printf("\n")
			fmt.Printf("2. Connect to Aurora MySQL (on EC2 or locally):\n")
			if cfg.AuroraHost != "" {
				fmt.Printf("   mysql -h %s", cfg.AuroraHost)
				if cfg.AuroraPort > 0 && cfg.AuroraPort != 3306 {
					fmt.Printf(" -P %d", cfg.AuroraPort)
				}
				fmt.Printf(" -u %s", cfg.AuroraUser)
				if cfg.AuroraDatabase != "" {
					fmt.Printf(" -D %s", cfg.AuroraDatabase)
				}
				fmt.Printf("\n")
			} else {
				fmt.Printf("   mysql -h <aurora-host> -u <user> -D <database>\n")
			}
			fmt.Printf("\n")
			fmt.Printf("3. Execute SQL file:\n")
			fmt.Printf("   source ./load-data-tenant-%d.sql\n", cfg.TenantID)
			fmt.Printf("   # OR\n")
			fmt.Printf("   mysql ... < ./load-data-tenant-%d.sql\n", cfg.TenantID)
			fmt.Printf("\n")
			fmt.Printf("⚠️  IMPORTANT: Aurora MySQL IAM Role Required\n")
			fmt.Printf("   Before executing SQL, ensure Aurora MySQL cluster has IAM role configured:\n")
			fmt.Printf("   - Parameter: aurora_load_from_s3_role or aws_default_s3_role\n")
			fmt.Printf("   - IAM role must have S3 read permissions for bucket: %s\n", cfg.S3Bucket)
			fmt.Printf("   - See README.md for detailed IAM role setup instructions\n")
			fmt.Printf("   - Error 63985 indicates IAM role is not configured\n")
			fmt.Printf("\n")
			fmt.Printf("=======================\n")
		}
	}
	if !cfg.Quiet {
		fmt.Printf("=======================\n")
	}

	logger.Info("Migration completed successfully")
}
