// Copyright (c) 2024 Netskope, Inc. All rights reserved.

package config

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strconv"

	"gopkg.in/yaml.v3"
)

// Config holds all configuration for the migration tool.
type Config struct {
	// Tenant & Table
	TenantID  int
	TableName string

	// MariaDB Connection
	MariaDBHost     string
	MariaDBPort     int
	MariaDBUser     string
	MariaDBPassword string
	MariaDBDatabase string

	// S3 Configuration
	S3Bucket  string
	S3Prefix  string
	AWSRegion string

	// Optional: Aurora connection for SQL execution
	AuroraHost                 string
	AuroraPort                 int
	AuroraUser                 string
	AuroraSecretsManagerSecret string // AWS Secrets Manager secret name (e.g., "rds!cluster-xxx")
	AuroraRegion               string // AWS region for Secrets Manager
	AuroraDatabase             string
	ExecuteSQL                 bool // Flag to execute LOAD DATA FROM S3

	// Segmentation & Parallelism
	Segments        int // Default: 16
	MaxParallelSegs int // Default: 8
	BatchSize       int // Default: 100000

	// CSV Options
	CSVDelimiter string // Default: ","
	CSVQuote     string // Default: "\""

	// SQL Execution Timeout (seconds)
	SQLExecTimeout int // Default: 300 (5 minutes)

	// Output Control
	Quiet bool // Suppress "Next Steps" instructions (useful when run via script)
}

// LoadConfig loads configuration from CLI flags, environment variables, and YAML file.
// Priority: CLI flags > environment variables > YAML file > defaults
func LoadConfig() (*Config, error) {
	cfg := &Config{}

	// CLI flags
	tenantID := flag.Int("tenant-id", 0, "Tenant ID to migrate")
	tableName := flag.String("table-name", "fis_aggr", "Table name (default: fis_aggr)")
	mariadbHost := flag.String("mariadb-host", "", "MariaDB host:port")
	mariadbPort := flag.Int("mariadb-port", 3306, "MariaDB port (default: 3306)")
	mariadbUser := flag.String("mariadb-user", "", "MariaDB username")
	mariadbPassword := flag.String("mariadb-password", "", "MariaDB password")
	mariadbAuth := flag.String("mariadb-auth", "", "MariaDB auth file path (JSON with user and password)")
	mariadbDatabase := flag.String("mariadb-database", "fis", "MariaDB database name (default: fis)")
	s3Bucket := flag.String("s3-bucket", "", "S3 bucket name")
	s3Prefix := flag.String("s3-prefix", "fis-migration", "S3 key prefix (default: fis-migration)")
	awsRegion := flag.String("aws-region", "", "AWS region")
	segments := flag.Int("segments", 16, "Number of hash segments (default: 16)")
	maxParallelSegs := flag.Int("max-parallel-segments", 8, "Max parallel segments (default: 8)")
	batchSize := flag.Int("batch-size", 100000, "Batch size for pagination (default: 100000)")
	configFile := flag.String("config-file", "migration-config.yaml", "Config file path (default: migration-config.yaml)")

	// Aurora connection for SQL execution
	auroraHost := flag.String("aurora-host", "", "Aurora MySQL endpoint (optional)")
	auroraPort := flag.Int("aurora-port", 3306, "Aurora MySQL port (default: 3306)")
	auroraUser := flag.String("aurora-user", "", "Aurora MySQL username")
	auroraSecret := flag.String("aurora-secret", "", "AWS Secrets Manager secret name (e.g., rds!cluster-xxx)")
	auroraRegion := flag.String("aurora-region", "", "AWS region for Secrets Manager (e.g., us-east-1)")
	auroraDatabase := flag.String("aurora-database", "fis", "Aurora MySQL database name (default: fis)")
	executeSQL := flag.Bool("execute-sql", false, "Execute LOAD DATA FROM S3 after generating SQL")
	sqlExecTimeout := flag.Int("sql-exec-timeout", 300, "SQL execution timeout in seconds (default: 300)")
	quiet := flag.Bool("quiet", false, "Suppress 'Next Steps' instructions (useful when run via script)")

	flag.Parse()

	// Load from YAML file if it exists
	if *configFile != "" {
		if err := loadFromYAML(cfg, *configFile); err != nil && !os.IsNotExist(err) {
			return nil, fmt.Errorf("failed to load config file: %w", err)
		}
	}

	// Override with environment variables
	loadFromEnv(cfg)

	// Override with CLI flags (highest priority)
	if *tenantID > 0 {
		cfg.TenantID = *tenantID
	}
	if *tableName != "" {
		cfg.TableName = *tableName
	}
	if *mariadbHost != "" {
		cfg.MariaDBHost = *mariadbHost
	}
	if *mariadbPort > 0 {
		cfg.MariaDBPort = *mariadbPort
	}
	if *mariadbUser != "" {
		cfg.MariaDBUser = *mariadbUser
	}
	if *mariadbPassword != "" {
		cfg.MariaDBPassword = *mariadbPassword
	}
	if *mariadbAuth != "" {
		if err := cfg.ReadMariaDBAuth(*mariadbAuth); err != nil {
			return nil, fmt.Errorf("failed to read MariaDB auth file: %w", err)
		}
	}
	if *mariadbDatabase != "" {
		cfg.MariaDBDatabase = *mariadbDatabase
	}
	if *s3Bucket != "" {
		cfg.S3Bucket = *s3Bucket
	}
	if *s3Prefix != "" {
		cfg.S3Prefix = *s3Prefix
	}
	if *awsRegion != "" {
		cfg.AWSRegion = *awsRegion
	}
	if *segments > 0 {
		cfg.Segments = *segments
	}
	if *maxParallelSegs > 0 {
		cfg.MaxParallelSegs = *maxParallelSegs
	}
	if *batchSize > 0 {
		cfg.BatchSize = *batchSize
	}
	if *auroraHost != "" {
		cfg.AuroraHost = *auroraHost
	}
	if *auroraPort > 0 {
		cfg.AuroraPort = *auroraPort
	}
	if *auroraUser != "" {
		cfg.AuroraUser = *auroraUser
	}
	if *auroraSecret != "" {
		cfg.AuroraSecretsManagerSecret = *auroraSecret
	}
	if *auroraRegion != "" {
		cfg.AuroraRegion = *auroraRegion
	}
	if *auroraDatabase != "" {
		cfg.AuroraDatabase = *auroraDatabase
	}
	if *executeSQL {
		cfg.ExecuteSQL = true
	}
	if *sqlExecTimeout > 0 {
		cfg.SQLExecTimeout = *sqlExecTimeout
	}
	if *quiet {
		cfg.Quiet = true
	}

	// Set defaults
	if cfg.Segments == 0 {
		cfg.Segments = 16
	}
	if cfg.MaxParallelSegs == 0 {
		cfg.MaxParallelSegs = 8
	}
	if cfg.BatchSize == 0 {
		cfg.BatchSize = 100000
	}
	if cfg.CSVDelimiter == "" {
		cfg.CSVDelimiter = ","
	}
	if cfg.CSVQuote == "" {
		cfg.CSVQuote = "\""
	}
	if cfg.MariaDBDatabase == "" {
		cfg.MariaDBDatabase = "fis"
	}
	if cfg.AuroraDatabase == "" {
		cfg.AuroraDatabase = "fis"
	}
	if cfg.AuroraPort == 0 {
		cfg.AuroraPort = 3306
	}
	if cfg.MariaDBPort == 0 {
		cfg.MariaDBPort = 3306
	}
	if cfg.SQLExecTimeout == 0 {
		cfg.SQLExecTimeout = 300
	}

	// Validate required fields
	if cfg.TenantID <= 0 {
		return nil, fmt.Errorf("tenant-id is required")
	}
	if cfg.TableName == "" {
		return nil, fmt.Errorf("table-name is required")
	}
	if cfg.MariaDBHost == "" {
		return nil, fmt.Errorf("mariadb-host is required")
	}
	if cfg.S3Bucket == "" {
		return nil, fmt.Errorf("s3-bucket is required")
	}
	if cfg.AWSRegion == "" {
		return nil, fmt.Errorf("aws-region is required")
	}

	// Validate Aurora connection if execute-sql is set
	if cfg.ExecuteSQL {
		if cfg.AuroraHost == "" {
			return nil, fmt.Errorf("aurora-host is required when -execute-sql is set")
		}
		if cfg.AuroraUser == "" {
			return nil, fmt.Errorf("aurora-user is required when -execute-sql is set")
		}
		if cfg.AuroraSecretsManagerSecret == "" {
			return nil, fmt.Errorf("aurora-secret is required when -execute-sql is set")
		}
		if cfg.AuroraRegion == "" {
			return nil, fmt.Errorf("aurora-region is required when -execute-sql is set")
		}
	}

	return cfg, nil
}

// loadFromYAML loads configuration from a YAML file.
func loadFromYAML(cfg *Config, filepath string) error {
	data, err := os.ReadFile(filepath)
	if err != nil {
		return err
	}

	var yamlCfg struct {
		TenantID                   int    `yaml:"tenant_id"`
		TableName                  string `yaml:"table_name"`
		MariaDBHost                string `yaml:"mariadb_host"`
		MariaDBPort                int    `yaml:"mariadb_port"`
		MariaDBUser                string `yaml:"mariadb_user"`
		MariaDBPassword            string `yaml:"mariadb_password"`
		MariaDBDatabase            string `yaml:"mariadb_database"`
		S3Bucket                   string `yaml:"s3_bucket"`
		S3Prefix                   string `yaml:"s3_prefix"`
		AWSRegion                  string `yaml:"aws_region"`
		AuroraHost                 string `yaml:"aurora_host"`
		AuroraPort                 int    `yaml:"aurora_port"`
		AuroraUser                 string `yaml:"aurora_user"`
		AuroraSecretsManagerSecret string `yaml:"aurora_secret"`
		AuroraRegion               string `yaml:"aurora_region"`
		AuroraDatabase             string `yaml:"aurora_database"`
		ExecuteSQL                 bool   `yaml:"execute_sql"`
		Segments                   int    `yaml:"segments"`
		MaxParallelSegs            int    `yaml:"max_parallel_segments"`
		BatchSize                  int    `yaml:"batch_size"`
		SQLExecTimeout             int    `yaml:"sql_exec_timeout"`
	}

	if err := yaml.Unmarshal(data, &yamlCfg); err != nil {
		return err
	}

	// Apply YAML values to config (only if not already set)
	if yamlCfg.TenantID > 0 {
		cfg.TenantID = yamlCfg.TenantID
	}
	if yamlCfg.TableName != "" {
		cfg.TableName = yamlCfg.TableName
	}
	if yamlCfg.MariaDBHost != "" {
		cfg.MariaDBHost = yamlCfg.MariaDBHost
	}
	if yamlCfg.MariaDBPort > 0 {
		cfg.MariaDBPort = yamlCfg.MariaDBPort
	}
	if yamlCfg.MariaDBUser != "" {
		cfg.MariaDBUser = yamlCfg.MariaDBUser
	}
	if yamlCfg.MariaDBPassword != "" {
		cfg.MariaDBPassword = yamlCfg.MariaDBPassword
	}
	if yamlCfg.MariaDBDatabase != "" {
		cfg.MariaDBDatabase = yamlCfg.MariaDBDatabase
	}
	if yamlCfg.S3Bucket != "" {
		cfg.S3Bucket = yamlCfg.S3Bucket
	}
	if yamlCfg.S3Prefix != "" {
		cfg.S3Prefix = yamlCfg.S3Prefix
	}
	if yamlCfg.AWSRegion != "" {
		cfg.AWSRegion = yamlCfg.AWSRegion
	}
	if yamlCfg.AuroraHost != "" {
		cfg.AuroraHost = yamlCfg.AuroraHost
	}
	if yamlCfg.AuroraPort > 0 {
		cfg.AuroraPort = yamlCfg.AuroraPort
	}
	if yamlCfg.AuroraUser != "" {
		cfg.AuroraUser = yamlCfg.AuroraUser
	}
	if yamlCfg.AuroraSecretsManagerSecret != "" {
		cfg.AuroraSecretsManagerSecret = yamlCfg.AuroraSecretsManagerSecret
	}
	if yamlCfg.AuroraRegion != "" {
		cfg.AuroraRegion = yamlCfg.AuroraRegion
	}
	if yamlCfg.AuroraDatabase != "" {
		cfg.AuroraDatabase = yamlCfg.AuroraDatabase
	}
	cfg.ExecuteSQL = yamlCfg.ExecuteSQL
	if yamlCfg.Segments > 0 {
		cfg.Segments = yamlCfg.Segments
	}
	if yamlCfg.MaxParallelSegs > 0 {
		cfg.MaxParallelSegs = yamlCfg.MaxParallelSegs
	}
	if yamlCfg.BatchSize > 0 {
		cfg.BatchSize = yamlCfg.BatchSize
	}
	if yamlCfg.SQLExecTimeout > 0 {
		cfg.SQLExecTimeout = yamlCfg.SQLExecTimeout
	}

	return nil
}

// loadFromEnv loads configuration from environment variables.
func loadFromEnv(cfg *Config) {
	if val := os.Getenv("FIS_MIGRATION_TENANT_ID"); val != "" {
		if tid, err := strconv.Atoi(val); err == nil {
			cfg.TenantID = tid
		}
	}
	if val := os.Getenv("FIS_MIGRATION_TABLE_NAME"); val != "" {
		cfg.TableName = val
	}
	if val := os.Getenv("FIS_MIGRATION_MARIADB_HOST"); val != "" {
		cfg.MariaDBHost = val
	}
	if val := os.Getenv("FIS_MIGRATION_MARIADB_PORT"); val != "" {
		if port, err := strconv.Atoi(val); err == nil {
			cfg.MariaDBPort = port
		}
	}
	if val := os.Getenv("FIS_MIGRATION_MARIADB_USER"); val != "" {
		cfg.MariaDBUser = val
	}
	if val := os.Getenv("FIS_MIGRATION_MARIADB_PASSWORD"); val != "" {
		cfg.MariaDBPassword = val
	}
	if val := os.Getenv("FIS_MIGRATION_MARIADB_DATABASE"); val != "" {
		cfg.MariaDBDatabase = val
	}
	if val := os.Getenv("FIS_MIGRATION_S3_BUCKET"); val != "" {
		cfg.S3Bucket = val
	}
	if val := os.Getenv("FIS_MIGRATION_S3_PREFIX"); val != "" {
		cfg.S3Prefix = val
	}
	if val := os.Getenv("FIS_MIGRATION_AWS_REGION"); val != "" {
		cfg.AWSRegion = val
	}
	if val := os.Getenv("FIS_MIGRATION_AURORA_HOST"); val != "" {
		cfg.AuroraHost = val
	}
	if val := os.Getenv("FIS_MIGRATION_AURORA_PORT"); val != "" {
		if port, err := strconv.Atoi(val); err == nil {
			cfg.AuroraPort = port
		}
	}
	if val := os.Getenv("FIS_MIGRATION_AURORA_USER"); val != "" {
		cfg.AuroraUser = val
	}
	if val := os.Getenv("FIS_MIGRATION_AURORA_SECRET"); val != "" {
		cfg.AuroraSecretsManagerSecret = val
	}
	if val := os.Getenv("FIS_MIGRATION_AURORA_REGION"); val != "" {
		cfg.AuroraRegion = val
	}
	if val := os.Getenv("FIS_MIGRATION_AURORA_DATABASE"); val != "" {
		cfg.AuroraDatabase = val
	}
	if val := os.Getenv("FIS_MIGRATION_EXECUTE_SQL"); val != "" {
		cfg.ExecuteSQL = (val == "true" || val == "1")
	}
	if val := os.Getenv("FIS_MIGRATION_SEGMENTS"); val != "" {
		if segs, err := strconv.Atoi(val); err == nil {
			cfg.Segments = segs
		}
	}
	if val := os.Getenv("FIS_MIGRATION_MAX_PARALLEL_SEGMENTS"); val != "" {
		if max, err := strconv.Atoi(val); err == nil {
			cfg.MaxParallelSegs = max
		}
	}
	if val := os.Getenv("FIS_MIGRATION_BATCH_SIZE"); val != "" {
		if batch, err := strconv.Atoi(val); err == nil {
			cfg.BatchSize = batch
		}
	}
	if val := os.Getenv("FIS_MIGRATION_SQL_EXEC_TIMEOUT"); val != "" {
		if timeout, err := strconv.Atoi(val); err == nil {
			cfg.SQLExecTimeout = timeout
		}
	}
}

// GetMariaDBDSN returns the MariaDB connection string.
func (c *Config) GetMariaDBDSN() string {
	host := c.MariaDBHost
	if c.MariaDBPort > 0 && c.MariaDBPort != 3306 {
		host = fmt.Sprintf("%s:%d", c.MariaDBHost, c.MariaDBPort)
	}

	dsn := fmt.Sprintf("tcp(%s)/%s?parseTime=true", host, c.MariaDBDatabase)
	if c.MariaDBUser != "" {
		if c.MariaDBPassword != "" {
			dsn = fmt.Sprintf("%s:%s@%s", c.MariaDBUser, c.MariaDBPassword, dsn)
		} else {
			dsn = fmt.Sprintf("%s@%s", c.MariaDBUser, dsn)
		}
	}
	return dsn
}

// ReadMariaDBAuth reads MariaDB credentials from an auth file (JSON format).
func (c *Config) ReadMariaDBAuth(authFile string) error {
	if authFile == "" {
		return nil
	}

	data, err := os.ReadFile(authFile)
	if err != nil {
		return fmt.Errorf("failed to read auth file: %w", err)
	}

	var auth struct {
		User     string `json:"user"`
		Password string `json:"password"`
	}

	if err := json.Unmarshal(data, &auth); err != nil {
		return fmt.Errorf("failed to parse auth file: %w", err)
	}

	c.MariaDBUser = auth.User
	c.MariaDBPassword = auth.Password
	return nil
}
