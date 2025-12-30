// Copyright (c) 2024 Netskope, Inc. All rights reserved.

package config

import (
	"os"
	"testing"
)

func TestLoadConfig_EnvironmentVariables(t *testing.T) {
	// Set environment variables
	os.Setenv("FIS_MIGRATION_TENANT_ID", "1234")
	os.Setenv("FIS_MIGRATION_TABLE_NAME", "test_table")
	os.Setenv("FIS_MIGRATION_MARIADB_HOST", "localhost:3306")
	os.Setenv("FIS_MIGRATION_S3_BUCKET", "test-bucket")
	os.Setenv("FIS_MIGRATION_AWS_REGION", "us-east-1")
	defer func() {
		os.Unsetenv("FIS_MIGRATION_TENANT_ID")
		os.Unsetenv("FIS_MIGRATION_TABLE_NAME")
		os.Unsetenv("FIS_MIGRATION_MARIADB_HOST")
		os.Unsetenv("FIS_MIGRATION_S3_BUCKET")
		os.Unsetenv("FIS_MIGRATION_AWS_REGION")
	}()

	// Note: LoadConfig() uses flag.Parse() which requires actual CLI args
	// For full testing, we'd need to reset flags or use a different approach
	// This is a placeholder for the test structure
}

func TestConfig_GetMariaDBDSN(t *testing.T) {
	tests := []struct {
		name     string
		config   *Config
		wantDSN  string
		contains []string // strings that should be in DSN
	}{
		{
			name: "with user and password",
			config: &Config{
				MariaDBHost:     "localhost",
				MariaDBPort:     3306,
				MariaDBUser:     "testuser",
				MariaDBPassword: "testpass",
				MariaDBDatabase: "testdb",
			},
			contains: []string{"testuser", "testpass", "testdb", "localhost"},
		},
		{
			name: "with custom port",
			config: &Config{
				MariaDBHost:     "localhost",
				MariaDBPort:     3307,
				MariaDBUser:     "testuser",
				MariaDBPassword: "testpass",
				MariaDBDatabase: "testdb",
			},
			contains: []string{"localhost:3307"},
		},
		{
			name: "without password",
			config: &Config{
				MariaDBHost:     "localhost",
				MariaDBPort:     3306,
				MariaDBUser:     "testuser",
				MariaDBDatabase: "testdb",
			},
			contains: []string{"testuser", "testdb"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsn := tt.config.GetMariaDBDSN()
			for _, substr := range tt.contains {
				if !contains(dsn, substr) {
					t.Errorf("DSN should contain %q, got %q", substr, dsn)
				}
			}
		})
	}
}

func TestConfig_ReadMariaDBAuth(t *testing.T) {
	// Create a temporary auth file
	tmpFile, err := os.CreateTemp("", "auth-*.json")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	authJSON := `{"user": "testuser", "password": "testpass"}`
	if _, err := tmpFile.WriteString(authJSON); err != nil {
		t.Fatalf("failed to write auth file: %v", err)
	}
	tmpFile.Close()

	cfg := &Config{}
	if err := cfg.ReadMariaDBAuth(tmpFile.Name()); err != nil {
		t.Errorf("ReadMariaDBAuth() error = %v", err)
	}

	if cfg.MariaDBUser != "testuser" {
		t.Errorf("expected user testuser, got %s", cfg.MariaDBUser)
	}
	if cfg.MariaDBPassword != "testpass" {
		t.Errorf("expected password testpass, got %s", cfg.MariaDBPassword)
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 || 
		(len(s) > len(substr) && (s[:len(substr)] == substr || 
		s[len(s)-len(substr):] == substr || 
		containsMiddle(s, substr))))
}

func containsMiddle(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

