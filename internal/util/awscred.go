// Copyright (c) 2024 Netskope, Inc. All rights reserved.

package util

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/secretsmanager"
)

// AWS IAM credential file paths (vault-injected for Aurora MySQL)
const (
	DefaultAWSAuroraKeyFile  = "/vault/secrets/awsauroramysqlkey"
	DefaultAWSAuroraPassFile = "/vault/secrets/awsauroramysqlpass"

	// AWSSQLPasswordEnv allows bypassing Secrets Manager lookups (e.g., smoketests/local).
	// When set (even to an empty string), ResolveAWSDBPassword returns the value directly.
	AWSSQLPasswordEnv = "FIS_AWS_SQL_PASSWORD" //nolint:gosec // env var name, not a credential
)

// LoadAWSCredentialsFromVault loads AWS IAM credentials from vault-injected files.
// Env vars take precedence if already set, then vault files are tried.
// These credentials are used to authenticate with AWS Secrets Manager.
func LoadAWSCredentialsFromVault() {
	// Check if already set via environment
	if os.Getenv("AWS_ACCESS_KEY_ID") != "" && os.Getenv("AWS_SECRET_ACCESS_KEY") != "" {
		return
	}

	// Try to load from vault files (Aurora-specific paths)
	if os.Getenv("AWS_ACCESS_KEY_ID") == "" {
		if content, err := os.ReadFile(DefaultAWSAuroraKeyFile); err == nil {
			_ = os.Setenv("AWS_ACCESS_KEY_ID", strings.TrimSpace(string(content)))
		}
	}

	if os.Getenv("AWS_SECRET_ACCESS_KEY") == "" {
		if content, err := os.ReadFile(DefaultAWSAuroraPassFile); err == nil {
			_ = os.Setenv("AWS_SECRET_ACCESS_KEY", strings.TrimSpace(string(content)))
		}
	}
}

// GetPasswordFromSecretsManager retrieves the database password from AWS Secrets Manager.
// The secret JSON is expected to contain a "password" field.
func GetPasswordFromSecretsManager(secretName, region string) (string, error) {
	if secretName == "" {
		return "", fmt.Errorf("secret name is required for Secrets Manager")
	}
	if region == "" {
		return "", fmt.Errorf("region is required for Secrets Manager")
	}

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(region),
	})
	if err != nil {
		return "", fmt.Errorf("create AWS session: %w", err)
	}

	svc := secretsmanager.New(sess)
	out, err := svc.GetSecretValue(&secretsmanager.GetSecretValueInput{
		SecretId:     aws.String(secretName),
		VersionStage: aws.String("AWSCURRENT"),
	})
	if err != nil {
		return "", fmt.Errorf("get secret value: %w", err)
	}
	if out.SecretString == nil {
		return "", fmt.Errorf("secret string empty for %s", secretName)
	}

	var payload struct {
		Password string `json:"password"`
	}
	if err := json.Unmarshal([]byte(*out.SecretString), &payload); err != nil {
		return "", fmt.Errorf("parse secret json: %w", err)
	}
	if payload.Password == "" {
		return "", fmt.Errorf("password field empty in secret %s", secretName)
	}

	return payload.Password, nil
}

// ResolveAWSDBPassword returns the AWS DB password. If AWSSQLPasswordEnv is set
// (even to an empty string), that value is returned. Otherwise, the password is
// fetched from AWS Secrets Manager using the provided secret and region.
func ResolveAWSDBPassword(secretName, region string) (string, error) {
	if pwd, ok := os.LookupEnv(AWSSQLPasswordEnv); ok {
		return pwd, nil
	}
	return GetPasswordFromSecretsManager(secretName, region)
}

