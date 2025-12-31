// Copyright (c) 2024 Netskope, Inc. All rights reserved.

package util

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
)

// AWS IAM credential file paths (vault-injected for Aurora MySQL)
const (
	DefaultAWSAuroraKeyFile  = "/vault/secrets/awsauroramysqlkey"
	DefaultAWSAuroraPassFile = "/vault/secrets/awsauroramysqlpass"

	// AWSSQLPasswordEnv allows bypassing Secrets Manager lookups (e.g., smoketests/local).
	// When set (even to an empty string), ResolveAWSDBPassword returns the value directly.
	AWSSQLPasswordEnv = "FIS_AWS_SQL_PASSWORD" //nolint:gosec // env var name, not a credential
)

// LoadAWSCredentials loads AWS IAM credentials with the following priority:
// 1. CLI flags (accessKeyID, secretAccessKey, sessionToken) - highest priority
// 2. Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN)
// 3. AWS SDK default chain (AWS CLI credentials, SSO cache, IAM roles, etc.)
// 4. Vault files (/vault/secrets/awsauroramysqlkey, /vault/secrets/awsauroramysqlpass) - fallback
//
// Critical: Only sets environment variables if CLI flags are explicitly provided.
// If no CLI flags, this function does NOT set any environment variables, allowing
// AWS SDK to use its full default credential chain (SSO, profiles, IAM roles, etc.).
func LoadAWSCredentials(accessKeyID, secretAccessKey, sessionToken string) {
	// Priority 1: CLI flags (if provided, set as environment variables)
	// Only set env vars if at least access key and secret key are provided via CLI
	if accessKeyID != "" && secretAccessKey != "" {
		_ = os.Setenv("AWS_ACCESS_KEY_ID", accessKeyID)
		_ = os.Setenv("AWS_SECRET_ACCESS_KEY", secretAccessKey)
		// Session token is optional - only set if provided
		if sessionToken != "" {
			_ = os.Setenv("AWS_SESSION_TOKEN", sessionToken)
		}
		return
	}

	// Priority 2: Check if already set via environment variables
	// If env vars are already set, don't interfere - let AWS SDK use them
	if os.Getenv("AWS_ACCESS_KEY_ID") != "" && os.Getenv("AWS_SECRET_ACCESS_KEY") != "" {
		return
	}

	// Priority 3: AWS SDK default chain will be used automatically if no CLI flags and no env vars
	// This includes:
	// - AWS CLI credentials (~/.aws/credentials, ~/.aws/config)
	// - AWS SSO cached credentials (~/.aws/sso/cache/ after aws sso login)
	// - IAM roles (if running on EC2)
	// - Other credential providers in the default chain
	// No action needed here - AWS SDK handles this automatically

	// Priority 4: Fallback to vault files (for backward compatibility with Kubernetes deployments)
	// Only if no CLI flags, no env vars, and AWS SDK default chain is not available
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

	ctx := context.Background()
	awsCfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(region),
	)
	if err != nil {
		return "", fmt.Errorf("create AWS config: %w", err)
	}

	svc := secretsmanager.NewFromConfig(awsCfg)
	out, err := svc.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{
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
