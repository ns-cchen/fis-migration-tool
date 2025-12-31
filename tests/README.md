# Go Test Suite

This directory contains Go test files that can be run using `go test`. The tests use testcontainers to automatically set up LocalStack and MariaDB for testing.

## Running Tests

### Run All Tests

```bash
go test ./tests -v
```

### Run Specific Tests

```bash
# Run only AWS credential tests
go test ./tests -v -run Test.*CLI

# Run a specific test
go test ./tests -v -run Test1CLIFlagsSessionToken
```

**Note:** Tests always use testcontainers compose module to run `docker-compose.test.yml`, which starts LocalStack and MariaDB containers. Make sure Docker is running.

## Test Files

- `aws_credentials_test.go`: Tests for AWS credential handling (CLI flags, environment variables, YAML config, AWS CLI, SSO, etc.). LocalStack and MariaDB are automatically set up via testcontainers in `TestMain`.

## Prerequisites

- Docker must be running
- Go 1.24+ installed
- Migration binary must be built (tests will auto-build if not found)

## Benefits of Using `go test`

- Standard Go testing workflow
- Better integration with IDE and CI/CD
- Parallel test execution support
- Built-in test coverage
- No need to manage separate binaries
- Automatic test discovery
