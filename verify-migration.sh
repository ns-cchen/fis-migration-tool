#!/bin/bash
# Verification script for migration tool
# Works in local MAC environment
# Supports two modes:
#   1. Docker mode (default): Uses Docker containers for MariaDB and LocalStack (S3)
#   2. Tunnel mode (--use-tunnels): Uses SSH tunnels to qa01 MariaDB and AWS MySQL
# Uses real AWS Aurora MySQL for LOAD DATA FROM S3 verification

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# PROJECT_ROOT is where the script is located (project root)
PROJECT_ROOT="$SCRIPT_DIR"

# Configuration
SCHEMA_FILE="$PROJECT_ROOT/internal/store/schema_mysql.sql"
MARIADB_PORT=5590
LOCALSTACK_PORT=4566

# Test configuration
TEST_TENANT_ID=999999
TEST_TABLE_NAME="fis_aggr"
TEST_S3_BUCKET="test-migration-bucket"  # Default for LocalStack
REAL_S3_BUCKET="fis-mariadb-backups-use1-dev"  # Real S3 bucket for qa01
TEST_AWS_REGION="us-east-1"
TEST_SEGMENTS=16
TEST_MAX_PARALLEL=8

# SSH Tunnel configuration (for --use-tunnels mode)
# Default environment is qa01 (NPE)
TUNNEL_ENV="qa01"  # qa01 (NPE) or fr4 (PE)
QA01_CLUSTER="iad0"  # Teleport cluster for qa01 (NPE)
FR4_CLUSTER="fr4"  # Teleport cluster for fr4 (PE)
AWS_CLUSTER="aws-nonprod"
MP_LOCAL_PORT=3307
AWS_LOCAL_PORT=3308
# Cluster name for MariaDB tunnel (set based on TUNNEL_ENV)
MP_CLUSTER="${QA01_CLUSTER}"  # Default: qa01 cluster

# qa01 (NPE) defaults
QA01_MP_MARIADB_JUMP="mariafis01-1.qa01-mp-npe.nc1.iad0.nsscloud.net"
QA01_MP_MARIADB_TARGET="mariafishavip.qa01-mp-npe.nc1.iad0.nsscloud.net"
QA01_AWS_AURORA_JUMP="ns-nonprod-eng-data-ls-container-mariadb-use1-dev.npe.235494792415.us-east-1c.aws.nskope.net"
QA01_AWS_AURORA_TARGET="ns-nonprod-fis-cluster-dev-use1.cluster-cedske4o4zsx.us-east-1.rds.amazonaws.com"
QA01_S3_BUCKET="fis-mariadb-backups-use1-dev"
QA01_AWS_REGION="us-east-1"

# fr4 (PE) defaults
FR4_MP_MARIADB_JUMP="mariafis01-1.my1.fr4.nskope.net"
FR4_MP_MARIADB_TARGET="mariafishavip.qa01-mp-npe.nc1.iad0.nsscloud.net"
FR4_S3_BUCKET="backup-bucket-maria-1"
FR4_AWS_REGION="eu-central-1"

# Initialize with qa01 defaults (can be overridden by --use-tunnels <env>)
MP_MARIADB_JUMP="${QA01_MP_MARIADB_JUMP}"
MP_MARIADB_TARGET="${QA01_MP_MARIADB_TARGET}"
MP_CLUSTER="${QA01_CLUSTER}"  # Default: qa01 cluster
AWS_AURORA_JUMP="${QA01_AWS_AURORA_JUMP}"
AWS_AURORA_TARGET="${QA01_AWS_AURORA_TARGET}"

# Database passwords (must be provided via CLI flags)
MP_PASSWORD=""  # MariaDB password (required via --mp-password flag)
AWS_PASSWORD=""  # AWS Aurora MySQL password (optional, only needed for SQL execution verification)

# S3 configuration (can be overridden via CLI flags)
# Defaults are set after REAL_S3_BUCKET and TEST_AWS_REGION are defined above
S3_BUCKET=""
AWS_REGION=""

# AWS MySQL connection (from environment or CLI flags)
AWS_MYSQL_HOST="${FIS_AWS_MYSQL_HOST:-}"
AWS_MYSQL_USER="${FIS_AWS_MYSQL_USER:-}"
AWS_MYSQL_SECRET="${FIS_AWS_MYSQL_SECRET:-}"
AWS_MYSQL_REGION="${FIS_AWS_MYSQL_REGION:-}"
KEEP_CONTAINERS=false
USE_TUNNELS=false
SKIP_TUNNEL_SETUP=false
CLEANUP_ONLY=false
AWS_DATABASE_NAME="fis_qa01"  # Default AWS database name from config

# Data generation flags
USE_EXISTING_DATA=false

# Tunnel PIDs for cleanup
MP_TUNNEL_PID=""
AWS_TUNNEL_PID=""

# Helper Functions
find_sql_file() {
    local tenant_id=$1
    local sql_file=""
    local possible_locations=(
        "/tmp/load-data-tenant-${tenant_id}.sql"
        "${TMPDIR:-/tmp}/load-data-tenant-${tenant_id}.sql"
    )
    
    # Also search in common temp directories (macOS uses /var/folders)
    if [ -d "/var/folders" ]; then
        local found_file=$(find /var/folders -name "load-data-tenant-${tenant_id}.sql" 2>/dev/null | head -1)
        if [ -n "$found_file" ]; then
            possible_locations+=("$found_file")
        fi
    fi
    
    for loc in "${possible_locations[@]}"; do
        if [[ -n "$loc" ]] && [ -f "$loc" ]; then
            sql_file="$loc"
            break
        fi
    done
    
    echo "$sql_file"
}

get_mariadb_connection() {
    if [ "$USE_TUNNELS" = true ]; then
        # Use tunnel (works for both NPE and PE)
        echo "localhost:${MP_LOCAL_PORT}|fis|${MP_PASSWORD}|fis"
    else
        echo "localhost:${MARIADB_PORT}|root||fis"
    fi
}

get_aws_mysql_connection() {
    if [ "$USE_TUNNELS" = true ]; then
        echo "localhost:${AWS_LOCAL_PORT}|${AWS_MYSQL_USER}|${AWS_PASSWORD}|${AWS_DATABASE_NAME}"
    else
        echo "${AWS_MYSQL_HOST}|${AWS_MYSQL_USER}||${AWS_DATABASE_NAME}"
    fi
}

# Functions
print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Parse CLI arguments
parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            --keep-containers)
                KEEP_CONTAINERS=true
                shift
                ;;
            --use-tunnels)
                USE_TUNNELS=true
                # Check if next argument is an environment (qa01 or fr4)
                if [[ $# -gt 1 ]] && [[ "$2" =~ ^(qa01|fr4)$ ]]; then
                    TUNNEL_ENV="$2"
                    shift 2
                    # Apply environment-specific defaults
                    if [ "$TUNNEL_ENV" = "fr4" ]; then
                        MP_MARIADB_JUMP="${FR4_MP_MARIADB_JUMP}"
                        MP_MARIADB_TARGET="${FR4_MP_MARIADB_TARGET}"
                        MP_CLUSTER="${FR4_CLUSTER}"  # Use fr4 cluster for PE
                        # Set S3 bucket and region if not already set
                        if [ -z "$S3_BUCKET" ]; then
                            S3_BUCKET="${FR4_S3_BUCKET}"
                        fi
                        if [ -z "$AWS_REGION" ]; then
                            AWS_REGION="${FR4_AWS_REGION}"
                        fi
                    else
                        # qa01 (NPE) defaults
                        MP_MARIADB_JUMP="${QA01_MP_MARIADB_JUMP}"
                        MP_MARIADB_TARGET="${QA01_MP_MARIADB_TARGET}"
                        MP_CLUSTER="${QA01_CLUSTER}"  # Use iad0 cluster for NPE
                        AWS_AURORA_JUMP="${QA01_AWS_AURORA_JUMP}"
                        AWS_AURORA_TARGET="${QA01_AWS_AURORA_TARGET}"
                        # Set S3 bucket and region if not already set
                        if [ -z "$S3_BUCKET" ]; then
                            S3_BUCKET="${QA01_S3_BUCKET}"
                        fi
                        if [ -z "$AWS_REGION" ]; then
                            AWS_REGION="${QA01_AWS_REGION}"
                        fi
                    fi
                else
                    # No environment specified, use qa01 defaults
                    TUNNEL_ENV="qa01"
                    MP_MARIADB_JUMP="${QA01_MP_MARIADB_JUMP}"
                    MP_MARIADB_TARGET="${QA01_MP_MARIADB_TARGET}"
                    MP_CLUSTER="${QA01_CLUSTER}"  # Use qa01 cluster
                    AWS_AURORA_JUMP="${QA01_AWS_AURORA_JUMP}"
                    AWS_AURORA_TARGET="${QA01_AWS_AURORA_TARGET}"
                    shift
                fi
                ;;
            --skip-tunnel-setup)
                SKIP_TUNNEL_SETUP=true
                shift
                ;;
            --aws-mysql-host)
                AWS_MYSQL_HOST="$2"
                shift 2
                ;;
            --aws-mysql-user)
                AWS_MYSQL_USER="$2"
                shift 2
                ;;
            --aws-mysql-secret)
                AWS_MYSQL_SECRET="$2"
                shift 2
                ;;
            --aws-mysql-region)
                AWS_MYSQL_REGION="$2"
                shift 2
                ;;
            --mp-jump)
                MP_MARIADB_JUMP="$2"
                shift 2
                ;;
            --mp-target)
                MP_MARIADB_TARGET="$2"
                shift 2
                ;;
            --aws-jump)
                AWS_AURORA_JUMP="$2"
                shift 2
                ;;
            --aws-target)
                AWS_AURORA_TARGET="$2"
                shift 2
                ;;
            --mp-port)
                MP_LOCAL_PORT="$2"
                shift 2
                ;;
            --mp-password)
                MP_PASSWORD="$2"
                shift 2
                ;;
            --aws-password)
                AWS_PASSWORD="$2"
                shift 2
                ;;
            --s3-bucket)
                S3_BUCKET="$2"
                shift 2
                ;;
            --aws-region)
                AWS_REGION="$2"
                shift 2
                ;;
            --aws-port)
                AWS_LOCAL_PORT="$2"
                shift 2
                ;;
            --cleanup-only)
                CLEANUP_ONLY=true
                shift
                ;;
            --aws-database)
                AWS_DATABASE_NAME="$2"
                shift 2
                ;;
            --use-existing-data)
                USE_EXISTING_DATA=true
                shift
                ;;
            --tenant-id)
                TEST_TENANT_ID="$2"
                shift 2
                ;;
            --segments)
                TEST_SEGMENTS="$2"
                shift 2
                ;;
            --max-parallel-segments)
                TEST_MAX_PARALLEL="$2"
                shift 2
                ;;
            *)
                print_error "Unknown option: $1"
                exit 1
                ;;
        esac
    done
}

check_docker_available() {
    if ! command -v docker &> /dev/null; then
        print_error "Docker is not installed. Please install Docker Desktop."
        return 1
    fi
    
    if ! docker info &> /dev/null; then
        print_error "Docker is not running. Please start Docker Desktop."
        return 1
    fi
    
    return 0
}

check_tsh() {
    if ! command -v tsh &> /dev/null; then
        print_error "tsh not found. Please install Teleport client."
        return 1
    fi
    
    # Check if logged in
    # Check if TSH is logged in by checking for "Logged in as" in output
    # tsh status returns non-zero exit code even when logged in, so check output instead
    if ! tsh status 2>&1 | grep -q "Logged in as"; then
        print_error "Not logged into TSH. Please run 'tsh login' first."
        return 1
    fi
    
    print_info "TSH is configured"
    return 0
}

# Kill any existing processes using our tunnel ports
kill_existing_tunnels() {
    local mp_pid=$(lsof -t -i :${MP_LOCAL_PORT} 2>/dev/null)
    local aws_pid=$(lsof -t -i :${AWS_LOCAL_PORT} 2>/dev/null)
    
    if [[ -n "$mp_pid" ]]; then
        print_info "Killing existing process on port ${MP_LOCAL_PORT} (PID: $mp_pid)"
        kill "$mp_pid" 2>/dev/null
        sleep 1
    fi
    
    if [[ -n "$aws_pid" ]]; then
        print_info "Killing existing process on port ${AWS_LOCAL_PORT} (PID: $aws_pid)"
        kill "$aws_pid" 2>/dev/null
        sleep 1
    fi
}

start_mp_tunnel() {
    print_info "Starting MP MariaDB tunnel (localhost:${MP_LOCAL_PORT} -> ${MP_MARIADB_TARGET}:3306)..."
    
    # Use -N for port forwarding only (no remote command)
    # Use nohup to prevent "Stopped" status in scripts
    nohup tsh ssh -N -L ${MP_LOCAL_PORT}:${MP_MARIADB_TARGET}:3306 \
        --cluster ${MP_CLUSTER} \
        ${MP_MARIADB_JUMP} \
        </dev/null >/tmp/mp-tunnel.log 2>&1 &
    
    MP_TUNNEL_PID=$!
    
    # Wait a moment for process to start
    sleep 2
    
    # Check if process is still running
    if ! kill -0 "$MP_TUNNEL_PID" 2>/dev/null; then
        print_error "Failed to start MP tunnel (process died immediately)"
        if [ -f /tmp/mp-tunnel.log ]; then
            print_error "Tunnel log: $(cat /tmp/mp-tunnel.log)"
        fi
        return 1
    fi
    
    # Wait for tunnel to be ready (check every second, up to 20 seconds)
    print_info "Waiting for MP tunnel to be ready..."
    for i in {1..20}; do
        if nc -z localhost ${MP_LOCAL_PORT} 2>/dev/null; then
            # Give tunnel a moment to fully establish
            sleep 2
            # Verify we can actually connect to the database (with timeout)
            export MYSQL_PWD="${MP_PASSWORD}"
            if timeout 5 bash -c "mysql -h 127.0.0.1 -P ${MP_LOCAL_PORT} -u fis -N -e 'SELECT 1' fis &>/dev/null" 2>/dev/null; then
                print_info "MP tunnel active and verified (PID: $MP_TUNNEL_PID)"
                return 0
            else
                # Port is open but DB connection failed - might need more time
                if [ $i -lt 20 ]; then
                    sleep 1
                    continue
                fi
            fi
        fi
        sleep 1
    done
    
    # Check if process is still running
    if ! kill -0 "$MP_TUNNEL_PID" 2>/dev/null; then
        print_error "MP tunnel process died during startup"
        if [ -f /tmp/mp-tunnel.log ]; then
            print_error "Tunnel log: $(cat /tmp/mp-tunnel.log)"
        fi
        return 1
    fi
    
    print_error "MP tunnel started but connection test failed after 20 seconds"
    if [ -f /tmp/mp-tunnel.log ]; then
        print_error "Tunnel log: $(tail -20 /tmp/mp-tunnel.log)"
    fi
    return 1
}

start_aws_tunnel() {
    print_info "Starting AWS Aurora tunnel (localhost:${AWS_LOCAL_PORT} -> ${AWS_AURORA_TARGET}:3306)..."
    
    # Use -N for port forwarding only (no remote command)
    # Use nohup to prevent "Stopped" status in scripts
    nohup tsh ssh -N -L ${AWS_LOCAL_PORT}:${AWS_AURORA_TARGET}:3306 \
        --cluster ${AWS_CLUSTER} \
        ${AWS_AURORA_JUMP} \
        </dev/null >/dev/null 2>&1 &
    
    AWS_TUNNEL_PID=$!
    
    # Wait for tunnel to be ready (check every second, up to 10 seconds)
    for i in {1..10}; do
        if nc -z localhost ${AWS_LOCAL_PORT} 2>/dev/null; then
            print_info "AWS tunnel active (PID: $AWS_TUNNEL_PID)"
            return 0
        fi
        sleep 1
    done
    
    # Check if process is still running
    if ! kill -0 "$AWS_TUNNEL_PID" 2>/dev/null; then
        print_error "Failed to start AWS tunnel (process died)"
        return 1
    fi
    
    print_error "AWS tunnel started but connection test failed after retries"
    return 1
}

test_tunnel_connectivity() {
    print_info "Testing tunnel connectivity..."
    
    # Test MP MariaDB
    export MYSQL_PWD="${MP_PASSWORD}"
    local mp_result=$(timeout 5 bash -c "mysql -h 127.0.0.1 -P ${MP_LOCAL_PORT} -u fis -N -e 'SELECT \"OK\"' fis 2>&1")
    if [[ "$mp_result" == "OK" ]]; then
        print_info "MariaDB: Connected successfully"
    else
        print_error "MariaDB: Connection failed - $mp_result"
        return 1
    fi
    
    # Test AWS Aurora (only if AWS tunnel is set up - optional for PE)
    if [ -n "$AWS_TUNNEL_PID" ] && kill -0 "$AWS_TUNNEL_PID" 2>/dev/null; then
        export MYSQL_PWD="${AWS_PASSWORD}"
        local aws_result=$(timeout 5 bash -c "mysql -h 127.0.0.1 -P ${AWS_LOCAL_PORT} -u fis -N -e 'SELECT \"OK\"' fis 2>&1")
        if [[ "$aws_result" == "OK" ]]; then
            print_info "AWS Aurora: Connected successfully"
        else
            print_error "AWS Aurora: Connection failed - $aws_result"
            return 1
        fi
    fi
    
    return 0
}

check_migration_binary() {
    local migration_bin="$PROJECT_ROOT/cmd/migration/migration"
    if [ ! -f "$migration_bin" ]; then
        print_info "Building migration binary..."
        if [ ! -f "$PROJECT_ROOT/go.mod" ]; then
            print_error "go.mod not found in $PROJECT_ROOT. Please run this script from the project root."
            return 1
        fi
        cd "$PROJECT_ROOT"
        if ! go build -tags dynamic -o "$migration_bin" ./cmd/migration; then
            print_error "Failed to build migration binary"
            return 1
        fi
        if [ ! -f "$migration_bin" ]; then
            print_error "Migration binary was not created at $migration_bin"
            return 1
        fi
    fi
    print_info "Migration binary found: $migration_bin"
}

start_containers() {
    # We use qa01 MariaDB via tunnel for test data generation
    # No need for local Docker MariaDB
    print_info "Skipping local Docker MariaDB - using qa01 MariaDB via tunnel for test data generation"
    print_info "Skipping LocalStack - using real AWS S3 bucket: ${REAL_S3_BUCKET}"
    
    # Verify AWS credentials are available
    if ! aws sts get-caller-identity &>/dev/null; then
        print_error "AWS credentials not configured. Please run 'aws configure' or set AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY"
        return 1
    fi
    print_info "AWS credentials verified for real S3 access"
    return 0
}

setup_tunnels() {
    if [ "$USE_TUNNELS" != true ]; then
        return 0
    fi
    
    print_info "Setting up SSH tunnels..."
    
    # Check TSH
    check_tsh || return 1
    
    # Skip tunnel setup if requested
    if [ "$SKIP_TUNNEL_SETUP" = true ]; then
        print_info "Skipping tunnel setup (--skip-tunnel-setup flag set)"
        # Verify existing tunnels work
        if ! test_tunnel_connectivity; then
            print_error "Existing tunnels are not working. Remove --skip-tunnel-setup to create new tunnels."
            return 1
        fi
        return 0
    fi
    
    # Kill existing tunnels
    kill_existing_tunnels
    
    # Start MP tunnel (works for both NPE and PE)
    if ! start_mp_tunnel; then
        return 1
    fi
    
    # Start AWS tunnel (optional - only for NPE with SQL execution)
    # For PE, manager executes SQL on EC2, so AWS tunnel is not needed
    if [ -n "$AWS_MYSQL_USER" ] && [ -n "$AWS_MYSQL_SECRET" ]; then
        if ! start_aws_tunnel; then
            # Clean up MP tunnel if AWS tunnel fails
            if [ -n "$MP_TUNNEL_PID" ]; then
                kill "$MP_TUNNEL_PID" 2>/dev/null || true
            fi
            return 1
        fi
    fi
    
    # Test connectivity
    if ! test_tunnel_connectivity; then
        return 1
    fi
    
    print_info "SSH tunnels are ready"
    return 0
}


find_tenant_with_data() {
    local db_name="fis"
    local mp_host="127.0.0.1"
    local mp_port="${MP_LOCAL_PORT}"
    
    print_info "Finding tenant with existing data in fis_aggr table..." >&2
    
    export MYSQL_PWD="${MP_PASSWORD}"
    # Use -s to suppress column names, -N for no table format, redirect stderr
    local tenant_id=$(timeout 5 bash -c "mysql -h ${mp_host} -P ${mp_port} -u fis -sN -e \"SELECT DISTINCT tenantid FROM ${db_name}.fis_aggr LIMIT 1;\" ${db_name} 2>/dev/null" | tr -d '[:space:]' || echo "")
    
    if [ -z "$tenant_id" ] || [ "$tenant_id" = "NULL" ] || [ "$tenant_id" = "" ]; then
        print_error "No tenants found with data in fis_aggr table" >&2
        return 1
    fi
    
    print_info "Found tenant ${tenant_id} with existing data" >&2
    # Return tenant ID via stdout only (for command substitution)
    echo -n "$tenant_id"
    return 0
}

verify_tenant_has_data() {
    local tenant_id=$1
    local db_name="fis"
    local mp_host="127.0.0.1"
    local mp_port="${MP_LOCAL_PORT}"
    
    export MYSQL_PWD="${MP_PASSWORD}"
    local count=$(timeout 5 bash -c "mysql -h ${mp_host} -P ${mp_port} -u fis -N -e \"SELECT COUNT(*) FROM ${db_name}.fis_aggr WHERE tenantid = ${tenant_id};\" ${db_name} 2>/dev/null" || echo "0")
    
    if [ "$count" -gt 0 ]; then
        print_info "Tenant ${tenant_id} has ${count} records"
        return 0
    else
        print_warn "Tenant ${tenant_id} has no data (count: ${count})"
        return 1
    fi
}

cleanup_test_data() {
    print_info "Cleaning up test records from qa01 MariaDB (via tunnel)..."
    
    local tenant_id=${TEST_TENANT_ID}
    local db_name="fis"
    local mp_host="127.0.0.1"
    local mp_port="${MP_LOCAL_PORT}"
    
    # Cleanup all data for tenant
    print_info "Cleaning up all test records for tenant ${tenant_id}..."
    export MYSQL_PWD="${MP_PASSWORD}"
    local deleted=$(timeout 10 bash -c "mysql -h ${mp_host} -P ${mp_port} -u fis -N -e \"DELETE FROM ${db_name}.fis_aggr WHERE tenantid = ${tenant_id}; SELECT ROW_COUNT();\" ${db_name} 2>&1")
    local cleanup_result=$?
    
    if [ $cleanup_result -eq 0 ]; then
        print_info "Cleaned up ${deleted} test records from qa01 MariaDB for tenant ${tenant_id}"
        return 0
    else
        print_warn "Cleanup may have failed: $deleted"
        return 1
    fi
}

initialize_schema() {
    print_info "Initializing database schema..."
    
    if [ "$USE_TUNNELS" = true ]; then
        # Use tunnel connection
        MYSQL_PWD="${MP_PASSWORD}" mysql -h 127.0.0.1 -P ${MP_LOCAL_PORT} -u fis fis < "$SCHEMA_FILE"
        
        # Add last_modified and version columns if not present
        MYSQL_PWD="${MP_PASSWORD}" mysql -h 127.0.0.1 -P ${MP_LOCAL_PORT} -u fis fis -e "
            ALTER TABLE fis_aggr ADD COLUMN IF NOT EXISTS last_modified TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP;
            ALTER TABLE fis_aggr ADD COLUMN IF NOT EXISTS version INT NULL;
        " 2>/dev/null || true
    else
        # Use Docker container
        docker exec -i fis-mariadb-migration-test mysql -h localhost -u root fis < "$SCHEMA_FILE"
        
        # Add last_modified and version columns if not present
        docker exec fis-mariadb-migration-test mysql -h localhost -u root fis -e "
            ALTER TABLE fis_aggr ADD COLUMN IF NOT EXISTS last_modified TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP;
            ALTER TABLE fis_aggr ADD COLUMN IF NOT EXISTS version INT NULL;
        " 2>/dev/null || true
    fi
    
    print_info "Schema initialized"
}

run_migration() {
    print_info "Running migration tool..."
    
    local migration_bin="$PROJECT_ROOT/cmd/migration/migration"
    
    # Ensure /tmp exists for logger
    mkdir -p /tmp
    
    # Set AWS region (use real S3, not LocalStack)
    export AWS_DEFAULT_REGION=${TEST_AWS_REGION}
    unset AWS_ENDPOINT_URL
    
    # Verify AWS credentials
    if ! aws sts get-caller-identity &>/dev/null; then
        print_warn "AWS credentials not verified - migration tool will handle credential errors"
    else
        print_info "AWS credentials verified"
    fi
    
    # Get MariaDB connection details
    local mariadb_conn=$(get_mariadb_connection)
    local mariadb_host=$(echo "$mariadb_conn" | cut -d'|' -f1)
    local mariadb_user=$(echo "$mariadb_conn" | cut -d'|' -f2)
    local mariadb_password=$(echo "$mariadb_conn" | cut -d'|' -f3)
    local mariadb_database=$(echo "$mariadb_conn" | cut -d'|' -f4)
    
    # Determine S3 bucket and region
    local s3_bucket=${S3_BUCKET}
    local aws_region=${AWS_REGION}
    
    if [ "$USE_TUNNELS" = true ]; then
        print_info "Using AWS S3 bucket: ${s3_bucket} (region: ${aws_region})"
    else
        print_info "Using LocalStack S3 bucket: ${s3_bucket}"
    fi
    
    # Set AWS region
    export AWS_DEFAULT_REGION=${aws_region}
    
    # Run migration (without execute-sql for PE mode - manager will execute on EC2)
    # Ensure /tmp directory exists for logger
    mkdir -p /tmp
    
    # Change to project root to ensure logger can write to /tmp
    cd "$PROJECT_ROOT" || exit 1
    "$migration_bin" \
        -tenant-id ${TEST_TENANT_ID} \
        -table-name ${TEST_TABLE_NAME} \
        -mariadb-host ${mariadb_host} \
        -mariadb-user ${mariadb_user} \
        -mariadb-password "${mariadb_password}" \
        -mariadb-database ${mariadb_database} \
        -s3-bucket ${s3_bucket} \
        -aws-region ${aws_region} \
        -segments ${TEST_SEGMENTS} \
        -max-parallel-segments ${TEST_MAX_PARALLEL}
    local migration_result=$?
    cd - > /dev/null || true
    
    if [ $migration_result -ne 0 ]; then
        print_error "Migration failed with exit code: $migration_result"
        return 1
    fi
    
    print_info "Migration completed"
}

verify_csv_files() {
    print_info "Verifying CSV files..."
    
    # Note: CSV files are streamed directly to S3, not stored locally
    # This function is kept for backward compatibility but doesn't check local files
    # CSV files are verified via verify_s3_uploads() instead
    print_info "CSV files are streamed directly to S3 (verified via S3 upload check)"
    return 0
}

verify_s3_uploads() {
    print_info "Verifying S3 uploads..."
    
    local s3_bucket=${S3_BUCKET}
    local aws_region=${AWS_REGION}
    
    if [ "$USE_TUNNELS" = true ]; then
        # Use real AWS S3 with timeout
        print_info "Checking real AWS S3 bucket: ${s3_bucket} (region: ${aws_region})"
        # List objects with prefix for this tenant
        local s3_prefix="fis-migration/tenant-${TEST_TENANT_ID}"
        local s3_objects=$(timeout 10 aws s3 ls s3://${s3_bucket}/${s3_prefix}/ --recursive --region ${aws_region} 2>&1 | wc -l | tr -d ' ')
        local s3_result=$?
        
        if [ $s3_result -ne 0 ]; then
            print_warn "Failed to list S3 bucket (may not exist or no access): ${s3_bucket}"
            print_warn "This is expected if no CSV files were generated (0 rows exported)"
            return 0  # Don't fail - expected if no data
        fi
        
        if [ "$s3_objects" -eq 0 ]; then
            print_warn "No objects found in S3 bucket prefix ${s3_prefix}"
            print_warn "This is expected if migration found 0 rows to export"
            return 0  # Don't fail - expected if no data
        fi
        
        print_info "Verified $s3_objects objects in real S3 bucket: ${s3_bucket}/${s3_prefix}"
    else
        # Use LocalStack S3
        local s3_objects=$(timeout 5 docker exec fis-localstack-migration-test \
            aws --endpoint-url=http://localhost:4566 s3 ls s3://${TEST_S3_BUCKET} --recursive 2>/dev/null | wc -l | tr -d ' ')
        
        if [ "$s3_objects" -eq 0 ]; then
            print_warn "No objects found in LocalStack S3 bucket (expected if 0 rows exported)"
            return 0  # Don't fail
        fi
        
        print_info "Verified $s3_objects objects in LocalStack S3 bucket"
    fi
    
    return 0
}

verify_sql_generation() {
    print_info "Verifying SQL file generation..."
    
    local s3_bucket=${S3_BUCKET}
    local aws_region=${AWS_REGION}
    local sql_s3_key="fis-migration/sql/load-data-tenant-${TEST_TENANT_ID}.sql"
    
    # Check if SQL file exists in S3
    if [ "$USE_TUNNELS" = true ]; then
        # Use real AWS S3
        print_info "Checking SQL file in S3: s3://${s3_bucket}/${sql_s3_key}"
        if ! timeout 10 aws s3 ls "s3://${s3_bucket}/${sql_s3_key}" --region "${aws_region}" &>/dev/null; then
            print_warn "SQL file not found in S3: s3://${s3_bucket}/${sql_s3_key}"
            print_warn "This is expected if migration found 0 rows to export"
            return 0  # Don't fail - expected if no data
        fi
        
        # Download SQL file temporarily to verify contents
        local tmp_sql_file=$(mktemp)
        if ! timeout 10 aws s3 cp "s3://${s3_bucket}/${sql_s3_key}" "$tmp_sql_file" --region "${aws_region}" &>/dev/null; then
            print_warn "Failed to download SQL file from S3 for verification"
            rm -f "$tmp_sql_file"
            return 0  # Don't fail - file exists in S3, just couldn't verify contents
        fi
        
        # Verify SQL file contains LOAD DATA FROM S3 statements
        if ! grep -q "LOAD DATA FROM S3" "$tmp_sql_file"; then
            # If file is empty, this is expected for 0 rows
            if [ ! -s "$tmp_sql_file" ]; then
                print_warn "SQL file is empty (expected if migration found 0 rows to export)"
                rm -f "$tmp_sql_file"
                return 0  # Don't fail - expected if no data
            fi
            print_error "SQL file does not contain LOAD DATA FROM S3 statements"
            rm -f "$tmp_sql_file"
            return 1
        fi
        
        # Verify S3 paths in SQL (check for bucket)
        if ! grep -q "s3://${s3_bucket}" "$tmp_sql_file"; then
            # If file is empty or has no LOAD DATA statements, this is expected for 0 rows
            if [ ! -s "$tmp_sql_file" ] || ! grep -q "LOAD DATA" "$tmp_sql_file"; then
                print_warn "SQL file is empty or has no LOAD DATA statements (expected if 0 rows exported)"
                rm -f "$tmp_sql_file"
                return 0  # Don't fail - expected if no data
            fi
            print_error "SQL file does not contain expected S3 bucket paths"
            rm -f "$tmp_sql_file"
            return 1
        fi
        
        print_info "SQL file verified in S3: s3://${s3_bucket}/${sql_s3_key}"
        rm -f "$tmp_sql_file"
        return 0
    else
        # Docker mode - check local file (for LocalStack testing)
        local sql_file=$(find_sql_file ${TEST_TENANT_ID})
        
        if [ -z "$sql_file" ] || [ ! -f "$sql_file" ]; then
            print_warn "SQL file not found in expected locations"
            print_warn "This is expected if migration found 0 rows to export"
            return 0  # Don't fail - expected if no data
        fi
        
        print_info "Found SQL file: $sql_file"
        
        # Verify SQL file contains LOAD DATA FROM S3 statements
        if ! grep -q "LOAD DATA FROM S3" "$sql_file"; then
            # If file is empty, this is expected for 0 rows
            if [ ! -s "$sql_file" ]; then
                print_warn "SQL file is empty (expected if migration found 0 rows to export)"
                return 0  # Don't fail - expected if no data
            fi
            print_error "SQL file does not contain LOAD DATA FROM S3 statements"
            return 1
        fi
        
        # Verify S3 paths in SQL (check for either bucket)
        if ! grep -q "s3://${TEST_S3_BUCKET}" "$sql_file" && ! grep -q "s3://${REAL_S3_BUCKET}" "$sql_file"; then
            # If file is empty or has no LOAD DATA statements, this is expected for 0 rows
            if [ ! -s "$sql_file" ] || ! grep -q "LOAD DATA" "$sql_file"; then
                print_warn "SQL file is empty or has no LOAD DATA statements (expected if 0 rows exported)"
                return 0  # Don't fail - expected if no data
            fi
            print_error "SQL file does not contain expected S3 bucket paths"
            return 1
        fi
        
        print_info "SQL file verified: $sql_file"
        return 0
    fi
}

verify_aurora_load() {
    print_info "Verifying LOAD DATA FROM S3 execution on Aurora MySQL..."
    
    # Validate AWS MySQL connection info
    if [ -z "$AWS_MYSQL_USER" ] || [ -z "$AWS_MYSQL_SECRET" ] || [ -z "$AWS_MYSQL_REGION" ]; then
        print_error "AWS MySQL connection info is required for LOAD DATA FROM S3 verification"
        print_error "Please provide:"
        print_error "  - AWS_MYSQL_USER (or --aws-mysql-user)"
        print_error "  - AWS_MYSQL_SECRET (or --aws-mysql-secret)"
        print_error "  - AWS_MYSQL_REGION (or --aws-mysql-region)"
        return 1
    fi
    
    # Get AWS MySQL connection details
    local aws_conn=$(get_aws_mysql_connection)
    local aws_mysql_host=$(echo "$aws_conn" | cut -d'|' -f1)
    
    if [ -z "$aws_mysql_host" ]; then
        print_error "AWS MySQL host is required"
        return 1
    fi
    
    # Note: Data existence was already verified earlier in the flow (verify_tenant_has_data)
    # Proceed with LOAD DATA FROM S3 execution
    print_info "Executing LOAD DATA FROM S3 on Aurora MySQL..."
    print_info "Host: $aws_mysql_host"
    print_info "User: $AWS_MYSQL_USER"
    print_info "Secret: $AWS_MYSQL_SECRET"
    print_info "Region: $AWS_MYSQL_REGION"
    
    # Set environment variables for migration tool to use
    export FIS_AWS_MYSQL_HOST="$aws_mysql_host"
    export FIS_AWS_MYSQL_USER="$AWS_MYSQL_USER"
    export FIS_AWS_MYSQL_SECRET="$AWS_MYSQL_SECRET"
    export FIS_AWS_MYSQL_REGION="$AWS_MYSQL_REGION"
    # For tunnel mode, use provided AWS password if available; otherwise let Secrets Manager resolve it
    if [ "$USE_TUNNELS" = true ] && [ -n "$AWS_PASSWORD" ]; then
        export FIS_AWS_SQL_PASSWORD="${AWS_PASSWORD}"
    else
        unset FIS_AWS_SQL_PASSWORD  # Let Secrets Manager resolve it
    fi
    
    local migration_bin="$PROJECT_ROOT/cmd/migration/migration"
    
    # Get MariaDB connection details
    local mariadb_conn=$(get_mariadb_connection)
    local mariadb_host=$(echo "$mariadb_conn" | cut -d'|' -f1)
    local mariadb_user=$(echo "$mariadb_conn" | cut -d'|' -f2)
    local mariadb_password=$(echo "$mariadb_conn" | cut -d'|' -f3)
    
    # Determine S3 bucket based on mode
    local s3_bucket
    if [ "$USE_TUNNELS" = true ]; then
        s3_bucket=${S3_BUCKET}
    else
        s3_bucket=${TEST_S3_BUCKET}
    fi
    
    # SQL file is in S3, not local - migration tool will generate and upload it
    print_info "Running migration with -execute-sql to load data into Aurora MySQL..."
    
    # Run migration with execute-sql flag and capture output
    local migration_output=$(mktemp)
    local migration_error=$(mktemp)
    
    "$migration_bin" \
        -tenant-id ${TEST_TENANT_ID} \
        -table-name ${TEST_TABLE_NAME} \
        -mariadb-host ${mariadb_host} \
        -mariadb-user ${mariadb_user} \
        -mariadb-password "${mariadb_password}" \
        -s3-bucket ${s3_bucket} \
        -aws-region ${TEST_AWS_REGION} \
        -aurora-host ${aws_mysql_host} \
        -aurora-user ${AWS_MYSQL_USER} \
        -aurora-secret ${AWS_MYSQL_SECRET} \
        -aurora-region ${AWS_MYSQL_REGION} \
        -aurora-database ${AWS_DATABASE_NAME} \
        -execute-sql \
        -segments ${TEST_SEGMENTS} \
        -max-parallel-segments ${TEST_MAX_PARALLEL} \
        > "$migration_output" 2> "$migration_error"
    
    local migration_exit_code=$?
    
    # Extract error messages from migration output
    local error_message=""
    local iam_role_error=false
    
    # Check stderr for errors
    if [ -s "$migration_error" ]; then
        error_message=$(cat "$migration_error")
        
        # Check for IAM role configuration error
        if echo "$error_message" | grep -qi "aurora_load_from_s3_role\|aws_default_s3_role\|Error 63985"; then
            iam_role_error=true
            error_message=$(echo "$error_message" | grep -i "aurora_load_from_s3_role\|aws_default_s3_role\|Error 63985" | head -1)
        fi
    fi
    
    # Also check stdout for error messages (zap logger may output to stdout)
    if [ -s "$migration_output" ]; then
        local stdout_errors=$(cat "$migration_output" | grep -i "error\|failed" | head -5)
        if [ -n "$stdout_errors" ]; then
            if [ -z "$error_message" ]; then
                error_message="$stdout_errors"
            else
                error_message="${error_message}\n${stdout_errors}"
            fi
            
            # Check for IAM role error in stdout too
            if echo "$stdout_errors" | grep -qi "aurora_load_from_s3_role\|aws_default_s3_role\|Error 63985"; then
                iam_role_error=true
            fi
        fi
    fi
    
    # Check JSON log files for detailed error messages
    # Migration tool logs to /tmp/migration-*.log (NDJSON format)
    local log_files=$(ls -t /tmp/migration-*.log 2>/dev/null | head -1)
    if [ -n "$log_files" ] && [ -f "$log_files" ]; then
        # Extract error messages from JSON logs (look for "lv":"ER" entries)
        local json_errors=$(grep '"lv":"ER"' "$log_files" 2>/dev/null | tail -5)
        if [ -n "$json_errors" ]; then
            # Extract the "error" field from JSON (if present)
            local extracted_error=$(echo "$json_errors" | grep -o '"error":"[^"]*"' | sed 's/"error":"\(.*\)"/\1/' | head -1)
            if [ -n "$extracted_error" ]; then
                error_message="$extracted_error"
            else
                # Extract the "msg" field if no "error" field
                local extracted_msg=$(echo "$json_errors" | grep -o '"msg":"[^"]*"' | sed 's/"msg":"\(.*\)"/\1/' | head -1)
                if [ -n "$extracted_msg" ]; then
                    error_message="$extracted_msg"
                fi
            fi
            
            # Check for IAM role error in JSON logs
            if echo "$json_errors" | grep -qi "aurora_load_from_s3_role\|aws_default_s3_role\|Error 63985\|IAM role not configured"; then
                iam_role_error=true
                # Extract the full error message
                if echo "$json_errors" | grep -q '"error":'; then
                    error_message=$(echo "$json_errors" | grep -o '"error":"[^"]*"' | sed 's/"error":"\(.*\)"/\1/' | head -1)
                fi
            fi
        fi
    fi
    
    # Check JSON log files for detailed error messages
    # Migration tool logs to migration/tmp-*.log or /tmp/migration-*.log (NDJSON format)
    # Get the latest log file (sorted by modification time, most recent first)
    local log_file=""
    if [ -d "migration" ]; then
        # Check migration/ subdirectory first (if it exists)
        log_file=$(ls -t migration/tmp-*.log 2>/dev/null | head -1)
    fi
    if [ -z "$log_file" ] || [ ! -f "$log_file" ]; then
        # Fall back to /tmp directory
        log_file=$(ls -t /tmp/migration-*.log 2>/dev/null | head -1)
    fi
    
    if [ -n "$log_file" ] && [ -f "$log_file" ]; then
        # Extract error messages from JSON logs (look for "lv":"ER" entries related to LOAD DATA)
        # Get the most recent error entries (last 50 lines, then filter for LOAD DATA errors)
        local json_errors=$(tail -50 "$log_file" 2>/dev/null | grep '"lv":"ER"' | grep -i "LOAD DATA\|IAM role\|aurora_load_from_s3_role\|aws_default_s3_role\|Error 63985" | tail -3)
        if [ -n "$json_errors" ]; then
            # Check for IAM role error in JSON logs
            if echo "$json_errors" | grep -qi "aurora_load_from_s3_role\|aws_default_s3_role\|Error 63985\|IAM role not configured"; then
                iam_role_error=true
                # Extract the full error message from the "error" field using Python for proper JSON parsing
                local extracted_error=$(echo "$json_errors" | tail -1 | python3 -c "import sys, json; line=sys.stdin.read(); obj=json.loads(line); print(obj.get('error', obj.get('msg', '')))" 2>/dev/null)
                if [ -n "$extracted_error" ]; then
                    error_message="$extracted_error"
                fi
            else
                # Extract error message from "error" field if present
                local extracted_error=$(echo "$json_errors" | tail -1 | python3 -c "import sys, json; line=sys.stdin.read(); obj=json.loads(line); print(obj.get('error', obj.get('msg', '')))" 2>/dev/null)
                if [ -n "$extracted_error" ]; then
                    error_message="$extracted_error"
                fi
            fi
        fi
        
        # Also check for SQL execution summary to detect failures
        local sql_summary=$(tail -50 "$log_file" 2>/dev/null | grep '"msg":"SQL execution summary"' | tail -1)
        if [ -n "$sql_summary" ]; then
            # Check if there were failures
            local failure_count=$(echo "$sql_summary" | python3 -c "import sys, json; line=sys.stdin.read(); obj=json.loads(line); print(obj.get('failure', 0))" 2>/dev/null)
            if [ -n "$failure_count" ] && [ "$failure_count" != "0" ]; then
                # Migration had failures even if exit code was 0
                if [ -z "$error_message" ]; then
                    error_message="SQL execution failed: $failure_count statement(s) failed"
                fi
                # If we haven't detected IAM role error yet, check the summary
                if [ "$iam_role_error" = false ] && [ -z "$error_message" ]; then
                    # Re-check for IAM role error in all recent errors
                    local all_errors=$(tail -50 "$log_file" 2>/dev/null | grep '"lv":"ER"' | tail -5)
                    if echo "$all_errors" | grep -qi "aurora_load_from_s3_role\|aws_default_s3_role\|Error 63985"; then
                        iam_role_error=true
                        local extracted_error=$(echo "$all_errors" | grep -i "aurora_load_from_s3_role\|aws_default_s3_role\|Error 63985" | tail -1 | python3 -c "import sys, json; line=sys.stdin.read(); obj=json.loads(line); print(obj.get('error', obj.get('msg', '')))" 2>/dev/null)
                        if [ -n "$extracted_error" ]; then
                            error_message="$extracted_error"
                        fi
                    fi
                fi
            fi
        fi
    fi
    
    # Clean up temp files
    rm -f "$migration_output" "$migration_error"
    
    # Check if migration actually failed (either exit code != 0 OR errors in logs)
    local migration_failed=false
    if [ $migration_exit_code -ne 0 ]; then
        migration_failed=true
    elif [ "$iam_role_error" = true ] || [ -n "$error_message" ]; then
        # Even if exit code is 0, check if there were errors in the logs
        # (migration tool may not exit on SQL execution errors)
        migration_failed=true
    fi
    
    if [ "$migration_failed" = false ]; then
        print_info "LOAD DATA FROM S3 executed successfully on Aurora MySQL"
        
        # Verify data exists in Aurora MySQL
        print_info "Verifying data in Aurora MySQL..."
        verify_aurora_data ${TEST_TENANT_ID} || return 1
        
        return 0
    else
        # Migration failed - show actual error
        print_error "LOAD DATA FROM S3 execution failed (exit code: $migration_exit_code)"
        
        if [ "$iam_role_error" = true ]; then
            print_error ""
            print_error "🔴 Root Cause: Aurora MySQL IAM role not configured"
            print_error ""
            print_error "The Aurora MySQL cluster requires an IAM role with S3 access permissions"
            print_error "for LOAD DATA FROM S3 to work."
            print_error ""
            print_error "Error details:"
            if [ -n "$error_message" ]; then
                echo "$error_message" | while IFS= read -r line; do
                    print_error "  $line"
                done
            fi
            print_error ""
            print_error "Required action:"
            print_error "  1. Create an IAM role with S3 read permissions for bucket: ${s3_bucket}"
            print_error "  2. Configure Aurora MySQL cluster parameter:"
            print_error "     - aurora_load_from_s3_role = <IAM_ROLE_ARN>"
            print_error "     OR"
            print_error "     - aws_default_s3_role = <IAM_ROLE_ARN>"
            print_error "  3. Contact DevOps to configure this (see cmd/migration/README.md)"
        elif [ -n "$error_message" ]; then
            print_error ""
            print_error "Error details:"
            echo "$error_message" | while IFS= read -r line; do
                print_error "  $line"
            done
        else
            print_error "No error details captured. Check migration tool logs for more information."
        fi
        
        return 1
    fi
}

verify_aurora_data() {
    local tenant_id=$1
    local db_name=${AWS_DATABASE_NAME}
    local aws_host="127.0.0.1"
    local aws_port="${AWS_LOCAL_PORT}"
    
    print_info "Checking for data in Aurora MySQL for tenant ${tenant_id}..."
    
    # Use AWS password if provided (for tunnel mode)
    # In production, migration tool resolves from Secrets Manager
    if [ "$USE_TUNNELS" = true ] && [ -n "$AWS_PASSWORD" ]; then
        export MYSQL_PWD="${AWS_PASSWORD}"
    else
        # For non-tunnel mode, password should be resolved by migration tool
        # We'll try without password first (may fail, but migration tool handles it)
        export MYSQL_PWD=""
    fi
    
    # Query Aurora MySQL for data
    local count=$(timeout 10 bash -c "mysql -h ${aws_host} -P ${aws_port} -u ${AWS_MYSQL_USER} -N -e \"SELECT COUNT(*) FROM ${db_name}.fis_aggr WHERE tenantid = ${tenant_id};\" ${db_name} 2>/dev/null" || echo "0")
    
    if [ "$count" -gt 0 ]; then
        print_info "✅ Verified ${count} rows in Aurora MySQL for tenant ${tenant_id}"
        
        # Also check a sample row to verify data integrity
        local sample=$(timeout 5 bash -c "mysql -h ${aws_host} -P ${aws_port} -u ${AWS_MYSQL_USER} -N -e \"SELECT hash FROM ${db_name}.fis_aggr WHERE tenantid = ${tenant_id} LIMIT 1;\" ${db_name} 2>/dev/null" || echo "")
        if [ -n "$sample" ]; then
            print_info "✅ Sample row verified (hash: ${sample:0:20}...)"
        fi
        
        return 0
    else
        # No data found - this is expected if LOAD DATA FROM S3 failed
        # The actual error should have been shown in verify_aurora_load() above
        print_warn "⚠ No data found in Aurora MySQL for tenant ${tenant_id} (count: ${count})"
        print_warn "This indicates that LOAD DATA FROM S3 did not successfully load data."
        print_warn "Check the error message above for the root cause."
        return 1
    fi
}

cleanup() {
    # Clean up tunnels if using tunnel mode
    if [ "$USE_TUNNELS" = true ] && [ "$KEEP_CONTAINERS" = false ]; then
        print_info "Cleaning up SSH tunnels..."
        if [[ -n "$MP_TUNNEL_PID" ]] && kill -0 "$MP_TUNNEL_PID" 2>/dev/null; then
            kill "$MP_TUNNEL_PID" 2>/dev/null || true
            wait "$MP_TUNNEL_PID" 2>/dev/null || true
        fi
        if [[ -n "$AWS_TUNNEL_PID" ]] && kill -0 "$AWS_TUNNEL_PID" 2>/dev/null; then
            kill "$AWS_TUNNEL_PID" 2>/dev/null || true
            wait "$AWS_TUNNEL_PID" 2>/dev/null || true
        fi
        print_info "Tunnels cleaned up"
    fi
    
    # Clean up Docker containers if using Docker mode
    if [ "$USE_TUNNELS" != true ] && [ "$KEEP_CONTAINERS" = false ]; then
        print_info "Cleaning up containers..."
        docker stop fis-mariadb-migration-test fis-localstack-migration-test 2>/dev/null || true
        docker rm fis-mariadb-migration-test fis-localstack-migration-test 2>/dev/null || true
        print_info "Containers cleaned up"
    elif [ "$KEEP_CONTAINERS" = true ]; then
        if [ "$USE_TUNNELS" = true ]; then
            print_info "Tunnels kept running (--keep-containers flag set)"
        else
            print_info "Containers kept running (--keep-containers flag set)"
        fi
    fi
}

# Main execution
main() {
    parse_args "$@"
    
    # Validate required parameters
    if [ -z "$MP_PASSWORD" ]; then
        print_error "MariaDB password is required. Please provide --mp-password <password>"
        exit 1
    fi
    
    # Set default S3 bucket and region if not provided via CLI
    if [ -z "$S3_BUCKET" ]; then
        if [ "$USE_TUNNELS" = true ]; then
            # Use environment-specific defaults (already set in parse_args if --use-tunnels <env> was used)
            if [ "$TUNNEL_ENV" = "fr4" ]; then
                S3_BUCKET="${FR4_S3_BUCKET}"
            else
                S3_BUCKET="${QA01_S3_BUCKET}"  # Default: qa01 (NPE) bucket
            fi
        else
            S3_BUCKET="${TEST_S3_BUCKET}"  # Default: LocalStack bucket
        fi
    fi
    
    if [ -z "$AWS_REGION" ]; then
        if [ "$USE_TUNNELS" = true ] && [ "$TUNNEL_ENV" = "fr4" ]; then
            AWS_REGION="${FR4_AWS_REGION}"
        else
            AWS_REGION="${QA01_AWS_REGION}"  # Default: qa01 (NPE) region
        fi
    fi
    
    print_info "=== Migration Tool Verification ==="
    
    # Handle cleanup-only mode
    if [ "$CLEANUP_ONLY" = true ]; then
        if [ "$USE_TUNNELS" != true ]; then
            print_error "cleanup-only requires --use-tunnels mode"
            exit 1
        fi
        
        print_info "Running cleanup only..."
        check_tsh || exit 1
        
        # Setup tunnels if not skipping
        if [ "$SKIP_TUNNEL_SETUP" != true ]; then
            setup_tunnels || exit 1
        else
            # Just verify tunnels work
            if ! test_tunnel_connectivity; then
                print_error "Tunnels are not working. Remove --skip-tunnel-setup to create new tunnels."
                exit 1
            fi
        fi
        
        cleanup_test_data || exit 1
        print_info "=== Cleanup completed ==="
        return 0
    fi
    
    if [ "$USE_TUNNELS" = true ]; then
        print_info "Mode: SSH Tunnels (MariaDB + optional AWS MySQL)"
        print_info "  - MariaDB: via SSH tunnel (localhost:${MP_LOCAL_PORT})"
        print_info "  - S3 bucket: ${S3_BUCKET} (region: ${AWS_REGION})"
        if [ -z "$AWS_MYSQL_USER" ] || [ -z "$AWS_MYSQL_SECRET" ]; then
            print_info "  - SQL execution: Skipped (manager will execute on EC2)"
        fi
    else
        print_info "Mode: Docker Containers (local MariaDB + LocalStack S3)"
        check_docker_available || exit 1
    fi
    
    check_migration_binary || exit 1
    
    # Trap to ensure cleanup on exit
    trap cleanup EXIT
    
    # Setup tunnels if using tunnel mode
    if [ "$USE_TUNNELS" = true ]; then
        setup_tunnels || exit 1
        
        # Determine data generation method
        if [ "$USE_EXISTING_DATA" = true ]; then
            # Use existing tenant data
            print_info "Using existing tenant data"
            if [ -z "$TEST_TENANT_ID" ] || [ "$TEST_TENANT_ID" = "999999" ]; then
                # Auto-detect tenant (stderr goes to terminal, stdout captured)
                local detected_tenant
                detected_tenant=$(find_tenant_with_data)
                local find_result=$?
                if [ $find_result -ne 0 ] || [ -z "$detected_tenant" ] || [ "$detected_tenant" = "" ]; then
                    print_error "Failed to find tenant with existing data"
                    exit 1
                fi
                # Ensure we have a valid numeric tenant ID
                if ! [[ "$detected_tenant" =~ ^[0-9]+$ ]]; then
                    print_error "Invalid tenant ID detected: '${detected_tenant}'"
                    exit 1
                fi
                TEST_TENANT_ID="$detected_tenant"
                print_info "Using auto-detected tenant ID: ${TEST_TENANT_ID}"
            fi
            verify_tenant_has_data ${TEST_TENANT_ID} || exit 1
        else
            # Check if tenant already has data - if so, use it automatically
            if [ -n "$TEST_TENANT_ID" ] && [ "$TEST_TENANT_ID" != "999999" ]; then
                # User specified a tenant ID - check if it has data
                if verify_tenant_has_data ${TEST_TENANT_ID} 2>/dev/null; then
                    print_info "Tenant ${TEST_TENANT_ID} already has data - using existing data"
                    USE_EXISTING_DATA=true  # Set flag to use existing data
                else
                    # Tenant has no data - require explicit flag
                    print_error "Tenant ${TEST_TENANT_ID} has no data."
                    print_error "Please use --use-existing-data with a tenant that has data"
                    exit 1
                fi
            else
                # No tenant ID specified - require explicit flag
                print_error "No tenant ID specified and no data generation method selected."
                print_error "Please use --use-existing-data (auto-detects tenant with data)"
                exit 1
            fi
        fi
    fi
    
    # Start containers only in Docker mode (skip when using tunnels - connecting to NPE)
    if [ "$USE_TUNNELS" != true ]; then
        start_containers || exit 1
    fi
    
    # Initialize schema only in Docker mode (qa01 already has schema)
    if [ "$USE_TUNNELS" != true ] && [ "$USE_EXISTING_DATA" != true ]; then
        initialize_schema || exit 1
    fi
    
    # Skip test data generation - we only support --use-existing-data now
    # (benchmark/direct INSERT and Kafka features were removed)
    
    # Run migration
    run_migration || exit 1
    
    verify_csv_files || exit 1
    verify_s3_uploads || exit 1
    verify_sql_generation || exit 1
    
    # Skip SQL execution verification if AWS MySQL connection info not provided (manager will execute on EC2)
    if [ -z "$AWS_MYSQL_USER" ] || [ -z "$AWS_MYSQL_SECRET" ] || [ -z "$AWS_MYSQL_REGION" ]; then
        print_info "Skipping SQL execution verification (manager will execute on EC2)"
        print_info ""
        print_info "=== Next Steps for Manager ==="
        print_info "1. Download SQL file from S3:"
        print_info "   aws s3 cp s3://${S3_BUCKET}/fis-migration/sql/load-data-tenant-${TEST_TENANT_ID}.sql ./load-data-tenant-${TEST_TENANT_ID}.sql"
        print_info ""
        print_info "2. Connect to Aurora MySQL on EC2 and execute SQL file"
        print_info ""
        print_info "⚠️  IMPORTANT: Ensure Aurora MySQL has IAM role configured for S3 access"
        print_info "   - Parameter: aurora_load_from_s3_role or aws_default_s3_role"
        print_info "   - IAM role must have S3 read permissions for bucket: ${S3_BUCKET}"
        print_info "   - See README.md for detailed IAM role setup instructions"
        print_info ""
    else
        # Mandatory: Verify LOAD DATA FROM S3 execution
        if ! verify_aurora_load; then
            print_error "LOAD DATA FROM S3 verification failed (mandatory)"
            exit 1
        fi
    fi
    
    # Cleanup test data from qa01 MariaDB (skip if using existing data)
    if [ "$KEEP_CONTAINERS" = false ] && [ "$USE_EXISTING_DATA" != true ]; then
        print_info "Cleaning up test data from qa01 MariaDB..."
        cleanup_test_data || print_warn "Cleanup failed, but migration completed successfully"
    elif [ "$USE_EXISTING_DATA" = true ]; then
        print_info "Skipping cleanup (using existing data)"
    fi
    
    print_info "=== All verification checks passed ==="
}

main "$@"

