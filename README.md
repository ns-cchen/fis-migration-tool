# Migration Tool

Database migration tool that exports `fis_aggr` data from MariaDB to CSV files, uploads them to S3, and generates `LOAD DATA FROM S3` SQL scripts for Aurora MySQL.

## Features

- **Hash-based Segmentation**: Partitions data by MD5 hash prefix ([00, FF]) for parallel processing
- **Streaming CSV Export**: Exports data with streaming multipart upload - each 100k-row batch is uploaded as a separate S3 multipart part
- **No Local File Storage**: Data is streamed directly to S3, eliminating the need for local disk space
- **Consistent Snapshot**: Uses `REPEATABLE READ` transaction isolation to create a point-in-time snapshot, preventing new inserts from appearing during export
- **S3 Multipart Upload**: Each hash range produces one S3 object with multiple parts (one per batch)
- **SQL Generation**: Generates `LOAD DATA FROM S3` statements and uploads SQL file to S3
- **Automatic Execution**: Optionally executes SQL statements on Aurora MySQL automatically

## Usage

### Basic Migration

```bash
./migration \
  -tenant-id 1234 \
  -table-name fis_aggr \
  -mariadb-host localhost:3306 \
  -mariadb-user root \
  -mariadb-password password \
  -s3-bucket my-migration-bucket \
  -aws-region us-east-1 \
  -segments 16 \
  -max-parallel-segments 8
```

### With Automatic SQL Execution

```bash
./migration \
  -tenant-id 1234 \
  -table-name fis_aggr \
  -mariadb-host localhost:3306 \
  -mariadb-user root \
  -mariadb-password password \
  -s3-bucket my-migration-bucket \
  -aws-region us-east-1 \
  -aurora-host aurora-cluster.region.rds.amazonaws.com \
  -aurora-user admin \
  -aurora-secret rds!cluster-xxx \
  -aurora-region us-east-1 \
  -execute-sql
```

## Configuration

### CLI Flags

#### Required Flags

- `-tenant-id <int>`: Tenant ID to migrate
- `-table-name <string>`: Table name (default: `fis_aggr`)
- `-mariadb-host <string>`: MariaDB host:port
- `-s3-bucket <string>`: S3 bucket name
- `-aws-region <string>`: AWS region

#### Optional Flags

- `-mariadb-port <int>`: MariaDB port (default: 3306)
- `-mariadb-user <string>`: MariaDB username
- `-mariadb-password <string>`: MariaDB password
- `-mariadb-database <string>`: MariaDB database name (default: `fis`)
- `-s3-prefix <string>`: S3 key prefix (default: `fis-migration`)
- `-segments <int>`: Number of hash segments (default: 16)
- `-max-parallel-segments <int>`: Max parallel segments (default: 8)
- `-batch-size <int>`: Batch size for pagination (default: 100000)
- `-config-file <string>`: Config file path (default: `migration-config.yaml`)

#### Aurora MySQL (for SQL execution)

- `-aurora-host <string>`: Aurora MySQL endpoint
- `-aurora-port <int>`: Aurora MySQL port (default: 3306)
- `-aurora-user <string>`: Aurora MySQL username
- `-aurora-secret <string>`: AWS Secrets Manager secret name (e.g., `rds!cluster-xxx`)
- `-aurora-region <string>`: AWS region for Secrets Manager
- `-aurora-database <string>`: Aurora MySQL database name (default: `fis`)
- `-execute-sql`: Execute `LOAD DATA FROM S3` after generating SQL
- `-sql-exec-timeout <int>`: SQL execution timeout in seconds (default: 300)

### Environment Variables

All flags can be set via environment variables with prefix `FIS_MIGRATION_`:

- `FIS_MIGRATION_TENANT_ID`
- `FIS_MIGRATION_MARIADB_HOST`
- `FIS_MIGRATION_S3_BUCKET`
- `FIS_MIGRATION_AWS_REGION`
- etc.

### YAML Config File

Create a `migration-config.yaml` file:

```yaml
tenant_id: 1234
table_name: fis_aggr
mariadb_host: localhost:3306
mariadb_user: root
mariadb_password: password
mariadb_database: fis
s3_bucket: my-migration-bucket
s3_prefix: fis-migration
aws_region: us-east-1
segments: 16
max_parallel_segments: 8
batch_size: 100000

# Optional: Aurora MySQL for SQL execution
aurora_host: aurora-cluster.region.rds.amazonaws.com
aurora_user: admin
aurora_secret: rds!cluster-xxx
aurora_region: us-east-1
aurora_database: fis
execute_sql: false
sql_exec_timeout: 300
```

## Configuration Priority

1. CLI flags (highest priority)
2. Environment variables
3. YAML config file
4. Defaults

## Output

### Streaming Multipart Upload

The tool uses **streaming multipart upload** to S3:

- **No local file storage**: Data is streamed directly to S3 as CSV batches
- **One S3 object per hash range**: Each hash range (e.g., `00-10`) produces one S3 object
- **Each batch = one multipart part**: Each 100k-row batch is converted to CSV bytes and uploaded as a separate S3 multipart part
- **Automatic completion**: After all batches are uploaded, the multipart upload is automatically completed

### S3 Keys

CSV files are uploaded to S3 with key pattern:

```
<prefix>/tenant-<T>/<table>/<filename>
```

Example: `fis-migration/tenant-1234/fis_aggr/tenant-1234.fis_aggr.hash-00-10.csv`

SQL file is uploaded to S3 with key pattern:

```
<prefix>/sql/load-data-tenant-<T>.sql
```

Example: `fis-migration/sql/load-data-tenant-1234.sql`

## Verifying S3 Uploads

After running the migration tool, you can verify that CSV files and SQL file were uploaded to S3.

### Using AWS CLI

**List all CSV files for a tenant:**

```bash
aws s3 ls s3://<bucket>/fis-migration/tenant-<tenant-id>/fis_aggr/ --recursive --region <region>
```

**Count CSV files:**

```bash
aws s3 ls s3://<bucket>/fis-migration/tenant-<tenant-id>/fis_aggr/ --recursive --region <region> | grep "\.csv$" | wc -l
```

**List SQL file:**

```bash
aws s3 ls s3://<bucket>/fis-migration/sql/load-data-tenant-<tenant-id>.sql --region <region>
```

**Download a CSV file to verify content:**

```bash
aws s3 cp s3://<bucket>/<csv-s3-key> ./verify.csv --region <region>
head -5 verify.csv  # View first 5 lines
```

**Download SQL file:**

```bash
aws s3 cp s3://<bucket>/fis-migration/sql/load-data-tenant-<tenant-id>.sql ./load-data-tenant-<tenant-id>.sql --region <region>
```

### Using Migration Tool Output

The migration tool prints a summary at the end showing:

- Total CSV files uploaded
- Individual CSV file S3 keys (up to 10 files, or first 5 + last 5 if more)
- SQL file S3 key
- AWS CLI command to verify all files

Example output:

```
=== Migration Summary ===
Tenant ID: 1234
Table: fis_aggr
Total rows exported: 500000
Total CSV files: 5
S3 bucket: my-migration-bucket
S3 prefix: fis-migration
SQL file S3 key: fis-migration/sql/load-data-tenant-1234.sql

CSV files uploaded to S3:
  1. s3://my-migration-bucket/fis-migration/tenant-1234/fis_aggr/tenant-1234.fis_aggr.hash-00-10.csv (100000 rows)
  2. s3://my-migration-bucket/fis-migration/tenant-1234/fis_aggr/tenant-1234.fis_aggr.hash-10-20.csv (100000 rows)
  ...

To verify all CSV files in S3:
  aws s3 ls s3://my-migration-bucket/fis-migration/tenant-1234/fis_aggr/ --recursive --region us-east-1
```

## AWS Credentials

The tool uses AWS credentials in the following order:

1. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
2. Vault-injected files (`/vault/secrets/awsauroramysqlkey`, `/vault/secrets/awsauroramysqlpass`)

For Aurora MySQL password, the tool uses AWS Secrets Manager (via `util.ResolveAWSDBPassword()`).

## Error Handling

- **Database errors**: Retry with exponential backoff (max 5 retries)
- **S3 errors**: Retry with exponential backoff (max 5 retries)
- **SQL execution errors**: Log and continue (don't fail entire migration)
- **Connection errors**: Retry up to 3 times with exponential backoff
- **Aurora MySQL LOAD DATA FROM S3 errors**:
  - **Error 63985**: Aurora MySQL cluster requires IAM role configuration for S3 access
    - **Fix**: Configure `aurora_load_from_s3_role` or `aws_default_s3_role` on the Aurora MySQL cluster
  - **Error 1062 (Duplicate entry)**: Data already exists in the target table
    - **Fix**: The migration tool uses `IGNORE` keyword to automatically skip duplicate entries
    - **Behavior**: Duplicate rows are skipped, migration continues successfully
    - **Note**: This allows re-running migration without failing on existing data
  - **Documentation**: See [AWS Aurora MySQL LOAD DATA FROM S3 documentation](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/AuroraMySQL.Integrating.LoadFromS3.html)

## Aurora MySQL IAM Role Configuration (Required for LOAD DATA FROM S3)

### Overview

Aurora MySQL requires an IAM role to be configured on the cluster to access S3 buckets for `LOAD DATA FROM S3` operations. Without this configuration, you will encounter **Error 63985** when attempting to load data from S3.

**Important:** This applies to **both NPE and PE environments**. When the manager executes SQL on EC2, the SQL statements run on Aurora MySQL, which needs the IAM role configured. The EC2 instance itself does not need the IAM role - only Aurora MySQL does.

### Error Message

```
Error 63985 (HY000): S3 API returned error: Both aurora_load_from_s3_role and aws_default_s3_role are not specified, please see documentation for more details
```

### What Needs to Be Configured

The Aurora MySQL cluster needs one of the following parameters configured:

- **`aurora_load_from_s3_role`**: IAM role ARN specifically for LOAD DATA FROM S3 operations
- **`aws_default_s3_role`**: Default IAM role for S3 operations (can be used for multiple purposes)
- **AWS Documentation Reference**:
  - [Aurora MySQL LOAD DATA FROM S3](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/AuroraMySQL.Integrating.LoadFromS3.html)
  - [Setting up IAM roles for Aurora](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/AuroraMySQL.Integrating.Authorizing.IAM.html)

## Examples

### Full Migration with SQL Execution

```bash
./migration \
  -tenant-id 1234 \
  -table-name fis_aggr \
  -mariadb-host mariadb.example.com:3306 \
  -mariadb-user fis_user \
  -mariadb-password secret \
  -s3-bucket fis-migration-bucket \
  -aws-region us-east-1 \
  -segments 16 \
  -max-parallel-segments 8 \
  -aurora-host aurora-cluster.us-east-1.rds.amazonaws.com \
  -aurora-user admin \
  -aurora-secret rds!cluster-abc123 \
  -aurora-region us-east-1 \
  -execute-sql
```

## End-to-End Migration Testing

The `verify-migration.sh` script provides a complete end-to-end migration test that:

1. Uses existing data from MariaDB
2. Dumps CSV files from MariaDB
3. Uploads CSV files to S3
4. Executes LOAD DATA FROM S3 on Aurora MySQL (optional)
5. Verifies data exists in Aurora MySQL


### Usage Examples

**Example 1: Local testing with existing data (NPE) - Auto-detect tenant**

```bash
./verify-migration.sh --use-tunnels qa01 --use-existing-data \
  --mp-password <npe-mariadb-password> \
  --aws-mysql-user fis \
  --aws-mysql-secret rds!cluster-d256b298-d749-488a-8e55-34018d6047dc \
  --aws-mysql-region us-east-1 \
  --aws-database fis_qa01
```

**What it does:**

1. Creates SSH tunnels to NPE MariaDB and AWS Aurora MySQL (qa01)
2. Auto-detects a tenant with existing data in MariaDB (no `--tenant-id` specified)
3. Exports tenant data from MariaDB → CSV files → Uploads to S3
4. Generates SQL file (`LOAD DATA FROM S3`) → Uploads to S3
5. Executes SQL on Aurora MySQL to load data
6. Verifies data exists in Aurora MySQL

**Flags explained:**

- `--use-tunnels qa01`: Auto-configures qa01 tunnel settings (jump/target/S3/region)
- `--use-existing-data`: Use existing tenant data, auto-detect tenant
- `--mp-password`: MariaDB password for NPE (required)
- `--aws-mysql-user`, `--aws-mysql-secret`, `--aws-mysql-region`: Aurora MySQL connection (for SQL execution)
- `--aws-database`: Aurora MySQL database name

**Example 2: With specific tenant ID (NPE) - Use existing data for tenant 1234**

```bash
./verify-migration.sh --use-tunnels qa01 --use-existing-data \
  --mp-password <npe-mariadb-password> \
  --tenant-id 1234 \
  --aws-mysql-user fis \
  --aws-mysql-secret rds!cluster-d256b298-d749-488a-8e55-34018d6047dc \
  --aws-mysql-region us-east-1 \
  --aws-database fis_qa01
```

**What it does:**

1. Creates SSH tunnels to NPE MariaDB and AWS Aurora MySQL
2. Uses existing data for tenant 1234 (explicit tenant ID, no auto-detection)
3. Exports tenant 1234 data from MariaDB → CSV files → Uploads to S3
4. Generates SQL file → Uploads to S3
5. Executes SQL on Aurora MySQL to load data
6. Verifies data exists in Aurora MySQL

**Flags explained:**

- `--tenant-id 1234`: Explicitly specify tenant ID (overrides auto-detection)
- `--use-existing-data`: Use existing data
- Other flags same as Example 1

**Example 3: PE (Production - fr4) - Export only, no SQL execution**

```bash
./verify-migration.sh --use-tunnels fr4 \
  --mp-password <pe-mariadb-password> \
  --tenant-id <tenant-id>
```

**What it does:**

1. Creates SSH tunnel to PE MariaDB (fr4) - auto-configured
2. Exports tenant data from PE MariaDB → CSV files → Uploads to PE S3 bucket
3. Generates SQL file → Uploads to S3
4. **Skips SQL execution** (no `--aws-mysql-*` flags provided)
5. Prints instructions for manager to download SQL file and execute on EC2

**Flags explained:**

- `--use-tunnels fr4`: Auto-configures fr4 tunnel settings (jump/target/S3/region)
- `--mp-password`: PE MariaDB password (required)
- `--tenant-id`: **Required** - Always specify tenant ID for PE (no auto-detection)
- **No `--aws-mysql-*` flags**: SQL execution skipped, manager will execute on EC2

### CLI Flags

- `--use-existing-data`: Use existing tenant data, don't generate (default: false)
- `--tenant-id N`: Specify tenant ID (optional, auto-detect if not provided)

## Local Execution with SSH Tunnels

The migration tool can be run locally using SSH tunnels to connect to qa01 MariaDB and AWS MySQL, without deploying to Kubernetes.

### Prerequisites

- `tsh` (Teleport SSH client) installed and configured
- Access to qa01 and AWS clusters via Teleport (`tsh login`)
- AWS credentials configured (for S3 and Secrets Manager)
- **MariaDB password** (required via `--mp-password` flag for both NPE and PE)
- **AWS Aurora MySQL password** (optional, only needed for SQL execution verification via `--aws-password` flag)

### Production Environment (PE) Testing

For **Production Environment (PE)**, the tool can be run locally since PE K8s pods don't have AWS secrets mounted. The manager will execute SQL on EC2 after CSV files are uploaded.

**PE Testing:**

- Uses SSH tunnels to connect to PE MariaDB (same as NPE)
- Provide MariaDB password via `--mp-password` flag (required for both NPE and PE)
- **Always specify `--tenant-id`** (no auto-detection in production)
- Override S3 bucket and region via `--s3-bucket` and `--aws-region` flags
- **Skips SQL execution** if AWS MySQL connection info not provided (manager executes on EC2)
- Provides clear instructions for manager on what SQL to execute

**Important:**

- Before manager executes SQL on EC2, ensure Aurora MySQL has IAM role configured for S3 access (see "Aurora MySQL IAM Role Configuration" section below)
- When manager runs SQL on EC2, the SQL executes on Aurora MySQL, which needs the IAM role configured (not the EC2 instance)
- Manager will encounter Error 63985 if Aurora doesn't have the IAM role configured

### Usage

Run the verification script with `--use-tunnels` flag:

```bash
# Example: Run verification with SSH tunnels (NPE/qa01) - Auto-detect tenant
# What it does: Auto-detects tenant, exports data, uploads to S3, executes SQL on Aurora
./verify-migration.sh --use-tunnels qa01 \
  --mp-password <npe-mariadb-password> \
  --aws-mysql-user fis \
  --aws-mysql-secret rds!cluster-xxx \
  --aws-mysql-region us-east-1
# Flags:
#   --use-tunnels qa01: Auto-configures qa01 tunnel settings (jump/target/S3/region)
#   --mp-password: MariaDB password (required)
#   --aws-mysql-*: Required for SQL execution
# Note: Tenant ID auto-detected (uses --use-existing-data by default if no --tenant-id)

# Example: Run verification for PE (fr4) - Export only, no SQL execution
# What it does: Exports tenant data, uploads to PE S3, generates SQL file, skips SQL execution
./verify-migration.sh --use-tunnels fr4 \
  --mp-password <pe-mariadb-password> \
  --tenant-id <tenant-id>
# Flags:
#   --use-tunnels fr4: Auto-configures fr4 tunnel settings (jump/target/S3/region)
#   --mp-password: MariaDB password (required)
#   --tenant-id: Tenant ID (required for PE)
# Note: No --aws-mysql-* flags = SQL execution skipped, manager executes on EC2
```

```

### Tunnel Configuration

The script uses the following default tunnel configuration:

- **MP MariaDB (qa01/NPE)**:
  - Jump host: `mariafis01-1.qa01-mp-npe.nc1.iad0.nsscloud.net`
  - Target: `mariafishavip.qa01-mp-npe.nc1.iad0.nsscloud.net`
  - Local port: `3307`
  - Cluster: `iad0`

- **MP MariaDB (fr4/PE)**:
  - Jump host: `mariafis01-1.my1.fr4.nskope.net`
  - Target: `mariafis01-1.my1.fr4.nskope.net`
  - Local port: `3307` (same as NPE, can be overridden with `--mp-port`)
  - Cluster: `fr4`

- **AWS Aurora MySQL (qa01/NPE)**:
  - Jump host: `ns-nonprod-eng-data-ls-container-mariadb-use1-dev.npe.235494792415.us-east-1c.aws.nskope.net`
  - Target: `ns-nonprod-fis-cluster-dev-use1.cluster-cedske4o4zsx.us-east-1.rds.amazonaws.com`
  - Local port: `3308`
  - Cluster: `aws-nonprod`
  - Secret: `rds!cluster-d256b298-d749-488a-8e55-34018d6047dc`
  - Region: `us-east-1`

- **AWS Aurora MySQL (fr4/PE)**:
  - Jump host: (specify via `--aws-jump` flag if needed)
  - Target: `ns-prod-fis-cluster-euc1.proxy-crk82gmwwc03.eu-central-1.rds.amazonaws.com` (RDS Proxy endpoint)
  - Local port: `3308` (same as NPE, can be overridden with `--aws-port`)
  - Cluster: (specify via `--aws-jump` flag if tunnel is needed)
  - Secret: `rds!cluster-dc527dd1-6e19-4aa4-a14f-c370fab851f1-ODQqsh`
  - Region: `eu-central-1`

**Simplified Usage:**
- `--use-tunnels qa01`: Use qa01 (NPE) environment - auto-configures all tunnel settings
- `--use-tunnels fr4`: Use fr4 (PE) environment - auto-configures all tunnel settings
- `--mp-password <password>`: **Required** - MariaDB password (for both NPE and PE)
- `--aws-password <password>`: Optional - AWS Aurora MySQL password (only needed for SQL execution verification)
- `--tenant-id <id>`: Tenant ID to export (required for PE, optional for NPE with auto-detection)

**Advanced Override Flags (optional):**
- `--mp-jump`: Override MariaDB jump host (usually not needed with `--use-tunnels <env>`)
- `--mp-target`: Override MariaDB target host (usually not needed with `--use-tunnels <env>`)
- `--s3-bucket <bucket>`: Override S3 bucket (auto-set by `--use-tunnels <env>`)
- `--aws-region <region>`: Override AWS region (auto-set by `--use-tunnels <env>`)
- `--aws-jump`: Override AWS Aurora jump host
- `--aws-target`: Override AWS Aurora target host
- `--mp-port`: Override MariaDB local port (default: 3307)
- `--aws-port`: Override AWS local port (default: 3308)

### Environment Variables

- `FIS_AWS_MYSQL_SECRET`: AWS Secrets Manager secret name (if not provided via CLI)
- `FIS_AWS_MYSQL_REGION`: AWS region for Secrets Manager (if not provided via CLI)
- `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`: For S3 access
- Note: MP and AWS MySQL passwords are hardcoded in the script (NPE only)

### Tunnel Management

- Script automatically creates tunnels if `--use-tunnels` is set
- Use `--skip-tunnel-setup` if tunnels are already running (script will verify they work)
- Script cleans up tunnels on exit (unless `--keep-containers` is set)


### Example: Full Local Execution

**NPE (Non-Production):**
```bash
# 1. Ensure you're logged into Teleport
tsh login

# 2. Run verification with tunnels (qa01/NPE)
./verify-migration.sh --use-tunnels qa01 \
  --mp-password <npe-mariadb-password> \
  --aws-mysql-user fis \
  --aws-mysql-secret rds!cluster-xxx \
  --aws-mysql-region us-east-1

# 3. Or skip tunnel setup if tunnels already exist
./verify-migration.sh --use-tunnels qa01 --skip-tunnel-setup \
  --mp-password <npe-mariadb-password> \
  --aws-mysql-user fis \
  --aws-mysql-secret rds!cluster-xxx \
  --aws-mysql-region us-east-1
```

**PE (Production - fr4):**

```bash
# 1. Ensure you're logged into Teleport
tsh login

# 2. Run verification for PE (fr4) - export only, manager executes SQL on EC2
./verify-migration.sh --use-tunnels fr4 \
  --mp-password <pe-mariadb-password> \
  --tenant-id <tenant-id>
# Flags:
#   --use-tunnels fr4: Auto-configures fr4 tunnel settings (jump/target/S3/region)
#   --mp-password: PE MariaDB password (required)
#   --tenant-id: Tenant ID (required for PE)
# Note: No --aws-mysql-* flags = SQL execution skipped, manager executes on EC2

# 3. Or with SQL execution (if AWS MySQL connection info provided)
./verify-migration.sh --use-tunnels fr4 \
  --mp-password <pe-mariadb-password> \
  --tenant-id <tenant-id> \
  --aws-mysql-user fis \
  --aws-mysql-secret rds!cluster-dc527dd1-6e19-4aa4-a14f-c370fab851f1-ODQqsh \
  --aws-mysql-region eu-central-1
```

## Running Migration Tests

The migration tool can be run in two ways: **locally** (using SSH tunnels) or **in a Kubernetes pod**.

### Local Testing

Local testing uses SSH tunnels to connect to real databases without deploying to Kubernetes.

#### Prerequisites

- `tsh` (Teleport SSH client) installed and configured
- Access to qa01 and AWS clusters via Teleport (`tsh login`)
- AWS credentials configured (for S3 and Secrets Manager)
- Migration binary built:

  ```bash
  go build -o cmd/migration/migration ./cmd/migration
  ```

#### Quick Start

**1. Ensure you're logged into Teleport:**

```bash
tsh login
```

**NPE (Non-Production Environment) Examples:**

**Example 0: Use existing data (auto-detect tenant)**

```bash
# What it does:
#   - Creates SSH tunnel to NPE MariaDB (qa01)
#   - Auto-detects a tenant with existing data in MariaDB
#   - Exports tenant data from MariaDB → CSV files → S3
#   - Generates SQL file → S3
#   - Skips SQL execution (manager can execute SQL manually later)
./verify-migration.sh --use-tunnels qa01 \
  --mp-password <npe-mariadb-password> \
  --use-existing-data
# Flags:
#   --use-tunnels qa01: Use SSH tunnels for qa01 (NPE) environment (auto-configures jump/target/S3/region)
#   --mp-password: MariaDB password (required)
#   --use-existing-data: Use existing tenant data, auto-detect tenant
# Note: No --tenant-id = auto-detects tenant with data
# Note: No --aws-mysql-* flags = SQL execution skipped
```

**Example 1: Dump specific tenant to S3 (export only, no SQL execution)**

```bash
# What it does:
#   - Creates SSH tunnel to NPE MariaDB (qa01)
#   - Exports tenant 1016 data from MariaDB → CSV files → S3
#   - Generates SQL file → S3
#   - Skips SQL execution (manager can execute SQL manually later)
./verify-migration.sh --use-tunnels qa01 \
  --mp-password <npe-mariadb-password> \
  --tenant-id 1016
# Flags:
#   --use-tunnels qa01: Use SSH tunnels for qa01 (NPE) environment (auto-configures jump/target/S3/region)
#   --mp-password: MariaDB password (required)
#   --tenant-id: Tenant ID to export (required)
# Note: No --aws-mysql-* flags = SQL execution skipped
```

**Example 2: Dump specific tenant to S3 and execute SQL automatically**

```bash
# What it does:
#   - Creates SSH tunnels to NPE MariaDB and AWS Aurora (qa01)
#   - Exports tenant 1016 data from MariaDB → CSV files → S3
#   - Generates SQL file → S3
#   - Executes SQL on Aurora MySQL to load data automatically
#   - Verifies data in Aurora MySQL
./verify-migration.sh --use-tunnels qa01 \
  --mp-password <npe-mariadb-password> \
  --tenant-id 1016 \
  --aws-mysql-user fis \
  --aws-mysql-secret rds!cluster-d256b298-d749-488a-8e55-34018d6047dc \
  --aws-mysql-region us-east-1
# Flags:
#   --use-tunnels qa01: Auto-configures qa01 tunnel settings
#   --aws-mysql-user, --aws-mysql-secret, --aws-mysql-region: Required for SQL execution
#   --aws-database: Optional (defaults to fis_qa01)
```

**Example 3: Dump specific tenant with custom segments and parallelism**

```bash
# What it does:
#   - Same as Example 1, but with custom hash segmentation
#   - Uses 32 segments (instead of default 16) for finer-grained parallel processing
#   - Uses 16 parallel workers (instead of default 8) for faster export
./verify-migration.sh --use-tunnels qa01 \
  --mp-password <npe-mariadb-password> \
  --tenant-id 1016 \
  --segments 32 \
  --max-parallel-segments 16
# Flags:
#   --segments: Number of hash segments (default: 16)
#   --max-parallel-segments: Maximum parallel workers (default: 8)
# Note: More segments = finer-grained parallel processing, more workers = faster export
```

**PE (Production Environment - fr4) Examples:**

**Example 0: Use existing data (auto-detect tenant)**

```bash
# What it does:
#   - Creates SSH tunnel to PE MariaDB (fr4)
#   - Auto-detects a tenant with existing data in MariaDB
#   - Exports tenant data from PE MariaDB → CSV files → PE S3 bucket
#   - Generates SQL file → PE S3 bucket
#   - Skips SQL execution (manager will execute SQL on EC2)
./verify-migration.sh --use-tunnels fr4 \
  --mp-password <pe-mariadb-password> \
  --use-existing-data
# Flags:
#   --use-tunnels fr4: Use SSH tunnels for fr4 (PE) environment (auto-configures jump/target/S3/region)
#   --mp-password: MariaDB password (required)
#   --use-existing-data: Use existing tenant data, auto-detect tenant
# Note: No --tenant-id = auto-detects tenant with data (works in PE too)
# Note: No --aws-mysql-* flags = SQL execution skipped, manager executes on EC2
```

**Example 1: Dump specific tenant to S3 (export only, manager executes SQL on EC2)**

```bash
# What it does:
#   - Creates SSH tunnel to PE MariaDB (fr4)
#   - Exports tenant data from PE MariaDB → CSV files → PE S3 bucket
#   - Generates SQL file → PE S3 bucket
#   - Skips SQL execution (manager will execute SQL on EC2)
./verify-migration.sh --use-tunnels fr4 \
  --mp-password <pe-mariadb-password> \
  --tenant-id <tenant-id>
# Flags:
#   --use-tunnels fr4: Use SSH tunnels for fr4 (PE) environment (auto-configures jump/target/S3/region)
#   --mp-password: MariaDB password (required)
#   --tenant-id: Tenant ID (required for PE, no auto-detection)
# Note: No --aws-mysql-* flags = SQL execution skipped, manager executes on EC2
# Note: fr4 auto-configures:
#   - Jump: mariafis01-1.my1.fr4.nskope.net
#   - Target: mariafishavip.qa01-mp-npe.nc1.iad0.nsscloud.net
#   - S3 bucket: backup-bucket-maria-1
#   - Region: eu-central-1
```

**Example 2: Dump specific tenant to S3 and execute SQL automatically**

```bash
# What it does:
#   - Same as Example 1, but also executes SQL on Aurora MySQL automatically
#   - Creates SSH tunnel to PE Aurora MySQL (if needed)
#   - Executes LOAD DATA FROM S3 on Aurora MySQL
./verify-migration.sh --use-tunnels fr4 \
  --mp-password <pe-mariadb-password> \
  --tenant-id <tenant-id> \
  --aws-mysql-user fis \
  --aws-mysql-secret rds!cluster-dc527dd1-6e19-4aa4-a14f-c370fab851f1-ODQqsh \
  --aws-mysql-region eu-central-1
# Flags:
#   --use-tunnels fr4: Auto-configures fr4 tunnel settings
#   --aws-mysql-*: Required for SQL execution
#   --aws-database: Optional (defaults to fis)
```

**Example 3: Dump specific tenant with custom segments and parallelism**

```bash
# What it does:
#   - Same as Example 1, but with custom hash segmentation
#   - Uses 32 segments (instead of default 16) for finer-grained parallel processing
#   - Uses 16 parallel workers (instead of default 8) for faster export
./verify-migration.sh --use-tunnels fr4 \
  --mp-password <pe-mariadb-password> \
  --tenant-id <tenant-id> \
  --segments 32 \
  --max-parallel-segments 16
# Flags:
#   --segments: Number of hash segments (default: 16)
#   --max-parallel-segments: Maximum parallel workers (default: 8)
# Note: More segments = finer-grained parallel processing, more workers = faster export
```
