-- Initialize MariaDB for testing
-- This script runs automatically when MariaDB container starts

CREATE DATABASE IF NOT EXISTS fis;
USE fis;

-- Create fis_aggr table with original schema
CREATE TABLE IF NOT EXISTS fis_aggr (
    tenantid INT NOT NULL,
    hash VARCHAR(255) NOT NULL,
    aggr LONGTEXT NOT NULL,
    UNIQUE(tenantid, hash)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Check and add last_modified column if it doesn't exist
-- ALGORITHM=INSTANT minimizes locking by only modifying metadata (supported in MariaDB 10.3+ and MySQL 8.0.12+)
SET @dbname = DATABASE();
SET @tablename = 'fis_aggr';
SET @columnname = 'last_modified';
SET @preparedStatement = (SELECT IF(
    (
        SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = @dbname
        AND TABLE_NAME = @tablename
        AND COLUMN_NAME = @columnname
    ) > 0,
    'SELECT 1', -- Column exists, do nothing
    CONCAT('ALTER TABLE ', @tablename, ' ADD COLUMN ', @columnname, ' TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, ALGORITHM=INSTANT')
));
PREPARE alterIfNotExists FROM @preparedStatement;
EXECUTE alterIfNotExists;
DEALLOCATE PREPARE alterIfNotExists;

-- Check and add version column if it doesn't exist
SET @columnname = 'version';
SET @preparedStatement = (SELECT IF(
    (
        SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = @dbname
        AND TABLE_NAME = @tablename
        AND COLUMN_NAME = @columnname
    ) > 0,
    'SELECT 1', -- Column exists, do nothing
    CONCAT('ALTER TABLE ', @tablename, ' ADD COLUMN ', @columnname, ' INT NULL, ALGORITHM=INSTANT')
));
PREPARE alterIfNotExists FROM @preparedStatement;
EXECUTE alterIfNotExists;
DEALLOCATE PREPARE alterIfNotExists;

-- Add indexes for better query performance (if they don't exist)
-- Check and add idx_hash index if it doesn't exist
SET @indexname = 'idx_hash';
SET @preparedStatement = (SELECT IF(
    (
        SELECT COUNT(*) FROM INFORMATION_SCHEMA.STATISTICS
        WHERE TABLE_SCHEMA = @dbname
        AND TABLE_NAME = @tablename
        AND INDEX_NAME = @indexname
    ) > 0,
    'SELECT 1', -- Index exists, do nothing
    CONCAT('ALTER TABLE ', @tablename, ' ADD INDEX ', @indexname, ' (hash)')
));
PREPARE alterIfNotExists FROM @preparedStatement;
EXECUTE alterIfNotExists;
DEALLOCATE PREPARE alterIfNotExists;

-- Check and add idx_tenant_hash index if it doesn't exist
SET @indexname = 'idx_tenant_hash';
SET @preparedStatement = (SELECT IF(
    (
        SELECT COUNT(*) FROM INFORMATION_SCHEMA.STATISTICS
        WHERE TABLE_SCHEMA = @dbname
        AND TABLE_NAME = @tablename
        AND INDEX_NAME = @indexname
    ) > 0,
    'SELECT 1', -- Index exists, do nothing
    CONCAT('ALTER TABLE ', @tablename, ' ADD INDEX ', @indexname, ' (tenantid, hash)')
));
PREPARE alterIfNotExists FROM @preparedStatement;
EXECUTE alterIfNotExists;
DEALLOCATE PREPARE alterIfNotExists;

-- Insert some test data
INSERT INTO fis_aggr (tenantid, hash, aggr, version) VALUES
(1016, '00112233445566778899aabbccddeeff', '{"locs":{"app1":[{"inst":"inst1","ts":1767109960}]}}', 1),
(1016, '00aabbccddeeff001122334455667788', '{"locs":{"app2":[{"inst":"inst2","ts":1767109961}]}}', 1),
(1016, '00ffeeddccbbaa998877665544332211', '{"locs":{"app3":[{"inst":"inst3","ts":1767109962}]}}', 1),
(1016, '112233445566778899aabbccddeeff00', '{"locs":{"app4":[{"inst":"inst4","ts":1767109963}]}}', 1),
(1016, '2233445566778899aabbccddeeff0011', '{"locs":{"app5":[{"inst":"inst5","ts":1767109964}]}}', 1);
