-- Initialize MariaDB for testing
-- This script runs automatically when MariaDB container starts

CREATE DATABASE IF NOT EXISTS fis;
USE fis;

-- Create fis_aggr table (simplified for testing)
CREATE TABLE IF NOT EXISTS fis_aggr (
    tenantid INT NOT NULL,
    hash VARCHAR(255) NOT NULL,
    aggr LONGTEXT NOT NULL,
    version INT NULL,
    last_modified TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE(tenantid, hash),
    INDEX idx_hash (hash),
    INDEX idx_tenant_hash (tenantid, hash)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Insert some test data
INSERT INTO fis_aggr (tenantid, hash, aggr, version) VALUES
(1016, '00112233445566778899aabbccddeeff', '{"locs":{"app1":[{"inst":"inst1","ts":1767109960}]}}', 1),
(1016, '00aabbccddeeff001122334455667788', '{"locs":{"app2":[{"inst":"inst2","ts":1767109961}]}}', 1),
(1016, '00ffeeddccbbaa998877665544332211', '{"locs":{"app3":[{"inst":"inst3","ts":1767109962}]}}', 1),
(1016, '112233445566778899aabbccddeeff00', '{"locs":{"app4":[{"inst":"inst4","ts":1767109963}]}}', 1),
(1016, '2233445566778899aabbccddeeff0011', '{"locs":{"app5":[{"inst":"inst5","ts":1767109964}]}}', 1);

