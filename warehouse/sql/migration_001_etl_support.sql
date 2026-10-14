-- Run once against databases created with the original warehouse schema.
-- The statements are safe to re-run; the original fact rows keep NULL source IDs.
USE wildfire_dw;

SET @has_source_record_id = (
    SELECT COUNT(*) FROM information_schema.columns
    WHERE table_schema = DATABASE() AND table_name = 'fact_wildfire'
      AND column_name = 'source_record_id'
);
SET @ddl = IF(@has_source_record_id = 0,
    'ALTER TABLE fact_wildfire ADD COLUMN source_record_id CHAR(64) NULL',
    'SELECT 1');
PREPARE migration_stmt FROM @ddl;
EXECUTE migration_stmt;
DEALLOCATE PREPARE migration_stmt;

SET @has_source_sha256 = (
    SELECT COUNT(*) FROM information_schema.columns
    WHERE table_schema = DATABASE() AND table_name = 'fact_wildfire'
      AND column_name = 'source_sha256'
);
SET @ddl = IF(@has_source_sha256 = 0,
    'ALTER TABLE fact_wildfire ADD COLUMN source_sha256 CHAR(64) NULL',
    'SELECT 1');
PREPARE migration_stmt FROM @ddl;
EXECUTE migration_stmt;
DEALLOCATE PREPARE migration_stmt;

SET @has_source_object_key = (
    SELECT COUNT(*) FROM information_schema.columns
    WHERE table_schema = DATABASE() AND table_name = 'fact_wildfire'
      AND column_name = 'source_object_key'
);
SET @ddl = IF(@has_source_object_key = 0,
    'ALTER TABLE fact_wildfire ADD COLUMN source_object_key VARCHAR(512) NULL',
    'SELECT 1');
PREPARE migration_stmt FROM @ddl;
EXECUTE migration_stmt;
DEALLOCATE PREPARE migration_stmt;

SET @has_source_index = (
    SELECT COUNT(*) FROM information_schema.statistics
    WHERE table_schema = DATABASE() AND table_name = 'fact_wildfire'
      AND index_name = 'uq_fact_source_record'
);
SET @ddl = IF(@has_source_index = 0,
    'ALTER TABLE fact_wildfire ADD UNIQUE KEY uq_fact_source_record (source_record_id)',
    'SELECT 1');
PREPARE migration_stmt FROM @ddl;
EXECUTE migration_stmt;
DEALLOCATE PREPARE migration_stmt;

CREATE TABLE IF NOT EXISTS etl_batch (
    source_object_key VARCHAR(512) PRIMARY KEY,
    source_sha256 CHAR(64) NOT NULL,
    row_start BIGINT UNSIGNED NOT NULL,
    row_end BIGINT UNSIGNED NOT NULL,
    status ENUM('RUNNING', 'SUCCEEDED', 'FAILED') NOT NULL,
    run_id CHAR(36) NOT NULL,
    records_read BIGINT UNSIGNED NOT NULL DEFAULT 0,
    records_accepted BIGINT UNSIGNED NOT NULL DEFAULT 0,
    records_rejected BIGINT UNSIGNED NOT NULL DEFAULT 0,
    facts_inserted BIGINT UNSIGNED NOT NULL DEFAULT 0,
    started_at DATETIME NOT NULL,
    finished_at DATETIME NULL,
    error_message TEXT NULL,
    INDEX idx_etl_batch_status (status)
);

CREATE TABLE IF NOT EXISTS stg_wildfire (
    run_id CHAR(36) NOT NULL,
    source_object_key VARCHAR(512) NOT NULL,
    source_record_id CHAR(64) NOT NULL,
    source_sha256 CHAR(64) NOT NULL,
    observed_at DATETIME NOT NULL,
    latitude DECIMAL(9,6) NOT NULL,
    longitude DECIMAL(9,6) NOT NULL,
    wildfire TINYINT NOT NULL,
    pr DOUBLE NULL,
    rmax DOUBLE NULL,
    rmin DOUBLE NULL,
    sph DOUBLE NULL,
    srad DOUBLE NULL,
    tmmn DOUBLE NULL,
    tmmx DOUBLE NULL,
    vs DOUBLE NULL,
    bi DOUBLE NULL,
    fm100 DOUBLE NULL,
    fm1000 DOUBLE NULL,
    erc DOUBLE NULL,
    etr DOUBLE NULL,
    pet DOUBLE NULL,
    vpd DOUBLE NULL,
    INDEX idx_stg_run (run_id),
    INDEX idx_stg_source_record (source_record_id)
);

CREATE TABLE IF NOT EXISTS stg_wildfire_reject (
    run_id CHAR(36) NOT NULL,
    source_record_id CHAR(64) NOT NULL,
    source_object_key VARCHAR(512) NOT NULL,
    source_sha256 CHAR(64) NOT NULL,
    rejection_reason VARCHAR(1000) NOT NULL,
    raw_payload LONGTEXT NOT NULL,
    INDEX idx_stg_reject_run (run_id),
    INDEX idx_stg_reject_source (source_object_key)
);

CREATE TABLE IF NOT EXISTS etl_reject (
    source_record_id CHAR(64) PRIMARY KEY,
    source_object_key VARCHAR(512) NOT NULL,
    source_sha256 CHAR(64) NOT NULL,
    rejection_reason VARCHAR(1000) NOT NULL,
    raw_payload LONGTEXT NOT NULL,
    rejected_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_etl_reject_source (source_object_key)
);
