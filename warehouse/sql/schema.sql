
CREATE DATABASE IF NOT EXISTS wildfire_dw
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_0900_ai_ci;
USE wildfire_dw;

-- 1. Bảng chiều thời gian
CREATE TABLE IF NOT EXISTS dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    year_num SMALLINT NOT NULL,
    month_num TINYINT NOT NULL,
    day_num TINYINT NOT NULL,
    CONSTRAINT chk_dim_date_parts CHECK (
        month_num BETWEEN 1 AND 12
        AND day_num BETWEEN 1 AND 31
        AND date_key = YEAR(full_date) * 10000 + MONTH(full_date) * 100 + DAY(full_date)
    )
);

-- 2. Bảng chiều vị trí địa lý
CREATE TABLE IF NOT EXISTS dim_location (
    location_key BIGINT AUTO_INCREMENT PRIMARY KEY,
    latitude DECIMAL(9,6) NOT NULL,
    longitude DECIMAL(9,6) NOT NULL,
    UNIQUE KEY uq_coordinates (latitude, longitude),
    CONSTRAINT chk_dim_location_latitude CHECK (latitude BETWEEN -90 AND 90),
    CONSTRAINT chk_dim_location_longitude CHECK (longitude BETWEEN -180 AND 180)
);

-- 3. Bảng fact lưu dữ liệu quan sát cháy rừng
CREATE TABLE IF NOT EXISTS fact_wildfire (
    fact_id BIGINT AUTO_INCREMENT PRIMARY KEY,

    date_key INT NOT NULL,
    location_key BIGINT NOT NULL,
    observed_at DATETIME NOT NULL,

    -- Stable SHA-256 identity computed from the immutable source object and row payload.
    -- NULL remains allowed for legacy/local synthetic rows; ETL always supplies it.
    source_record_id CHAR(64) NULL,
    source_sha256 CHAR(64) NULL,
    source_object_key VARCHAR(512) NULL,

    wildfire TINYINT NOT NULL,

    -- Các biến thời tiết
    pr DOUBLE,
    rmax DOUBLE,
    rmin DOUBLE,
    sph DOUBLE,
    srad DOUBLE,
    tmmn DOUBLE,
    tmmx DOUBLE,
    vs DOUBLE,

    -- Các chỉ số nguy cơ cháy
    bi DOUBLE,
    fm100 DOUBLE,
    fm1000 DOUBLE,
    erc DOUBLE,

    -- Cân bằng nước và độ khô
    etr DOUBLE,
    pet DOUBLE,
    vpd DOUBLE,

    INDEX idx_fact_date (date_key),
    INDEX idx_fact_location (location_key),
    UNIQUE KEY uq_fact_source_record (source_record_id),

    CONSTRAINT fk_wildfire_date
        FOREIGN KEY (date_key)
        REFERENCES dim_date(date_key),

    CONSTRAINT fk_wildfire_location
        FOREIGN KEY (location_key)
        REFERENCES dim_location(location_key),

    CONSTRAINT chk_wildfire_label
        CHECK (wildfire IN (0, 1))
);

-- One row per immutable source CSV object. A succeeded object is skipped on later runs.
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

-- Transient MySQL landing table. Successful runs remove their staging rows.
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

-- Rejected rows also pass through a non-unique staging table so Spark task retries
-- cannot fail on duplicate inserts; the final transaction de-duplicates them.
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

-- Rejected source rows remain available for diagnosis after staging is cleared.
CREATE TABLE IF NOT EXISTS etl_reject (
    source_record_id CHAR(64) PRIMARY KEY,
    source_object_key VARCHAR(512) NOT NULL,
    source_sha256 CHAR(64) NOT NULL,
    rejection_reason VARCHAR(1000) NOT NULL,
    raw_payload LONGTEXT NOT NULL,
    rejected_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_etl_reject_source (source_object_key)
);
