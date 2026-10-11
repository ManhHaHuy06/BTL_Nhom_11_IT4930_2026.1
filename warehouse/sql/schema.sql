
USE wildfire_dw;

-- 1. Bảng chiều thời gian
CREATE TABLE IF NOT EXISTS dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    year_num SMALLINT NOT NULL,
    month_num TINYINT NOT NULL,
    day_num TINYINT NOT NULL
);

-- 2. Bảng chiều vị trí địa lý
CREATE TABLE IF NOT EXISTS dim_location (
    location_key BIGINT AUTO_INCREMENT PRIMARY KEY,
    latitude DECIMAL(9,6) NOT NULL,
    longitude DECIMAL(9,6) NOT NULL,
    UNIQUE KEY uq_coordinates (latitude, longitude)
);

-- 3. Bảng fact lưu dữ liệu quan sát cháy rừng
CREATE TABLE IF NOT EXISTS fact_wildfire (
    fact_id BIGINT AUTO_INCREMENT PRIMARY KEY,

    date_key INT NOT NULL,
    location_key BIGINT NOT NULL,
    observed_at DATETIME NOT NULL,

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

    CONSTRAINT fk_wildfire_date
        FOREIGN KEY (date_key)
        REFERENCES dim_date(date_key),

    CONSTRAINT fk_wildfire_location
        FOREIGN KEY (location_key)
        REFERENCES dim_location(location_key),

    CONSTRAINT chk_wildfire_label
        CHECK (wildfire IN (0, 1))
);
