
USE wildfire_dw;

-- 1. Kiểm tra số lượng bản ghi và phân bố nhãn
SELECT
    COUNT(*) AS total_records,
    SUM(wildfire = 1) AS fire_count,
    SUM(wildfire = 0) AS no_fire_count
FROM fact_wildfire;

-- 2. Kiểm tra dữ liệu thời tiết bị NULL
SELECT
    COUNT(*) AS total_records,
    SUM(tmmx IS NULL) AS missing_temperature,
    SUM(pr IS NULL) AS missing_precipitation,
    SUM(vpd IS NULL) AS missing_vpd
FROM fact_wildfire;

-- 3. Kiểm tra tọa độ địa lý không hợp lệ
SELECT
    COUNT(*) AS invalid_locations
FROM dim_location
WHERE latitude NOT BETWEEN -90 AND 90
   OR longitude NOT BETWEEN -180 AND 180;

-- 4. Kiểm tra sự nhất quán giữa ngày quan sát và dimension
SELECT
    COUNT(*) AS mismatched_dates
FROM fact_wildfire f
JOIN dim_date d ON f.date_key = d.date_key
WHERE DATE(f.observed_at) <> d.full_date;

-- 5. Kiểm tra dữ liệu trùng vị trí và thời điểm
SELECT
    date_key,
    location_key,
    observed_at,
    COUNT(*) AS duplicate_count
FROM fact_wildfire
GROUP BY date_key, location_key, observed_at
HAVING COUNT(*) > 1
LIMIT 10;
