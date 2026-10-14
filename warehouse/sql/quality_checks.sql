
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

-- 6. Kiểm tra batch ETL lỗi hoặc chưa hoàn thành
SELECT status, COUNT(*) AS batch_count
FROM etl_batch
GROUP BY status;

-- 7. Kiểm tra số dòng bị loại theo lý do
SELECT rejection_reason, COUNT(*) AS rejected_records
FROM etl_reject
GROUP BY rejection_reason
ORDER BY rejected_records DESC;

-- 8. So sánh tổng dòng đã đọc với số dòng hợp lệ và bị loại
SELECT
    SUM(records_read) AS records_read,
    SUM(records_accepted) AS records_accepted,
    SUM(records_rejected) AS records_rejected,
    SUM(CAST(records_read AS SIGNED)
        - CAST(records_accepted AS SIGNED)
        - CAST(records_rejected AS SIGNED)) AS unaccounted_records
FROM etl_batch
WHERE status = 'SUCCEEDED';

-- 9. Các fact cũ hoặc dữ liệu thử không có lineage ETL
SELECT COUNT(*) AS facts_without_source_identity
FROM fact_wildfire
WHERE source_record_id IS NULL;
