
USE wildfire_dw;

-- 1. Tổng số quan sát và số quan sát có cháy
SELECT
    COUNT(*) AS total_records,
    SUM(wildfire = 1) AS fire_records,
    SUM(wildfire = 0) AS no_fire_records
FROM fact_wildfire;

-- 2. Thống kê cháy rừng theo năm
SELECT
    d.year_num,
    COUNT(*) AS total_records,
    SUM(f.wildfire = 1) AS fire_records,
    ROUND(AVG(f.wildfire) * 100, 2) AS fire_rate_pct
FROM fact_wildfire f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.year_num
ORDER BY d.year_num;

-- 3. Nhiệt độ trung bình theo năm và nhãn cháy
SELECT
    d.year_num,
    f.wildfire,
    ROUND(AVG(f.tmmx), 2) AS avg_max_temp,
    ROUND(AVG(f.pr), 2) AS avg_precipitation
FROM fact_wildfire f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.year_num, f.wildfire
ORDER BY d.year_num, f.wildfire;

-- 4. Truy vấn dữ liệu theo vị trí địa lý
SELECT
    d.full_date,
    l.latitude,
    l.longitude,
    f.wildfire,
    f.tmmx,
    f.pr,
    f.vpd
FROM fact_wildfire f
JOIN dim_date d ON f.date_key = d.date_key
JOIN dim_location l ON f.location_key = l.location_key
LIMIT 100;
