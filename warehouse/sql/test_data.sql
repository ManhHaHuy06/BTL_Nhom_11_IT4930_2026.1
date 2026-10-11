
USE wildfire_dw;

-- 1. Thêm ngày quan sát
INSERT INTO dim_date
(date_key, full_date, year_num, month_num, day_num)
VALUES
(20240815, '2024-08-15', 2024, 8, 15);

-- 2. Thêm vị trí địa lý
INSERT INTO dim_location (latitude, longitude)
VALUES (40.000000, -120.000000);

-- 3. Thêm một quan sát cháy rừng giả lập
INSERT INTO fact_wildfire (
    date_key,
    location_key,
    observed_at,
    wildfire,
    pr,
    tmmx,
    rmin,
    vs,
    vpd
)
SELECT
    20240815,
    location_key,
    '2024-08-15 12:00:00',
    0,
    0.0,
    35.0,
    20.0,
    3.2,
    2.4
FROM dim_location
WHERE latitude = 40.000000
  AND longitude = -120.000000;
