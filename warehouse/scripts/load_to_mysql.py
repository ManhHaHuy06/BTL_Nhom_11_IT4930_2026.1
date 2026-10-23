import os
import time
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
SCRIPT_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

# Cấu hình winutils cho môi trường Windows trước khi import PySpark
if os.name == "nt":
    hadoop_dir = SCRIPT_DIR / "hadoop"
    os.environ["HADOOP_HOME"] = str(hadoop_dir)
    os.environ["PATH"] += os.pathsep + str(hadoop_dir / "bin")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def get_latest_csv(data_dir: Path) -> Path:
    csv_files = list(data_dir.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"Không có file CSV nào trong thư mục {data_dir}")
    # Lấy file có thời gian chỉnh sửa mới nhất
    return max(csv_files, key=lambda p: p.stat().st_mtime)


def main():
    csv_file = get_latest_csv(BASE_DIR / "data")
    jar_path = SCRIPT_DIR / "mysql-connector-j-26.7.0.jar"

    print(f"[INFO] Khởi tạo SparkSession, đọc dữ liệu từ: {csv_file.name}")
    spark = (
        SparkSession.builder.appName("Wildfire_ETL_MySQL")
        .config("spark.jars", str(jar_path))
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    jdbc_url = os.getenv(
        "MYSQL_JDBC_URL",
        "jdbc:mysql://localhost:3307/wildfire_dw?rewriteBatchedStatements=true",
    )
    db_props = {
        "user": os.getenv("MYSQL_USER", "root"),
        "password": os.getenv("MYSQL_PASSWORD", "mynhsqlpass"),
        "driver": "com.mysql.cj.jdbc.Driver",
        "batchsize": "10000",
    }

    start_time = time.time()

    # Đọc CSV và đưa toàn bộ tên cột về chữ thường
    df_raw = spark.read.csv(str(csv_file), header=True, inferSchema=True)
    df_raw = df_raw.toDF(*[c.lower() for c in df_raw.columns])

    # Làm sạch dữ liệu & ép kiểu khớp với schema MySQL
    df_clean = (
        df_raw.dropna(subset=["latitude", "longitude", "datetime", "wildfire"])
        .withColumn("latitude", F.round(F.col("latitude").cast("double"), 6).cast("decimal(9,6)"))
        .withColumn("longitude", F.round(F.col("longitude").cast("double"), 6).cast("decimal(9,6)"))
        .withColumn("observed_at", F.to_timestamp("datetime"))
        .withColumn("full_date", F.to_date("observed_at"))
        .withColumn("date_key", F.date_format("observed_at", "yyyyMMdd").cast("int"))
        .withColumn("year_num", F.year("observed_at").cast("smallint"))
        .withColumn("month_num", F.month("observed_at").cast("tinyint"))
        .withColumn("day_num", F.dayofmonth("observed_at").cast("tinyint"))
        .withColumn(
            "wildfire",
            F.when(
                F.lower(F.col("wildfire").cast("string")).isin("1", "1.0", "true", "yes"), 1
            )
            .otherwise(0)
            .cast("tinyint"),
        )
        .filter(
            F.col("latitude").between(-90, 90)
            & F.col("longitude").between(-180, 180)
            & F.col("observed_at").isNotNull()
        )
    )

    # Cache DataFrame vì cần tái sử dụng cho 3 bảng (dim_date, dim_location, fact_wildfire)
    df_clean.cache()

    # 1. Nạp dim_date (loại bỏ các ngày đã tồn tại bằng left_anti join)
    print("[INFO] Đang cập nhật bảng dim_date...")
    existing_dates = spark.read.jdbc(url=jdbc_url, table="dim_date", properties=db_props).select("date_key")
    df_dim_date = (
        df_clean.select("date_key", "full_date", "year_num", "month_num", "day_num")
        .distinct()
        .join(existing_dates, on="date_key", how="left_anti")
    )
    df_dim_date.write.jdbc(url=jdbc_url, table="dim_date", mode="append", properties=db_props)

    # 2. Nạp dim_location (loại bỏ các tọa độ đã tồn tại)
    print("[INFO] Đang cập nhật bảng dim_location...")
    existing_locs = spark.read.jdbc(url=jdbc_url, table="dim_location", properties=db_props).select("latitude", "longitude")
    df_dim_loc = (
        df_clean.select("latitude", "longitude")
        .distinct()
        .join(existing_locs, on=["latitude", "longitude"], how="left_anti")
    )
    df_dim_loc.write.jdbc(url=jdbc_url, table="dim_location", mode="append", properties=db_props)

    # 3. Map khóa ngoại location_key và nạp bảng fact_wildfire
    print("[INFO] Đang ánh xạ khóa và nạp bảng fact_wildfire...")
    all_locs = spark.read.jdbc(url=jdbc_url, table="dim_location", properties=db_props).select(
        "location_key", "latitude", "longitude"
    )

    fact_cols = [
        "date_key", "location_key", "observed_at", "wildfire",
        "pr", "rmax", "rmin", "sph", "srad", "tmmn", "tmmx", "vs",
        "bi", "fm100", "fm1000", "erc", "etr", "pet", "vpd",
    ]

    df_fact = df_clean.join(F.broadcast(all_locs), on=["latitude", "longitude"], how="inner").select(*fact_cols)
    df_fact.write.jdbc(url=jdbc_url, table="fact_wildfire", mode="append", properties=db_props)

    df_clean.unpersist()
    elapsed = round(time.time() - start_time, 2)
    print(f"[INFO] Hoàn tất quá trình ETL trong {elapsed} giây.")
    spark.stop()


if __name__ == "__main__":
    main()