import os
from pathlib import Path
import boto3
import pandas as pd
from botocore.config import Config
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"


def get_s3_client():
    load_dotenv(BASE_DIR / ".env")
    return boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT"],
        aws_access_key_id=os.environ["S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["S3_SECRET_KEY"],
        region_name=os.getenv("S3_REGION", "ap-singapore-1"),
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
            request_checksum_calculation="when_required",
        ),
    )


def main():
    s3 = get_s3_client()
    bucket = os.getenv("DATA_BUCKET", "raw-zone")

    # Lấy danh sách các file CSV trong phân vùng wildfire/
    paginator = s3.get_paginator("list_objects_v2")
    csv_objects = [
        item
        for page in paginator.paginate(Bucket=bucket, Prefix="wildfire/")
        for item in page.get("Contents", [])
        if item["Key"].lower().endswith(".csv")
    ]

    if not csv_objects:
        raise RuntimeError(f"Không tìm thấy file CSV nào trong bucket '{bucket}'!")

    # Sắp xếp để lấy file mới nhất theo thời gian cập nhật
    csv_objects.sort(key=lambda x: x["LastModified"], reverse=True)
    latest_key = csv_objects[0]["Key"]

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    save_path = DATA_DIR / Path(latest_key).name

    print(f"[INFO] Đang tải: {latest_key} ...")
    s3.download_file(bucket, latest_key, str(save_path))
    print(f"[INFO] Hoàn tất. Đã lưu tại: {save_path}")

    # Kiểm tra nhanh cấu trúc dữ liệu vừa tải
    df_sample = pd.read_csv(save_path, nrows=5)
    print("\n--- Các cột dữ liệu ---\n", list(df_sample.columns))
    print("\n--- 2 dòng đầu tiên ---\n", df_sample.head(2))


if __name__ == "__main__":
    main()