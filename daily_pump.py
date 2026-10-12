"""Copy at most one CSV record batch from Kaggle to MinIO per invocation."""

from __future__ import annotations

import csv
import hashlib
import itertools
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path

DATASET = "firecastrl/us-wildfire-dataset"
STATE_KEY = "wildfire_pipeline/pointer.json"


def required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def is_missing(error: Exception) -> bool:
    response = getattr(error, "response", {})
    return response.get("Error", {}).get("Code") in {
        "404", "NoSuchBucket", "NoSuchKey", "NotFound"
    }


def ensure_bucket(s3, bucket: str) -> None:
    try:
        s3.head_bucket(Bucket=bucket)
    except Exception as error:
        if not is_missing(error):
            raise
        s3.create_bucket(Bucket=bucket)


def read_state(s3, bucket: str) -> dict | None:
    try:
        response = s3.get_object(Bucket=bucket, Key=STATE_KEY)
    except Exception as error:
        if is_missing(error):
            return None
        raise
    state = json.loads(response["Body"].read())
    if (
        not isinstance(state, dict)
        or type(state.get("last_row")) is not int
        or state["last_row"] < 0
        or not isinstance(state.get("source_sha256"), str)
        or len(state["source_sha256"]) != 64
        or not isinstance(state.get("source_file"), str)
    ):
        raise ValueError(f"Invalid state object: {bucket}/{STATE_KEY}")
    return state


def write_state(s3, bucket: str, row: int, digest: str, source_file: str,
                complete: bool) -> None:
    state = {
        "dataset": DATASET,
        "source_file": source_file,
        "source_sha256": digest,
        "last_row": row,
        "complete": complete,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    s3.put_object(
        Bucket=bucket,
        Key=STATE_KEY,
        Body=json.dumps(state, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
    )


def find_csv(download_dir: Path) -> Path:
    files = sorted(download_dir.rglob("*.csv"))
    requested = os.getenv("DATASET_CSV")
    if requested:
        files = [path for path in files if path.relative_to(download_dir).as_posix() == requested]
    if len(files) != 1:
        names = ", ".join(path.relative_to(download_dir).as_posix() for path in files)
        raise ValueError(
            f"Expected exactly one CSV (or set DATASET_CSV); found {len(files)}: {names}"
        )
    return files[0]


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def extract_batch(source_path: Path, output_path: Path, start_row: int,
                  chunk_size: int) -> tuple[int, bool]:
    """Skip CSV records, not physical lines; preserve multiline field boundaries."""
    count = 0
    with source_path.open("r", encoding="utf-8-sig", newline="") as source:
        reader = csv.reader(source)
        header = next(reader, None)
        if not header:
            raise ValueError("Source CSV is empty or has no header")
        for _ in itertools.islice(reader, start_row):
            pass
        with output_path.open("w", encoding="utf-8", newline="") as output:
            writer = csv.writer(output, lineterminator="\n")
            writer.writerow(header)
            for record in itertools.islice(reader, chunk_size):
                writer.writerow(record)
                count += 1
        complete = next(reader, None) is None
    return count, complete


def upload_batch(s3, bucket: str, key: str, batch_path: Path,
                 metadata: dict[str, str]) -> None:
    try:
        existing = s3.head_object(Bucket=bucket, Key=key)
    except Exception as error:
        if not is_missing(error):
            raise
    else:
        if existing.get("Metadata", {}) != metadata:
            raise ValueError(f"Existing object has different metadata: {bucket}/{key}")
        print(f"Batch already uploaded: {bucket}/{key}")
        return
    s3.upload_file(
        str(batch_path), bucket, key,
        ExtraArgs={"ContentType": "text/csv", "Metadata": metadata},
    )
    print(f"Uploaded: {bucket}/{key}")


def main() -> None:
    from dotenv import load_dotenv

    # Local development only; existing environment variables (GitHub secrets) win.
    load_dotenv(dotenv_path=Path(__file__).with_name(".env"), override=False)

    import boto3
    from botocore.client import Config
    from kaggle.api.kaggle_api_extended import KaggleApi

    endpoint = required_env("MINIO_ENDPOINT")
    access_key = required_env("MINIO_ACCESS_KEY")
    secret_key = required_env("MINIO_SECRET_KEY")
    if not endpoint.startswith(("http://", "https://")):
        raise ValueError("MINIO_ENDPOINT must start with http:// or https://")
    chunk_size = int(os.getenv("CHUNK_SIZE", "1000000"))
    if chunk_size <= 0:
        raise ValueError("CHUNK_SIZE must be a positive integer")
    data_bucket = os.getenv("DATA_BUCKET", "raw-zone")
    state_bucket = os.getenv("STATE_BUCKET", "system-state")

    s3 = boto3.client(
        "s3", endpoint_url=endpoint,
        aws_access_key_id=access_key, aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        region_name="us-east-1",
    )
    api = KaggleApi()
    api.authenticate()

    with tempfile.TemporaryDirectory(prefix="wildfire_kaggle_") as temp_dir:
        work_dir = Path(temp_dir)
        download_dir = work_dir / "download"
        download_dir.mkdir()
        print(f"Downloading Kaggle dataset {DATASET}...")
        api.dataset_download_files(DATASET, path=str(download_dir), unzip=True)
        source_path = find_csv(download_dir)
        source_file = source_path.relative_to(download_dir).as_posix()
        digest = sha256_file(source_path)

        ensure_bucket(s3, state_bucket)
        state = read_state(s3, state_bucket)
        if state and (
            state["source_sha256"] != digest or state["source_file"] != source_file
        ):
            raise RuntimeError(
                "Kaggle source changed. Review the new CSV before resetting the pointer; "
                "the old MinIO objects are preserved."
            )
        start_row = state["last_row"] if state else 0
        if state and state.get("complete"):
            print(f"Dataset already complete: {start_row} records")
            return

        batch_path = work_dir / "batch.csv"
        count, complete = extract_batch(source_path, batch_path, start_row, chunk_size)
        if count:
            ensure_bucket(s3, data_bucket)
            end_row = start_row + count
            key = (
                f"wildfire/source_sha256={digest}/"
                f"rows_{start_row + 1:012d}_{end_row:012d}.csv"
            )
            metadata = {
                "source-sha256": digest,
                "row-start": str(start_row + 1),
                "row-end": str(end_row),
            }
            upload_batch(s3, data_bucket, key, batch_path, metadata)
        else:
            end_row = start_row
        write_state(s3, state_bucket, end_row, digest, source_file, complete)
        print(f"Pointer: {end_row}; complete: {complete}")


if __name__ == "__main__":
    main()
