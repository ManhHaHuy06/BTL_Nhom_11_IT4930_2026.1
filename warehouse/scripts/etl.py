"""Load immutable wildfire CSV batches from OCI Object Storage into MySQL."""

from __future__ import annotations

import argparse
import logging
import os
import re
import uuid
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

LOGGER = logging.getLogger("wildfire_etl")
SOURCE_PREFIX = "wildfire/"
BATCH_PATTERN = re.compile(
    r"^wildfire/source_sha256=([0-9a-f]{64})/rows_(\d+)_(\d+)\.csv$"
)

FEATURE_COLUMNS = (
    "pr", "rmax", "rmin", "sph", "srad", "tmmn", "tmmx", "vs",
    "bi", "fm100", "fm1000", "erc", "etr", "pet", "vpd",
)
SOURCE_COLUMNS = ("latitude", "longitude", "datetime", "wildfire", *FEATURE_COLUMNS)
STAGE_COLUMNS = (
    "run_id", "source_object_key", "source_record_id", "source_sha256",
    "observed_at", "latitude", "longitude", "wildfire", *FEATURE_COLUMNS,
)
REJECT_COLUMNS = (
    "run_id", "source_record_id", "source_object_key", "source_sha256",
    "rejection_reason", "raw_payload",
)


@dataclass(frozen=True)
class SourceBatch:
    key: str
    source_sha256: str
    row_start: int
    row_end: int

    @property
    def expected_rows(self) -> int:
        return self.row_end - self.row_start + 1


def get_config(require_mysql: bool = True) -> dict[str, str | int]:
    load_dotenv(Path(__file__).resolve().parents[1] / ".env", override=False)
    required = ["S3_ENDPOINT", "S3_ACCESS_KEY", "S3_SECRET_KEY"]
    if require_mysql:
        required.extend(("MYSQL_HOST", "MYSQL_USER", "MYSQL_PASSWORD"))
    missing = [name for name in required if not os.getenv(name)]
    if missing:
        raise ValueError(
            "Missing required settings: " + ", ".join(missing)
            + ". Copy warehouse/.env.example to warehouse/.env and fill it locally."
        )
    return {
        "s3_endpoint": os.environ["S3_ENDPOINT"].rstrip("/"),
        "s3_region": os.getenv("S3_REGION", "ap-singapore-1"),
        "s3_access_key": os.environ["S3_ACCESS_KEY"],
        "s3_secret_key": os.environ["S3_SECRET_KEY"],
        "data_bucket": os.getenv("DATA_BUCKET", "raw-zone"),
        "mysql_host": os.getenv("MYSQL_HOST", ""),
        "mysql_port": int(os.getenv("MYSQL_PORT", "3306")),
        "mysql_database": os.getenv("MYSQL_DATABASE", "wildfire_dw"),
        "mysql_user": os.getenv("MYSQL_USER", ""),
        "mysql_password": os.getenv("MYSQL_PASSWORD", ""),
        "jdbc_batch_size": int(os.getenv("ETL_JDBC_BATCH_SIZE", "5000")),
        "jdbc_partitions": int(os.getenv("ETL_JDBC_PARTITIONS", "2")),
        "spark_master": os.getenv("SPARK_MASTER", "local[*]"),
        "spark_jars_packages": os.getenv("SPARK_JARS_PACKAGES", ""),
        "mysql_use_ssl": os.getenv("MYSQL_USE_SSL", "false").lower(),
    }


def make_s3_client(config: dict[str, str | int]):
    import boto3
    from botocore.client import Config

    return boto3.client(
        "s3",
        endpoint_url=config["s3_endpoint"],
        aws_access_key_id=config["s3_access_key"],
        aws_secret_access_key=config["s3_secret_key"],
        region_name=config["s3_region"],
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
            request_checksum_calculation="when_required",
        ),
    )


def discover_batches(s3, bucket: str) -> list[SourceBatch]:
    paginator = s3.get_paginator("list_objects_v2")
    batches: list[SourceBatch] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=SOURCE_PREFIX):
        for item in page.get("Contents", []):
            match = BATCH_PATTERN.fullmatch(item["Key"])
            if not match:
                continue
            source_sha256, row_start, row_end = match.groups()
            batch = SourceBatch(item["Key"], source_sha256, int(row_start), int(row_end))
            if batch.row_start < 1 or batch.row_end < batch.row_start:
                raise ValueError(f"Invalid row range in source object: {batch.key}")
            metadata = s3.head_object(Bucket=bucket, Key=batch.key).get("Metadata", {})
            expected_metadata = {
                "source-sha256": batch.source_sha256,
                "row-start": str(batch.row_start),
                "row-end": str(batch.row_end),
            }
            if any(metadata.get(key) != value for key, value in expected_metadata.items()):
                raise ValueError(f"Object metadata does not match its path: {batch.key}")
            batches.append(batch)
    batches.sort(key=lambda batch: (batch.source_sha256, batch.row_start))
    source_versions = {batch.source_sha256 for batch in batches}
    if len(source_versions) > 1:
        raise ValueError(
            "Multiple source dataset hashes are present under wildfire/. Refusing to mix "
            "dataset versions; review the crawler pointer and choose/reset the intended version."
        )

    expected_next_row = 1
    for batch in batches:
        if batch.row_start != expected_next_row:
            relation = "overlaps" if batch.row_start < expected_next_row else "leaves a gap before"
            raise ValueError(
                f"Source ranges are not contiguous: {batch.key} {relation} "
                f"row {expected_next_row}. Review the object set before loading."
            )
        expected_next_row = batch.row_end + 1
    return batches


def mysql_connection(config: dict[str, str | int]):
    import mysql.connector

    return mysql.connector.connect(
        host=config["mysql_host"],
        port=config["mysql_port"],
        database=config["mysql_database"],
        user=config["mysql_user"],
        password=config["mysql_password"],
        connection_timeout=20,
        autocommit=False,
    )


def succeeded_objects(connection) -> set[str]:
    cursor = connection.cursor()
    try:
        cursor.execute("SELECT source_object_key FROM etl_batch WHERE status = 'SUCCEEDED'")
        return {row[0] for row in cursor.fetchall()}
    finally:
        cursor.close()


def start_batch(connection, batch: SourceBatch, run_id: str) -> None:
    cursor = connection.cursor()
    try:
        cursor.execute("DELETE FROM stg_wildfire WHERE source_object_key = %s", (batch.key,))
        cursor.execute("DELETE FROM stg_wildfire_reject WHERE source_object_key = %s", (batch.key,))
        cursor.execute(
            """INSERT INTO etl_batch (
                   source_object_key, source_sha256, row_start, row_end, status,
                   run_id, records_read, records_accepted, records_rejected,
                   facts_inserted, started_at, finished_at, error_message
               ) VALUES (%s, %s, %s, %s, 'RUNNING', %s, 0, 0, 0, 0, UTC_TIMESTAMP(), NULL, NULL)
               ON DUPLICATE KEY UPDATE
                   source_sha256 = VALUES(source_sha256),
                   row_start = VALUES(row_start),
                   row_end = VALUES(row_end),
                   status = 'RUNNING',
                   run_id = VALUES(run_id),
                   records_read = 0,
                   records_accepted = 0,
                   records_rejected = 0,
                   facts_inserted = 0,
                   started_at = UTC_TIMESTAMP(),
                   finished_at = NULL,
                   error_message = NULL""",
            (batch.key, batch.source_sha256, batch.row_start, batch.row_end, run_id),
        )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        cursor.close()


def mark_batch_failed(connection, batch: SourceBatch, run_id: str, error: Exception) -> None:
    cursor = connection.cursor()
    try:
        cursor.execute(
            """UPDATE etl_batch
               SET status = 'FAILED', finished_at = UTC_TIMESTAMP(), error_message = %s
               WHERE source_object_key = %s AND run_id = %s""",
            (str(error)[:60000], batch.key, run_id),
        )
        connection.commit()
    except Exception:
        connection.rollback()
        LOGGER.exception("Could not record failed batch status for %s", batch.key)
    finally:
        cursor.close()


def build_spark(config: dict[str, str | int]):
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName("wildfire-data-warehouse-etl")
        .master(str(config["spark_master"]))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.hadoop.fs.s3a.endpoint", str(config["s3_endpoint"]))
        .config("spark.hadoop.fs.s3a.endpoint.region", str(config["s3_region"]))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.access.key", str(config["s3_access_key"]))
        .config("spark.hadoop.fs.s3a.secret.key", str(config["s3_secret_key"]))
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
    )
    if config["spark_jars_packages"]:
        builder = builder.config("spark.jars.packages", str(config["spark_jars_packages"]))
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def normalize_and_validate(spark, uri: str, batch: SourceBatch, run_id: str):
    from pyspark.sql import Window, functions as F

    source = (
        spark.read.option("header", "true")
        .option("encoding", "UTF-8")
        .option("mode", "PERMISSIVE")
        .csv(uri)
    )
    aliases: dict[str, str] = {}
    for column in source.columns:
        normalized = column.strip().lower()
        if normalized in aliases:
            raise ValueError(f"Duplicate source columns after normalization: {column}")
        aliases[normalized] = column
    missing = sorted(set(SOURCE_COLUMNS) - aliases.keys())
    if missing:
        raise ValueError(
            f"Source schema does not match the warehouse contract; missing {missing}. "
            f"Found columns: {source.columns}"
        )

    # Keep every source field in the deterministic row fingerprint, including extras.
    ordered_source_names = sorted(aliases)
    normalized = source.select(*[
        F.col(f"`{aliases[name]}`").cast("string").alias(name)
        for name in ordered_source_names
    ])
    payload = F.to_json(F.struct(*[F.col(name) for name in ordered_source_names]))
    frame = normalized.withColumn("_raw_payload", payload)
    frame = frame.withColumn("_row_hash", F.sha2(F.col("_raw_payload"), 256))

    # Exact duplicate rows receive a stable set of ordinals within this immutable object.
    # This preserves duplicate multiplicity without relying on Spark partition IDs.
    duplicate_window = Window.partitionBy("_row_hash").orderBy(F.col("_row_hash"))
    frame = frame.withColumn("_duplicate_ordinal", F.row_number().over(duplicate_window))
    frame = frame.withColumn(
        "source_record_id",
        F.sha2(
            F.concat_ws(
                "|", F.lit(batch.key), F.col("_row_hash"),
                F.col("_duplicate_ordinal").cast("string"),
            ),
            256,
        ),
    )
    frame = frame.withColumn("source_sha256", F.lit(batch.source_sha256))
    frame = frame.withColumn("source_object_key", F.lit(batch.key))
    frame = frame.withColumn("run_id", F.lit(run_id))

    time_candidates = [
        F.try_to_timestamp(F.col("datetime"), F.lit(pattern))
        for pattern in (
            "yyyy-MM-dd HH:mm:ss",
            "yyyy-MM-dd HH:mm:ss.SSS",
            "yyyy-MM-dd'T'HH:mm:ss",
            "yyyy-MM-dd'T'HH:mm:ss.SSS",
            "yyyy-MM-dd'T'HH:mm:ssXXX",
            "yyyy-MM-dd'T'HH:mm:ss.SSSXXX",
            "yyyy-MM-dd",
            "MM/dd/yyyy HH:mm:ss",
            "MM/dd/yyyy",
        )
    ]
    frame = frame.withColumn("observed_at", F.coalesce(*time_candidates))

    numeric_columns = ("latitude", "longitude", *FEATURE_COLUMNS)
    for column in numeric_columns:
        frame = frame.withColumn(f"_{column}_parsed", F.col(column).cast("double"))
    frame = frame.withColumn(
        "latitude_value", F.col("_latitude_parsed").cast("decimal(9,6)")
    ).withColumn(
        "longitude_value", F.col("_longitude_parsed").cast("decimal(9,6)")
    )

    label = F.lower(F.trim(F.col("wildfire")))
    frame = frame.withColumn(
        "wildfire_value",
        F.when(label.isin("1", "1.0", "true", "yes"), F.lit(1))
        .when(label.isin("0", "0.0", "false", "no"), F.lit(0))
        .otherwise(F.lit(None).cast("int")),
    )

    reasons = [
        F.when(F.col("observed_at").isNull(), F.lit("invalid_datetime")),
        F.when(F.col("_latitude_parsed").isNull(), F.lit("invalid_latitude")),
        F.when(F.col("_longitude_parsed").isNull(), F.lit("invalid_longitude")),
        F.when(F.col("latitude_value").isNull(), F.lit("latitude_precision_overflow")),
        F.when(F.col("longitude_value").isNull(), F.lit("longitude_precision_overflow")),
        F.when(
            ~F.col("_latitude_parsed").between(-90, 90), F.lit("latitude_out_of_range")
        ),
        F.when(
            ~F.col("_longitude_parsed").between(-180, 180), F.lit("longitude_out_of_range")
        ),
        F.when(F.col("wildfire_value").isNull(), F.lit("invalid_wildfire_label")),
    ]
    for column in numeric_columns[2:]:
        reasons.append(F.when(
            F.col(column).isNotNull()
            & (F.trim(F.col(column)) != "")
            & F.col(f"_{column}_parsed").isNull(),
            F.lit(f"invalid_{column}"),
        ))
        parsed = F.col(f"_{column}_parsed")
        reasons.append(F.when(
            parsed.isNotNull() & (F.isnan(parsed) | (F.abs(parsed) == float("inf"))),
            F.lit(f"non_finite_{column}"),
        ))
    frame = frame.withColumn("rejection_reason", F.concat_ws("; ", *reasons))
    frame = frame.withColumn("is_valid", F.length("rejection_reason") == 0)

    accepted = frame.where(F.col("is_valid")).select(
        "run_id", "source_object_key", "source_record_id", "source_sha256",
        "observed_at", "latitude_value", "longitude_value", "wildfire_value",
        *[F.col(f"_{column}_parsed").alias(column) for column in FEATURE_COLUMNS],
    ).select(*[
        F.col(name).alias(alias)
        for name, alias in zip(
            ("run_id", "source_object_key", "source_record_id", "source_sha256",
             "observed_at", "latitude_value", "longitude_value", "wildfire_value",
             *FEATURE_COLUMNS),
            STAGE_COLUMNS,
        )
    ])
    rejected = frame.where(~F.col("is_valid")).select(
        "run_id", "source_record_id", "source_object_key", "source_sha256",
        "rejection_reason", F.col("_raw_payload").alias("raw_payload"),
    ).select(*REJECT_COLUMNS)
    metrics = frame.agg(
        F.count(F.lit(1)).alias("records_read"),
        F.sum(F.when(F.col("is_valid"), 1).otherwise(0)).alias("records_accepted"),
        F.sum(F.when(~F.col("is_valid"), 1).otherwise(0)).alias("records_rejected"),
    ).first()
    counts = {key: int(metrics[key] or 0) for key in (
        "records_read", "records_accepted", "records_rejected"
    )}
    if counts["records_read"] != batch.expected_rows:
        raise ValueError(
            f"Object row count mismatch for {batch.key}: expected {batch.expected_rows}, "
            f"read {counts['records_read']}"
        )
    return accepted, rejected, counts


def jdbc_properties(config: dict[str, str | int]) -> dict[str, str]:
    return {
        "user": str(config["mysql_user"]),
        "password": str(config["mysql_password"]),
        "driver": "com.mysql.cj.jdbc.Driver",
        "batchsize": str(config["jdbc_batch_size"]),
        "numPartitions": str(config["jdbc_partitions"]),
        "rewriteBatchedStatements": "true",
    }


def jdbc_url(config: dict[str, str | int]) -> str:
    use_ssl = "true" if config["mysql_use_ssl"] == "true" else "false"
    return (
        f"jdbc:mysql://{config['mysql_host']}:{config['mysql_port']}/"
        f"{config['mysql_database']}?useSSL={use_ssl}&serverTimezone=UTC"
    )


def write_jdbc(dataframe, table: str, config: dict[str, str | int]) -> None:
    partitions = int(config["jdbc_partitions"])
    if partitions < 1:
        raise ValueError("ETL_JDBC_PARTITIONS must be at least 1")
    writer_frame = dataframe.coalesce(partitions) if dataframe.rdd.getNumPartitions() > partitions else dataframe
    (writer_frame.write.format("jdbc")
     .option("url", jdbc_url(config))
     .option("dbtable", table)
     .options(**jdbc_properties(config))
     .mode("append")
     .save())


def finish_batch(connection, batch: SourceBatch, run_id: str, counts: dict[str, int]) -> int:
    cursor = connection.cursor()
    try:
        # Replace this object's reject set only inside the success transaction. A
        # failed retry therefore leaves the previous diagnostic rows intact.
        cursor.execute("DELETE FROM etl_reject WHERE source_object_key = %s", (batch.key,))
        cursor.execute(
            """INSERT INTO etl_reject (
                   source_record_id, source_object_key, source_sha256,
                   rejection_reason, raw_payload
               )
               SELECT DISTINCT r.source_record_id, r.source_object_key,
                      r.source_sha256, r.rejection_reason, r.raw_payload
               FROM stg_wildfire_reject r
               WHERE r.run_id = %s""",
            (run_id,),
        )
        cursor.execute(
            """INSERT INTO dim_date (date_key, full_date, year_num, month_num, day_num)
               SELECT DISTINCT DATE_FORMAT(observed_at, '%Y%m%d') + 0,
                      DATE(observed_at), YEAR(observed_at), MONTH(observed_at), DAY(observed_at)
               FROM stg_wildfire s
               WHERE s.run_id = %s
                 AND NOT EXISTS (
                     SELECT 1 FROM dim_date d
                     WHERE d.full_date = DATE(s.observed_at)
                 )""",
            (run_id,),
        )
        cursor.execute(
            """INSERT INTO dim_location (latitude, longitude)
               SELECT DISTINCT s.latitude, s.longitude
               FROM stg_wildfire s
               WHERE s.run_id = %s
                 AND NOT EXISTS (
                     SELECT 1 FROM dim_location l
                     WHERE l.latitude = s.latitude AND l.longitude = s.longitude
                 )""",
            (run_id,),
        )
        cursor.execute(
            """INSERT INTO fact_wildfire (
                   source_record_id, source_sha256, source_object_key,
                   date_key, location_key, observed_at, wildfire,
                   pr, rmax, rmin, sph, srad, tmmn, tmmx, vs,
                   bi, fm100, fm1000, erc, etr, pet, vpd
               )
               SELECT DISTINCT s.source_record_id, s.source_sha256, s.source_object_key,
                      DATE_FORMAT(s.observed_at, '%Y%m%d') + 0,
                      l.location_key, s.observed_at, s.wildfire,
                      s.pr, s.rmax, s.rmin, s.sph, s.srad, s.tmmn, s.tmmx, s.vs,
                      s.bi, s.fm100, s.fm1000, s.erc, s.etr, s.pet, s.vpd
               FROM stg_wildfire s
               JOIN dim_location l
                 ON l.latitude = s.latitude AND l.longitude = s.longitude
               WHERE s.run_id = %s
                 AND NOT EXISTS (
                     SELECT 1 FROM fact_wildfire f
                     WHERE f.source_record_id = s.source_record_id
                 )""",
            (run_id,),
        )
        inserted = max(cursor.rowcount, 0)
        cursor.execute(
            """UPDATE etl_batch
               SET status = 'SUCCEEDED', records_read = %s, records_accepted = %s,
                   records_rejected = %s, facts_inserted = %s,
                   finished_at = UTC_TIMESTAMP(), error_message = NULL
               WHERE source_object_key = %s AND run_id = %s""",
            (counts["records_read"], counts["records_accepted"],
             counts["records_rejected"], inserted, batch.key, run_id),
        )
        cursor.execute("DELETE FROM stg_wildfire WHERE run_id = %s", (run_id,))
        cursor.execute("DELETE FROM stg_wildfire_reject WHERE run_id = %s", (run_id,))
        connection.commit()
        return inserted
    except Exception:
        connection.rollback()
        raise
    finally:
        cursor.close()


def process_batch(spark, connection, config: dict[str, str | int], batch: SourceBatch) -> None:
    run_id = str(uuid.uuid4())
    start_batch(connection, batch, run_id)
    try:
        uri = f"s3a://{config['data_bucket']}/{batch.key}"
        accepted, rejected, counts = normalize_and_validate(spark, uri, batch, run_id)
        if counts["records_accepted"]:
            write_jdbc(accepted, "stg_wildfire", config)
        if counts["records_rejected"]:
            write_jdbc(rejected, "stg_wildfire_reject", config)
        inserted = finish_batch(connection, batch, run_id, counts)
        LOGGER.info(
            "Completed %s: read=%d accepted=%d rejected=%d inserted=%d already_present=%d",
            batch.key, counts["records_read"], counts["records_accepted"],
            counts["records_rejected"], inserted,
            counts["records_accepted"] - inserted,
        )
    except Exception as error:
        connection.rollback()
        mark_batch_failed(connection, batch, run_id, error)
        raise


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--max-batches", type=int, default=0,
                        help="process at most N pending objects (0 means all)")
    parser.add_argument("--reprocess", action="store_true",
                        help="re-run succeeded objects; fact keys keep this idempotent")
    parser.add_argument("--dry-run", action="store_true",
                        help="list source objects and ranges without opening MySQL or Spark")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    config = get_config(require_mysql=not args.dry_run)
    s3 = make_s3_client(config)
    batches = discover_batches(s3, str(config["data_bucket"]))
    if not batches:
        raise RuntimeError(
            f"No source batches found under s3://{config['data_bucket']}/{SOURCE_PREFIX}"
        )
    if args.dry_run:
        for batch in batches:
            LOGGER.info("source=%s rows=%d-%d count=%d", batch.key,
                        batch.row_start, batch.row_end, batch.expected_rows)
        LOGGER.info("Discovered %d source batches", len(batches))
        return 0

    connection = mysql_connection(config)
    spark = None
    try:
        done = succeeded_objects(connection) if not args.reprocess else set()
        pending = [batch for batch in batches if batch.key not in done]
        if args.max_batches > 0:
            pending = pending[:args.max_batches]
        LOGGER.info("Found %d batches; %d pending", len(batches), len(pending))
        if not pending:
            LOGGER.info("Nothing to load")
            return 0
        spark = build_spark(config)
        for batch in pending:
            process_batch(spark, connection, config, batch)
        return 0
    finally:
        if spark is not None:
            spark.stop()
        connection.close()


if __name__ == "__main__":
    raise SystemExit(main())
