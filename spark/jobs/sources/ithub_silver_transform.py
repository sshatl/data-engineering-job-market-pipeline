from __future__ import annotations

import logging
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from jobs.common.job_enrichment import (
    detect_remote_type,
    detect_seniority,
    detect_skills,
)

logger = logging.getLogger(__name__)


def env(name: str, default: str | None = None) -> str:
    value = os.environ.get(name, default)
    if value is None:
        raise ValueError(f"Required environment variable is not set: {name}")
    return value


def main() -> None:
    bronze_bucket = env("BRONZE_BUCKET")
    silver_bucket = env("SILVER_BUCKET")
    ds = env("DS")

    minio_endpoint = env("MINIO_ENDPOINT")
    minio_access_key = env("MINIO_ACCESS_KEY")
    minio_secret_key = env("MINIO_SECRET_KEY")

    pg_host = env("PG_HOST")
    pg_port = env("PG_PORT")
    pg_db = env("PG_DB")
    pg_user = env("PG_USER")
    pg_password = env("PG_PASSWORD")
    pg_schema = env("PG_SCHEMA", "public")
    pg_table = env("PG_TABLE")

    search_path = f"s3a://{bronze_bucket}/jobs/source=ithub/dt={ds}/parsed/search_results.json"
    silver_path = f"s3a://{silver_bucket}/ithub_jobs_clean/dt={ds}/"

    jdbc_url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_db}"
    jdbc_table = f"{pg_schema}.{pg_table}"

    spark = (
        SparkSession.builder
        .appName("ithub_silver_transform")
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )

    try:
        df = spark.read.option("multiLine", "true").json(search_path)
    except Exception:
        logger.exception("Failed to read input data from S3")
        spark.stop()
        sys.exit(1)

    if len(df.head(1)) == 0:
        logger.warning("No search results found at %s — skipping transform", search_path)
        spark.stop()
        return

    detect_skills_udf = F.udf(detect_skills, T.ArrayType(T.StringType()))
    detect_remote_type_udf = F.udf(detect_remote_type, T.StringType())
    detect_seniority_udf = F.udf(
        lambda title, text: detect_seniority(title, None, text),
        T.StringType(),
    )

    normalized = (
        df.select(
            F.regexp_replace(
                F.regexp_extract(F.col("job_url"), r"https?://[^/]+/(.*?)/?$", 1),
                r"[^A-Za-z0-9_-]+",
                "_",
            ).alias("job_id"),
            F.col("job_url").cast("string").alias("job_url"),
            F.col("title").cast("string").alias("title"),
            F.col("company").cast("string").alias("company"),
            F.lit(None).cast("string").alias("location"),
            F.col("meta_text").cast("string").alias("snippet"),
            F.col("published_text").cast("string").alias("published_text"),
            F.col("description_full").cast("string").alias("description_full"),
            F.col("source").cast("string").alias("source"),
            F.col("query_name").cast("string").alias("query_name"),
            F.col("query_text").cast("string").alias("query_text"),
            F.col("role_family").cast("string").alias("role_family"),
            F.col("dt").cast("string").alias("dt"),
            F.col("fetched_at").cast("string").alias("fetched_at"),
            F.col("skills_raw").alias("skills_raw"),
        )
        .filter(F.col("job_url").isNotNull())
        .dropDuplicates(["job_url"])
    )

    title_lower = F.lower(F.coalesce(F.col("title"), F.lit("")))

    filtered = normalized.filter(
        title_lower.rlike(r"(data engineer|big data engineer|data engineering)")
    )

    cleaned = (
        filtered
        .withColumn("page_text", F.lit(None).cast("string"))
        .withColumn(
            "skills_raw_text",
            F.concat_ws(" ", F.coalesce(F.col("skills_raw"), F.array().cast("array<string>")))
        )
        .withColumn(
            "text_blob",
            F.concat_ws(
                " ",
                F.coalesce(F.col("title"), F.lit("")),
                F.coalesce(F.col("snippet"), F.lit("")),
                F.coalesce(F.col("description_full"), F.lit("")),
                F.coalesce(F.col("query_name"), F.lit("")),
                F.coalesce(F.col("query_text"), F.lit("")),
                F.coalesce(F.col("role_family"), F.lit("")),
                F.coalesce(F.col("skills_raw_text"), F.lit("")),
            )
        )
        .withColumn("skills_array", detect_skills_udf(F.col("text_blob")))
        .withColumn("skills", F.concat_ws(",", F.sort_array(F.col("skills_array"))))
        .withColumn("remote_type", detect_remote_type_udf(F.col("location"), F.col("text_blob")))
        .withColumn("seniority", detect_seniority_udf(F.col("title"), F.col("text_blob")))
        .withColumn(
            "source_job_uid",
            F.concat_ws(
                "::",
                F.coalesce(F.col("source"), F.lit("ithub")),
                F.coalesce(F.col("job_id"), F.col("job_url")),
            )
        )
        .select(
            F.col("source_job_uid").cast("string"),
            F.col("job_id").cast("string"),
            F.col("job_url").cast("string"),
            F.col("title").cast("string"),
            F.col("company").cast("string"),
            F.col("location").cast("string"),
            F.col("snippet").cast("string"),
            F.col("published_text").cast("string"),
            F.col("description_full").cast("string"),
            F.col("page_text").cast("string"),
            F.col("source").cast("string"),
            F.col("query_name").cast("string"),
            F.col("query_text").cast("string"),
            F.col("role_family").cast("string"),
            F.col("remote_type").cast("string"),
            F.col("seniority").cast("string"),
            F.col("skills").cast("string"),
            F.col("dt").cast("string"),
            F.col("fetched_at").cast("string"),
        )
    )

    (
        cleaned.coalesce(1)
        .write
        .mode("overwrite")
        .parquet(silver_path)
    )

    (
        cleaned.write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", jdbc_table)
        .option("user", pg_user)
        .option("password", pg_password)
        .option("driver", "org.postgresql.Driver")
        .option("truncate", "true")
        .mode("overwrite")
        .save()
    )

    logger.info("Silver parquet written to: %s", silver_path)
    logger.info("PostgreSQL table written to: %s", jdbc_table)

    spark.stop()


if __name__ == "__main__":
    main()