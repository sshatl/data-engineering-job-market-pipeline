from __future__ import annotations

import logging
import os

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

    jdbc_url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_db}"
    jdbc_table = f"{pg_schema}.{pg_table}"

    search_path = f"s3a://{bronze_bucket}/jobs/source=dou/dt={ds}/parsed/search_results.json"
    detail_path = f"s3a://{bronze_bucket}/jobs/source=dou/details_parsed/dt={ds}/*.json"
    silver_path = f"s3a://{silver_bucket}/dou_jobs_clean/dt={ds}/"

    spark = (
        SparkSession.builder
        .appName("dou_silver_transform")
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )

    search_df = spark.read.option("multiLine", "true").json(search_path)
    detail_df = spark.read.option("multiLine", "true").json(detail_path)

    detect_skills_udf = F.udf(detect_skills, T.ArrayType(T.StringType()))
    detect_remote_type_udf = F.udf(detect_remote_type, T.StringType())
    detect_seniority_udf = F.udf(
        lambda title, text: detect_seniority(title, None, text),
        T.StringType(),
    )

    normalized = (
        search_df
        .select(
            F.col("job_url").cast("string"),
            F.col("title").cast("string"),
            F.col("company").cast("string"),
            F.col("location").cast("string"),
            F.col("snippet").cast("string"),
            F.col("published_text").cast("string"),
            F.col("source").cast("string"),
            F.lit(None).cast("string").alias("query_name"),
            F.lit(None).cast("string").alias("query_text"),
            F.col("role_family").cast("string"),
            F.col("dt").cast("string"),
            F.col("fetched_at").cast("string"),
        )
        .filter(F.col("job_url").isNotNull())
        .dropDuplicates(["job_url"])
    )

    detail_normalized = (
        detail_df
        .select(
            F.col("job_id").cast("string"),
            F.col("job_url").cast("string"),
            F.col("title").cast("string").alias("detail_title"),
            F.col("company").cast("string").alias("detail_company"),
            F.col("location").cast("string").alias("detail_location"),
            F.col("published_text").cast("string").alias("detail_published_text"),
            F.col("description_full").cast("string"),
            F.col("page_text").cast("string"),
        )
        .filter(F.col("job_url").isNotNull())
        .dropDuplicates(["job_url"])
    )

    enriched = (
        normalized.alias("s")
        .join(detail_normalized.alias("d"), on="job_url", how="left")
        .withColumn("job_id_final", F.coalesce(F.col("d.job_id"), F.col("job_url")))
        .withColumn("title_final", F.coalesce(F.col("s.title"), F.col("d.detail_title")))
        .withColumn(
            "company_final",
            F.coalesce(
                F.when(F.length(F.trim(F.col("s.company"))) > 0, F.col("s.company")),
                F.when(F.length(F.trim(F.col("d.detail_company"))) > 0, F.col("d.detail_company")),
            )
        )
        .withColumn(
            "location_final",
            F.coalesce(
                F.when(F.length(F.trim(F.col("s.location"))) > 0, F.col("s.location")),
                F.when(F.length(F.trim(F.col("d.detail_location"))) > 0, F.col("d.detail_location")),
            )
        )
        .withColumn(
            "published_text_final",
            F.coalesce(
                F.when(F.length(F.trim(F.col("s.published_text"))) > 0, F.col("s.published_text")),
                F.when(F.length(F.trim(F.col("d.detail_published_text"))) > 0, F.col("d.detail_published_text")),
            )
        )
        .withColumn(
            "text_blob",
            F.concat_ws(
                " ",
                F.coalesce(F.col("title_final"), F.lit("")),
                F.coalesce(F.col("s.snippet"), F.lit("")),
                F.coalesce(F.col("description_full"), F.lit("")),
                F.coalesce(F.col("page_text"), F.lit("")),
                F.coalesce(F.col("location_final"), F.lit("")),
                F.coalesce(F.col("published_text_final"), F.lit("")),
                F.coalesce(F.col("query_name"), F.lit("")),
                F.coalesce(F.col("query_text"), F.lit("")),
                F.coalesce(F.col("role_family"), F.lit("")),
            )
        )
        .withColumn("skills_array", detect_skills_udf(F.col("text_blob")))
        .withColumn("skills", F.concat_ws(",", F.col("skills_array")))
        .withColumn("remote_type", detect_remote_type_udf(F.col("location_final"), F.col("text_blob")))
        .withColumn("seniority", detect_seniority_udf(F.col("title_final"), F.col("text_blob")))
        .withColumn(
            "source_job_uid",
            F.concat_ws(
                "::",
                F.coalesce(F.col("source"), F.lit("dou")),
                F.coalesce(F.col("d.job_id"), F.col("job_url")),
            )
        )
        .select(
            F.col("source_job_uid").cast("string"),
            F.col("job_id_final").alias("job_id").cast("string"),
            F.col("job_url").cast("string"),
            F.col("title_final").alias("title").cast("string"),
            F.col("company_final").alias("company").cast("string"),
            F.col("location_final").alias("location").cast("string"),
            F.col("s.snippet").alias("snippet").cast("string"),
            F.col("published_text_final").alias("published_text").cast("string"),
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
        enriched.coalesce(1)
        .write
        .mode("overwrite")
        .parquet(silver_path)
    )

    (
        enriched.write
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