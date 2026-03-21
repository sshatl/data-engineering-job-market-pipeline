from __future__ import annotations

import os
import re

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from jobs.common.job_enrichment import (
    detect_remote_type,
    detect_seniority,
    detect_skills,
)


def env(name: str, default: str | None = None) -> str:
    value = os.environ.get(name, default)
    if value is None:
        raise ValueError(f"Required environment variable is not set: {name}")
    return value


def extract_workua_job_id(job_url: str | None) -> str | None:
    if not job_url:
        return None
    match = re.search(r"/jobs/(\d+)/?", job_url)
    return match.group(1) if match else None


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

    search_path = f"s3a://{bronze_bucket}/jobs/source=workua/query=*/dt={ds}/parsed/*.json"
    detail_path = f"s3a://{bronze_bucket}/jobs/source=workua/details_parsed/dt={ds}/*.json"
    silver_path = f"s3a://{silver_bucket}/workua_jobs_clean/dt={ds}/"

    spark = (
        SparkSession.builder
        .appName("workua_silver_transform")
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )

    search_df = spark.read.option("multiLine", "true").json(search_path)
    detail_df = spark.read.option("multiLine", "true").json(detail_path)

    extract_job_id_udf = F.udf(extract_workua_job_id, T.StringType())
    detect_skills_udf = F.udf(detect_skills, T.ArrayType(T.StringType()))
    detect_remote_type_udf = F.udf(detect_remote_type, T.StringType())
    detect_seniority_udf = F.udf(
        lambda title, experience_text, text: detect_seniority(title, experience_text, text),
        T.StringType(),
    )

    normalized = (
        search_df
        .withColumn("job_id", extract_job_id_udf(F.col("job_url")))
        .select(
            F.col("job_id").cast("string"),
            F.col("job_url").cast("string"),
            F.col("title").cast("string"),
            F.col("company").cast("string"),
            F.col("location").cast("string"),
            F.col("snippet").cast("string"),
            F.col("published_text").cast("string"),
            F.col("source").cast("string"),
            F.col("query_name").cast("string"),
            F.col("query_text").cast("string"),
            F.col("role_family").cast("string"),
            F.col("dt").cast("string"),
            F.col("fetched_at").cast("string"),
        )
        .filter(F.col("job_id").isNotNull())
        .dropDuplicates(["job_id"])
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
            F.col("employment_type").cast("string"),
            F.col("experience_text").cast("string"),
            F.col("education_text").cast("string"),
            F.col("description_full").cast("string"),
            F.col("page_text").cast("string"),
        )
        .dropDuplicates(["job_id"])
    )

    enriched = (
        normalized.alias("s")
        .join(detail_normalized.alias("d"), on="job_id", how="left")
        .withColumn("job_url_final", F.coalesce(F.col("s.job_url"), F.col("d.job_url")))
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
                F.coalesce(F.col("employment_type"), F.lit("")),
                F.coalesce(F.col("experience_text"), F.lit("")),
                F.coalesce(F.col("education_text"), F.lit("")),
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
        .withColumn(
            "seniority",
            detect_seniority_udf(F.col("title_final"), F.col("experience_text"), F.col("text_blob")),
        )
        .withColumn(
            "source_job_uid",
            F.concat_ws(
                "::",
                F.coalesce(F.col("source"), F.lit("workua")),
                F.coalesce(F.col("job_id"), F.col("job_url_final")),
            )
        )
        .select(
            F.col("source_job_uid").cast("string"),
            F.col("job_id").cast("string"),
            F.col("job_url_final").alias("job_url").cast("string"),
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

    print(f"Silver parquet written to: {silver_path}")
    print(f"PostgreSQL table written to: {jdbc_table}")

    spark.stop()


if __name__ == "__main__":
    main()