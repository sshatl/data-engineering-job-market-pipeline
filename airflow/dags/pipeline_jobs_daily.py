from __future__ import annotations

import os
from datetime import datetime, timedelta

from lib.common.notifications import notify_telegram_on_failure
from lib.common.spark_submit import build_spark_submit_cmd
from lib.dou.tasks import (
    fetch_dou_detail_pages,
    fetch_dou_jobs,
    parse_dou_detail_pages,
)
from lib.ithub.tasks import fetch_ithub_jobs
from lib.workua.tasks import (
    fetch_workua_detail_pages,
    fetch_workua_jobs,
    parse_workua_detail_pages,
)

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

DEFAULT_DS = "{{ ds }}"

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": notify_telegram_on_failure,
}


def env(name: str, default: str | None = None) -> str:
    value = os.environ.get(name, default)
    if value is None:
        raise ValueError(f"Required environment variable is not set: {name}")
    return value


POSTGRES_USER = env("POSTGRES_USER")
POSTGRES_PASSWORD = env("POSTGRES_PASSWORD")
POSTGRES_DB = env("POSTGRES_DB")
POSTGRES_HOST = env("POSTGRES_HOST", "postgres")
POSTGRES_PORT = env("POSTGRES_PORT", "5432")
POSTGRES_SCHEMA = env("POSTGRES_SCHEMA", "public")

MINIO_ENDPOINT = env("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = env("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = env("MINIO_SECRET_KEY")
BRONZE_BUCKET = env("BRONZE_BUCKET", "jobs-bronze")
SILVER_BUCKET = env("SILVER_BUCKET", "jobs-silver")


def build_postgres_promote_cmd(final_table: str, staging_table: str) -> str:
    columns = [
        "source_job_uid",
        "job_id",
        "job_url",
        "title",
        "company",
        "location",
        "snippet",
        "published_text",
        "description_full",
        "page_text",
        "source",
        "query_name",
        "query_text",
        "role_family",
        "remote_type",
        "seniority",
        "skills",
        "dt",
        "fetched_at",
    ]

    column_list = ",\n    ".join(columns)

    sql = f"""
BEGIN;
TRUNCATE TABLE {POSTGRES_SCHEMA}.{final_table};
INSERT INTO {POSTGRES_SCHEMA}.{final_table} (
    {column_list}
)
SELECT
    {column_list}
FROM {POSTGRES_SCHEMA}.{staging_table};
COMMIT;
""".strip()

    cmd = f"""
docker exec -i \
  -e PGPASSWORD="{POSTGRES_PASSWORD}" \
  jm_postgres \
  psql -h {POSTGRES_HOST} -p {POSTGRES_PORT} -U {POSTGRES_USER} -d {POSTGRES_DB} -v ON_ERROR_STOP=1 -c "{sql}"
""".strip()

    return cmd


def build_postgres_non_empty_check_cmd(table_name: str) -> str:
    sql = f"""
DO $check$
BEGIN
    IF (SELECT count(*) FROM {POSTGRES_SCHEMA}.{table_name}) = 0 THEN
        RAISE EXCEPTION 'Operational check failed: {POSTGRES_SCHEMA}.{table_name} is empty';
    END IF;
END
$check$;
""".strip()

    cmd = f"""
docker exec -i \
  -e PGPASSWORD="{POSTGRES_PASSWORD}" \
  jm_postgres \
  psql -h {POSTGRES_HOST} -p {POSTGRES_PORT} -U {POSTGRES_USER} -d {POSTGRES_DB} -v ON_ERROR_STOP=1 <<'SQL'
{sql}
SQL
""".strip()

    return cmd


def build_postgres_quality_check_cmd() -> str:
    sql = f"""
DO $check$
DECLARE
    total_rows bigint;
    unknown_remote_ratio numeric;
    unknown_seniority_ratio numeric;
    empty_skills_ratio numeric;
BEGIN
    SELECT count(*) INTO total_rows
    FROM {POSTGRES_SCHEMA}.int_job_posts_all_sources;

    IF total_rows = 0 THEN
        RAISE EXCEPTION 'Operational check failed: {POSTGRES_SCHEMA}.int_job_posts_all_sources is empty';
    END IF;

    SELECT
        count(*) FILTER (WHERE remote_type = 'unknown')::numeric / count(*)
    INTO unknown_remote_ratio
    FROM {POSTGRES_SCHEMA}.int_job_posts_all_sources;

    SELECT
        count(*) FILTER (WHERE seniority = 'unknown')::numeric / count(*)
    INTO unknown_seniority_ratio
    FROM {POSTGRES_SCHEMA}.int_job_posts_all_sources;

    SELECT
        count(*) FILTER (WHERE skills IS NULL OR btrim(skills) = '')::numeric / count(*)
    INTO empty_skills_ratio
    FROM {POSTGRES_SCHEMA}.int_job_posts_all_sources;

    IF unknown_remote_ratio > 0.80 THEN
        RAISE EXCEPTION 'Operational check failed: unknown remote_type ratio is too high: %', unknown_remote_ratio;
    END IF;

    IF unknown_seniority_ratio > 0.80 THEN
        RAISE EXCEPTION 'Operational check failed: unknown seniority ratio is too high: %', unknown_seniority_ratio;
    END IF;

    IF empty_skills_ratio > 0.80 THEN
        RAISE EXCEPTION 'Operational check failed: empty skills ratio is too high: %', empty_skills_ratio;
    END IF;
END
$check$;
""".strip()

    cmd = f"""
docker exec -i \
  -e PGPASSWORD="{POSTGRES_PASSWORD}" \
  jm_postgres \
  psql -h {POSTGRES_HOST} -p {POSTGRES_PORT} -U {POSTGRES_USER} -d {POSTGRES_DB} -v ON_ERROR_STOP=1 <<'SQL'
{sql}
SQL
""".strip()

    return cmd


def build_spark_env_vars(pg_table: str) -> dict[str, str]:
    return {
        "MINIO_ENDPOINT": MINIO_ENDPOINT,
        "MINIO_ACCESS_KEY": MINIO_ACCESS_KEY,
        "MINIO_SECRET_KEY": MINIO_SECRET_KEY,
        "BRONZE_BUCKET": BRONZE_BUCKET,
        "SILVER_BUCKET": SILVER_BUCKET,
        "DS": DEFAULT_DS,
        "PG_HOST": POSTGRES_HOST,
        "PG_PORT": POSTGRES_PORT,
        "PG_DB": POSTGRES_DB,
        "PG_USER": POSTGRES_USER,
        "PG_PASSWORD": POSTGRES_PASSWORD,
        "PG_SCHEMA": POSTGRES_SCHEMA,
        "PG_TABLE": pg_table,
    }

def build_pipeline_metrics_cmd() -> str:
    sql = f"""
INSERT INTO {POSTGRES_SCHEMA}.pipeline_run_metrics
(run_date, dag_id, source_name, layer_name, metric_name, metric_value)

-- source tables row count
SELECT CURRENT_DATE, 'pipeline_jobs_daily', 'dou', 'silver', 'row_count', count(*) FROM {POSTGRES_SCHEMA}.dou_jobs_clean
UNION ALL
SELECT CURRENT_DATE, 'pipeline_jobs_daily', 'workua', 'silver', 'row_count', count(*) FROM {POSTGRES_SCHEMA}.workua_jobs_clean
UNION ALL
SELECT CURRENT_DATE, 'pipeline_jobs_daily', 'ithub', 'silver', 'row_count', count(*) FROM {POSTGRES_SCHEMA}.ithub_jobs_clean

-- unified dataset
UNION ALL
SELECT CURRENT_DATE, 'pipeline_jobs_daily', 'all_sources', 'gold', 'row_count', count(*) FROM {POSTGRES_SCHEMA}.int_job_posts_all_sources

-- quality metrics
UNION ALL
SELECT CURRENT_DATE, 'pipeline_jobs_daily', 'all_sources', 'quality', 'unknown_remote_ratio',
    count(*) FILTER (WHERE remote_type = 'unknown')::numeric / count(*)
FROM {POSTGRES_SCHEMA}.int_job_posts_all_sources

UNION ALL
SELECT CURRENT_DATE, 'pipeline_jobs_daily', 'all_sources', 'quality', 'unknown_seniority_ratio',
    count(*) FILTER (WHERE seniority = 'unknown')::numeric / count(*)
FROM {POSTGRES_SCHEMA}.int_job_posts_all_sources

UNION ALL
SELECT CURRENT_DATE, 'pipeline_jobs_daily', 'all_sources', 'quality', 'empty_skills_ratio',
    count(*) FILTER (WHERE skills IS NULL OR btrim(skills) = '')::numeric / count(*)
FROM {POSTGRES_SCHEMA}.int_job_posts_all_sources;
""".strip()

    cmd = f"""
docker exec -i \
  -e PGPASSWORD="{POSTGRES_PASSWORD}" \
  jm_postgres \
  psql -h {POSTGRES_HOST} -p {POSTGRES_PORT} -U {POSTGRES_USER} -d {POSTGRES_DB} -v ON_ERROR_STOP=1 <<'SQL'
{sql}
SQL
""".strip()

    return cmd

with DAG(
    dag_id="pipeline_jobs_daily",
    start_date=datetime(2026, 3, 14),
    schedule="0 9 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=2),
    tags=["pipeline", "daily", "jobs"],
) as dag:
    start = EmptyOperator(task_id="start")

    workua_fetch_jobs = PythonOperator(
        task_id="workua_fetch_jobs",
        python_callable=fetch_workua_jobs,
        execution_timeout=timedelta(minutes=20),
    )

    workua_fetch_details = PythonOperator(
        task_id="workua_fetch_details",
        python_callable=fetch_workua_detail_pages,
        execution_timeout=timedelta(minutes=30),
    )

    workua_parse_details = PythonOperator(
        task_id="workua_parse_details",
        python_callable=parse_workua_detail_pages,
        execution_timeout=timedelta(minutes=20),
    )

    dou_fetch_jobs = PythonOperator(
        task_id="dou_fetch_jobs",
        python_callable=fetch_dou_jobs,
        execution_timeout=timedelta(minutes=20),
    )

    dou_fetch_details = PythonOperator(
        task_id="dou_fetch_details",
        python_callable=fetch_dou_detail_pages,
        execution_timeout=timedelta(minutes=30),
    )

    dou_parse_details = PythonOperator(
        task_id="dou_parse_details",
        python_callable=parse_dou_detail_pages,
        execution_timeout=timedelta(minutes=20),
    )

    ithub_fetch_jobs = PythonOperator(
        task_id="ithub_fetch_jobs",
        python_callable=fetch_ithub_jobs,
        execution_timeout=timedelta(minutes=20),
    )

    bronze_done = EmptyOperator(task_id="bronze_done")

    workua_silver = BashOperator(
        task_id="workua_silver",
        bash_command=build_spark_submit_cmd(
            app_path="jobs/sources/workua_silver_transform.py",
            env_vars=build_spark_env_vars("workua_jobs_clean__staging"),
        ),
        execution_timeout=timedelta(minutes=30),
    )

    workua_promote_to_final = BashOperator(
        task_id="workua_promote_to_final",
        bash_command=build_postgres_promote_cmd(
            final_table="workua_jobs_clean",
            staging_table="workua_jobs_clean__staging",
        ),
        execution_timeout=timedelta(minutes=10),
    )

    dou_silver = BashOperator(
        task_id="dou_silver",
        bash_command=build_spark_submit_cmd(
            app_path="jobs/sources/dou_silver_transform.py",
            env_vars=build_spark_env_vars("dou_jobs_clean__staging"),
        ),
        execution_timeout=timedelta(minutes=30),
    )

    dou_promote_to_final = BashOperator(
        task_id="dou_promote_to_final",
        bash_command=build_postgres_promote_cmd(
            final_table="dou_jobs_clean",
            staging_table="dou_jobs_clean__staging",
        ),
        execution_timeout=timedelta(minutes=10),
    )

    ithub_silver = BashOperator(
        task_id="ithub_silver",
        bash_command=build_spark_submit_cmd(
            app_path="jobs/sources/ithub_silver_transform.py",
            env_vars=build_spark_env_vars("ithub_jobs_clean__staging"),
        ),
        execution_timeout=timedelta(minutes=30),
    )

    ithub_promote_to_final = BashOperator(
        task_id="ithub_promote_to_final",
        bash_command=build_postgres_promote_cmd(
            final_table="ithub_jobs_clean",
            staging_table="ithub_jobs_clean__staging",
        ),
        execution_timeout=timedelta(minutes=10),
    )

    silver_done = EmptyOperator(task_id="silver_done")

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="""
        docker exec -i jm_dbt dbt run --project-dir /usr/app --profiles-dir /usr/app
        """,
        execution_timeout=timedelta(minutes=20),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="""
        docker exec -i jm_dbt dbt test --project-dir /usr/app --profiles-dir /usr/app
        """,
        execution_timeout=timedelta(minutes=20),
    )

    check_workua_non_empty = BashOperator(
        task_id="check_workua_non_empty",
        bash_command=build_postgres_non_empty_check_cmd("workua_jobs_clean"),
        execution_timeout=timedelta(minutes=5),
    )

    check_dou_non_empty = BashOperator(
        task_id="check_dou_non_empty",
        bash_command=build_postgres_non_empty_check_cmd("dou_jobs_clean"),
        execution_timeout=timedelta(minutes=5),
    )

    check_ithub_non_empty = BashOperator(
        task_id="check_ithub_non_empty",
        bash_command=build_postgres_non_empty_check_cmd("ithub_jobs_clean"),
        execution_timeout=timedelta(minutes=5),
    )

    check_int_non_empty = BashOperator(
        task_id="check_int_non_empty",
        bash_command=build_postgres_non_empty_check_cmd("int_job_posts_all_sources"),
        execution_timeout=timedelta(minutes=5),
    )

    check_data_quality = BashOperator(
        task_id="check_data_quality",
        bash_command=build_postgres_quality_check_cmd(),
        execution_timeout=timedelta(minutes=5),
    )

    write_pipeline_metrics = BashOperator(
        task_id="write_pipeline_metrics",
        bash_command=build_pipeline_metrics_cmd(),
        execution_timeout=timedelta(minutes=5),
    )

    operational_checks_done = EmptyOperator(task_id="operational_checks_done")
    finish = EmptyOperator(task_id="finish")

    start >> workua_fetch_jobs >> workua_fetch_details >> workua_parse_details
    start >> dou_fetch_jobs >> dou_fetch_details >> dou_parse_details
    start >> ithub_fetch_jobs

    [workua_parse_details, dou_parse_details, ithub_fetch_jobs] >> bronze_done

    bronze_done >> workua_silver >> workua_promote_to_final
    bronze_done >> dou_silver >> dou_promote_to_final
    bronze_done >> ithub_silver >> ithub_promote_to_final

    [workua_promote_to_final, dou_promote_to_final, ithub_promote_to_final] >> silver_done
    silver_done >> dbt_run >> dbt_test

    dbt_test >> check_workua_non_empty
    dbt_test >> check_dou_non_empty
    dbt_test >> check_ithub_non_empty
    dbt_test >> check_int_non_empty
    dbt_test >> check_data_quality

    [
        check_workua_non_empty,
        check_dou_non_empty,
        check_ithub_non_empty,
        check_int_non_empty,
        check_data_quality,
    ] >> operational_checks_done >> write_pipeline_metrics >> finish