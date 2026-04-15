from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone

import boto3
import requests
from botocore.exceptions import ClientError
from lib.workua.parser import (
    WORKUA_QUERIES,
    build_workua_search_url,
    extract_workua_job_id,
    parse_workua_detail_html,
    parse_workua_search_cards,
)

logger = logging.getLogger(__name__)


def env(name: str, default: str | None = None) -> str:
    value = os.environ.get(name, default)
    if value is None:
        raise ValueError(f"Required environment variable is not set: {name}")
    return value


def get_s3_client():
    endpoint_url = env("MINIO_ENDPOINT")
    access_key = env("MINIO_ACCESS_KEY")
    secret_key = env("MINIO_SECRET_KEY")

    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
    )


def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36"
            ),
            "Accept-Language": "uk-UA,uk;q=0.9,en-US;q=0.8,en;q=0.7",
        }
    )
    return session


def upload_json(bucket: str, key: str, payload: list[dict] | dict) -> None:
    s3 = get_s3_client()
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json",
    )


def upload_html(bucket: str, key: str, html_text: str) -> None:
    s3 = get_s3_client()
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=html_text.encode("utf-8"),
        ContentType="text/html; charset=utf-8",
    )


def fetch_workua_jobs(**context) -> None:
    ds = context["ds"]
    bucket = env("BRONZE_BUCKET")
    fetched_at = datetime.now(timezone.utc).isoformat()

    session = build_session()

    global_seen_urls: set[str] = set()

    for query_cfg in WORKUA_QUERIES:
        query_name = query_cfg["query_name"]
        query_text = query_cfg["query_text"]
        role_family = query_cfg["role_family"]

        page = 1

        while True:
            url = build_workua_search_url(query_text, page)

            response = session.get(url, timeout=60)
            response.raise_for_status()

            raw_key = f"jobs/source=workua/query={query_name}/dt={ds}/raw/search_page_{page}.html"
            upload_html(bucket=bucket, key=raw_key, html_text=response.text)

            cards = parse_workua_search_cards(
                response.text,
                fetched_at=fetched_at,
                dt=ds,
                page=page,
                query_name=query_name,
                query_text=query_text,
                role_family=role_family,
            )

            new_cards = [card for card in cards if card["job_url"] not in global_seen_urls]

            if not new_cards:
                logger.info("Stop: no new cards found for query=%s, page=%d", query_name, page)
                break

            for card in new_cards:
                global_seen_urls.add(card["job_url"])

            parsed_key = f"jobs/source=workua/query={query_name}/dt={ds}/parsed/search_results_page_{page}.json"
            upload_json(bucket=bucket, key=parsed_key, payload=new_cards)

            logger.info("Uploaded %d cards to s3://%s/%s", len(new_cards), bucket, parsed_key)

            page += 1
            time.sleep(2)

            if page > 20:
                break


def fetch_workua_detail_pages(**context) -> None:
    ds = context["ds"]
    bucket = env("BRONZE_BUCKET")
    s3 = get_s3_client()

    all_cards: list[dict] = []

    for query_cfg in WORKUA_QUERIES:
        query_name = query_cfg["query_name"]

        page = 1
        while True:
            key = f"jobs/source=workua/query={query_name}/dt={ds}/parsed/search_results_page_{page}.json"
            try:
                obj = s3.get_object(Bucket=bucket, Key=key)
            except ClientError:
                break

            cards = json.loads(obj["Body"].read().decode("utf-8"))
            if not cards:
                break

            all_cards.extend(cards)
            page += 1

    deduped_cards: list[dict] = []
    seen_urls: set[str] = set()

    for card in all_cards:
        job_url = card["job_url"]
        if job_url in seen_urls:
            continue
        seen_urls.add(job_url)
        deduped_cards.append(card)

    session = build_session()

    for idx, card in enumerate(deduped_cards, start=1):
        job_url = card["job_url"]

        response = session.get(job_url, timeout=60)
        response.raise_for_status()

        job_id = extract_workua_job_id(job_url)
        detail_key = f"jobs/source=workua/details_raw/dt={ds}/job_{job_id}.html"
        upload_html(bucket=bucket, key=detail_key, html_text=response.text)

        logger.info("Uploaded detail page %d/%d to s3://%s/%s", idx, len(deduped_cards), bucket, detail_key)
        time.sleep(2)


def parse_workua_detail_pages(**context) -> None:
    ds = context["ds"]
    bucket = env("BRONZE_BUCKET")
    s3 = get_s3_client()

    prefix = f"jobs/source=workua/details_raw/dt={ds}/"
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

    contents = resp.get("Contents", [])
    if not contents:
        logger.warning("No raw detail files found under %s", prefix)
        return

    for obj in contents:
        key = obj["Key"]
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")

        filename = key.split("/")[-1]
        job_id = filename.replace("job_", "").replace(".html", "")
        job_url = f"https://www.work.ua/jobs/{job_id}/" if job_id.isdigit() else ""

        parsed = parse_workua_detail_html(
            html=body,
            job_id=job_id,
            job_url=job_url,
            ds=ds,
        )

        out_key = f"jobs/source=workua/details_parsed/dt={ds}/job_{job_id}.json"
        upload_json(bucket=bucket, key=out_key, payload=parsed)

        logger.info("Uploaded parsed detail to s3://%s/%s", bucket, out_key)