from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone

import boto3
import requests
from lib.dou.parser import (
    extract_dou_job_id,
    parse_dou_detail_html,
    parse_dou_search_cards,
)

logger = logging.getLogger(__name__)

DOU_LIST_URL = "https://jobs.dou.ua/vacancies/?category=Data+Engineer"
DOU_XHR_URL = "https://jobs.dou.ua/vacancies/xhr-load/?category=Data%20Engineer"


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


def fetch_dou_jobs(**context) -> None:
    ds = context["ds"]
    bucket = env("BRONZE_BUCKET")
    fetched_at = datetime.now(timezone.utc).isoformat()

    session = build_session()

    list_resp = session.get(DOU_LIST_URL, timeout=60)
    list_resp.raise_for_status()

    raw_page_1_key = f"jobs/source=dou/dt={ds}/raw/search_page_1.html"
    upload_html(bucket=bucket, key=raw_page_1_key, html_text=list_resp.text)

    page1_cards = parse_dou_search_cards(
        list_resp.text,
        fetched_at=fetched_at,
        dt=ds,
        page=1,
    )

    csrf_token = session.cookies.get("csrftoken", "")

    xhr_headers = {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin": "https://jobs.dou.ua",
        "Referer": DOU_LIST_URL,
        "X-Requested-With": "XMLHttpRequest",
    }

    all_cards: list[dict] = []
    seen_urls: set[str] = set()

    for card in page1_cards:
        job_url = card["job_url"]
        if job_url in seen_urls:
            continue
        seen_urls.add(job_url)
        all_cards.append(card)

    count = 40
    virtual_page = 2

    while True:
        xhr_resp = session.post(
            DOU_XHR_URL,
            headers=xhr_headers,
            data={
                "csrfmiddlewaretoken": csrf_token,
                "count": str(count),
            },
            timeout=60,
        )
        xhr_resp.raise_for_status()

        payload = xhr_resp.json()
        html_block = payload.get("html", "")
        last = payload.get("last", False)

        if not html_block or not html_block.strip():
            break

        raw_key = f"jobs/source=dou/dt={ds}/raw/search_page_{virtual_page}.html"
        upload_html(bucket=bucket, key=raw_key, html_text=html_block)

        xhr_cards = parse_dou_search_cards(
            html_block,
            fetched_at=fetched_at,
            dt=ds,
            page=virtual_page,
        )

        if not xhr_cards:
            break

        new_cards: list[dict] = []
        for card in xhr_cards:
            job_url = card["job_url"]
            if job_url in seen_urls:
                continue
            seen_urls.add(job_url)
            new_cards.append(card)

        if not new_cards:
            break

        all_cards.extend(new_cards)

        logger.info(
            "XHR count=%d: parsed=%d, new=%d, total=%d, last=%s",
            count, len(xhr_cards), len(new_cards), len(all_cards), last,
        )

        if last:
            break

        count += 20
        virtual_page += 1
        time.sleep(2)

    parsed_key = f"jobs/source=dou/dt={ds}/parsed/search_results.json"
    upload_json(bucket=bucket, key=parsed_key, payload=all_cards)

    logger.info("Uploaded %d jobs to s3://%s/%s", len(all_cards), bucket, parsed_key)


def fetch_dou_detail_pages(**context) -> None:
    ds = context["ds"]
    bucket = env("BRONZE_BUCKET")
    s3 = get_s3_client()

    parsed_key = f"jobs/source=dou/dt={ds}/parsed/search_results.json"
    obj = s3.get_object(Bucket=bucket, Key=parsed_key)
    cards = json.loads(obj["Body"].read().decode("utf-8"))

    session = build_session()

    for idx, card in enumerate(cards, start=1):
        job_url = card["job_url"]

        resp = session.get(job_url, timeout=60)
        resp.raise_for_status()

        job_id = extract_dou_job_id(job_url)
        detail_key = f"jobs/source=dou/details_raw/dt={ds}/job_{job_id}.html"
        upload_html(bucket=bucket, key=detail_key, html_text=resp.text)

        logger.info("Uploaded detail page %d/%d to s3://%s/%s", idx, len(cards), bucket, detail_key)
        time.sleep(2)


def parse_dou_detail_pages(**context) -> None:
    ds = context["ds"]
    bucket = env("BRONZE_BUCKET")
    s3 = get_s3_client()

    search_results_key = f"jobs/source=dou/dt={ds}/parsed/search_results.json"
    search_obj = s3.get_object(Bucket=bucket, Key=search_results_key)
    search_cards = json.loads(search_obj["Body"].read().decode("utf-8"))

    job_id_to_url: dict[str, str] = {}
    for card in search_cards:
        job_url = card.get("job_url", "")
        if not job_url:
            continue
        job_id = extract_dou_job_id(job_url)
        job_id_to_url[job_id] = job_url

    prefix = f"jobs/source=dou/details_raw/dt={ds}/"
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
        job_url = job_id_to_url.get(job_id, "")

        parsed = parse_dou_detail_html(
            html=body,
            job_id=job_id,
            job_url=job_url,
            ds=ds,
        )

        out_key = f"jobs/source=dou/details_parsed/dt={ds}/job_{job_id}.json"
        upload_json(bucket=bucket, key=out_key, payload=parsed)

        logger.info("Uploaded parsed detail to s3://%s/%s", bucket, out_key)