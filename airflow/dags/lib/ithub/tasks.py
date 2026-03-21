from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone

import boto3
import requests
from lib.ithub.parser import build_ithub_search_url, parse_ithub_search_page


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


def upload_json(bucket: str, key: str, payload: list[dict]) -> None:
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


def fetch_ithub_jobs(**context) -> None:
    ds = context["ds"]
    bucket = env("BRONZE_BUCKET")
    fetched_at = datetime.now(timezone.utc).isoformat()

    session = build_session()

    page = 1
    all_cards: list[dict] = []
    seen_urls: set[str] = set()

    while True:
        url = build_ithub_search_url(page)

        resp = session.get(url, timeout=60)
        resp.raise_for_status()

        raw_key = f"jobs/source=ithub/dt={ds}/raw/search_page_{page}.html"
        upload_html(bucket=bucket, key=raw_key, html_text=resp.text)

        cards = parse_ithub_search_page(
            html=resp.text,
            fetched_at=fetched_at,
            dt=ds,
            page=page,
        )

        if not cards:
            print(f"Stop: no cards found on page={page}")
            break

        new_cards: list[dict] = []
        for card in cards:
            job_url = card["job_url"]
            if job_url in seen_urls:
                continue
            seen_urls.add(job_url)
            new_cards.append(card)

        if not new_cards:
            print(f"Stop: no new cards found on page={page}")
            break

        all_cards.extend(new_cards)
        print(f"Page={page}, new={len(new_cards)}, total={len(all_cards)}")

        page += 1
        time.sleep(2)

        if page > 20:
            break

    parsed_key = f"jobs/source=ithub/dt={ds}/parsed/search_results.json"
    upload_json(bucket=bucket, key=parsed_key, payload=all_cards)

    print(f"Uploaded {len(all_cards)} jobs to s3://{bucket}/{parsed_key}")