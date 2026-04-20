from __future__ import annotations

import json
from io import BytesIO
from unittest.mock import MagicMock, patch

import pytest

ENV_VARS = {
    "MINIO_ENDPOINT": "http://minio:9000",
    "MINIO_ACCESS_KEY": "minioadmin",
    "MINIO_SECRET_KEY": "minioadmin",
    "BRONZE_BUCKET": "jobs-bronze",
}

SEARCH_PAGE_HTML = """
<ul>
    <li class="l-vacancy">
        <a class="vt" href="/vacancies/data-engineer-42/">Data Engineer</a>
        <div class="company">Acme Corp</div>
        <div class="cities">Kyiv</div>
        <div class="date">20 April 2026</div>
        <div class="sh-info">Build pipelines with Python and SQL</div>
    </li>
</ul>
"""

DETAIL_HTML = """
<html><body>
    <h1 class="g-h2">Data Engineer</h1>
    <div class="b-typo vacancy-section"><p>Full job description here.</p></div>
</body></html>
"""


def make_response(text: str = "", status: int = 200, json_data: dict | None = None):
    resp = MagicMock()
    resp.status_code = status
    resp.text = text
    resp.raise_for_status = MagicMock()
    if json_data is not None:
        resp.json.return_value = json_data
    return resp


@pytest.fixture()
def mock_s3():
    with patch("lib.dou.tasks.get_s3_client") as mock_factory:
        yield mock_factory.return_value


@patch.dict("os.environ", ENV_VARS)
def test_fetch_dou_jobs_uploads_search_results(mock_s3):
    """fetch_dou_jobs parses the first page and uploads search_results.json."""
    first_page_resp = make_response(text=SEARCH_PAGE_HTML)
    first_page_resp.cookies = MagicMock()
    first_page_resp.cookies.get = MagicMock(return_value="csrf123")

    xhr_resp = make_response(json_data={"html": "", "last": True})

    session_mock = MagicMock()
    session_mock.cookies.get.return_value = "csrf123"
    session_mock.get.return_value = first_page_resp
    session_mock.post.return_value = xhr_resp

    with patch("lib.dou.tasks.build_session", return_value=session_mock):
        from lib.dou.tasks import fetch_dou_jobs

        fetch_dou_jobs(ds="2026-04-20")

    put_calls = mock_s3.put_object.call_args_list
    keys = [c.kwargs["Key"] for c in put_calls]

    assert any("search_results.json" in k for k in keys), f"search_results.json not uploaded; keys={keys}"
    assert any("search_page_1.html" in k for k in keys), f"search_page_1.html not uploaded; keys={keys}"


@patch.dict("os.environ", ENV_VARS)
def test_fetch_dou_jobs_deduplicates_urls(mock_s3):
    """Duplicate job URLs across pages are deduplicated before upload."""
    duplicate_html = SEARCH_PAGE_HTML + SEARCH_PAGE_HTML

    first_page_resp = make_response(text=SEARCH_PAGE_HTML)
    first_page_resp.cookies = MagicMock()

    xhr_page2 = make_response(json_data={"html": duplicate_html, "last": True})

    session_mock = MagicMock()
    session_mock.cookies.get.return_value = ""
    session_mock.get.return_value = first_page_resp
    session_mock.post.return_value = xhr_page2

    with patch("lib.dou.tasks.build_session", return_value=session_mock):
        from lib.dou.tasks import fetch_dou_jobs

        fetch_dou_jobs(ds="2026-04-20")

    json_calls = [
        c for c in mock_s3.put_object.call_args_list if "search_results.json" in c.kwargs.get("Key", "")
    ]
    assert json_calls, "search_results.json was not uploaded"
    body = json.loads(json_calls[0].kwargs["Body"])
    urls = [item["job_url"] for item in body]
    assert len(urls) == len(set(urls)), "Duplicate URLs were not deduplicated"


@patch.dict("os.environ", ENV_VARS)
def test_fetch_dou_jobs_http_error_raises(mock_s3):
    """fetch_dou_jobs raises when the search page request fails."""
    import requests as req_lib

    session_mock = MagicMock()
    session_mock.get.side_effect = req_lib.exceptions.RequestException("timeout")

    with patch("lib.dou.tasks.build_session", return_value=session_mock):
        from lib.dou.tasks import fetch_dou_jobs

        with pytest.raises(req_lib.exceptions.RequestException):
            fetch_dou_jobs(ds="2026-04-20")


@patch.dict("os.environ", ENV_VARS)
def test_fetch_dou_detail_pages_uploads_html(mock_s3):
    """fetch_dou_detail_pages fetches each job URL and uploads raw HTML."""
    cards = [{"job_url": "https://jobs.dou.ua/vacancies/data-engineer-42/"}]
    mock_s3.get_object.return_value = {
        "Body": BytesIO(json.dumps(cards).encode())
    }

    detail_resp = make_response(text=DETAIL_HTML)
    session_mock = MagicMock()
    session_mock.get.return_value = detail_resp

    with patch("lib.dou.tasks.build_session", return_value=session_mock):
        from lib.dou.tasks import fetch_dou_detail_pages

        fetch_dou_detail_pages(ds="2026-04-20")

    put_calls = mock_s3.put_object.call_args_list
    keys = [c.kwargs["Key"] for c in put_calls]
    assert any("details_raw" in k and ".html" in k for k in keys), f"detail HTML not uploaded; keys={keys}"


@patch.dict("os.environ", ENV_VARS)
def test_fetch_dou_detail_pages_skips_on_http_error(mock_s3):
    """fetch_dou_detail_pages skips a card if its request fails (does not raise)."""
    import requests as req_lib

    cards = [
        {"job_url": "https://jobs.dou.ua/vacancies/data-engineer-42/"},
        {"job_url": "https://jobs.dou.ua/vacancies/data-engineer-99/"},
    ]
    mock_s3.get_object.return_value = {
        "Body": BytesIO(json.dumps(cards).encode())
    }

    ok_resp = make_response(text=DETAIL_HTML)
    session_mock = MagicMock()
    session_mock.get.side_effect = [
        req_lib.exceptions.RequestException("connection error"),
        ok_resp,
    ]

    with patch("lib.dou.tasks.build_session", return_value=session_mock):
        from lib.dou.tasks import fetch_dou_detail_pages

        fetch_dou_detail_pages(ds="2026-04-20")  # must not raise

    put_calls = mock_s3.put_object.call_args_list
    assert len(put_calls) == 1, "Only the successful card should be uploaded"


@patch.dict("os.environ", ENV_VARS)
def test_parse_dou_detail_pages_uploads_json(mock_s3):
    """parse_dou_detail_pages reads raw HTML from S3 and uploads parsed JSON."""
    ds = "2026-04-20"
    cards = [{"job_url": "https://jobs.dou.ua/vacancies/data-engineer-42/"}]

    def get_object_side_effect(Bucket, Key):  # noqa: N803
        if "search_results.json" in Key:
            return {"Body": BytesIO(json.dumps(cards).encode())}
        return {"Body": BytesIO(DETAIL_HTML.encode())}

    mock_s3.get_object.side_effect = get_object_side_effect
    mock_s3.list_objects_v2.return_value = {
        "Contents": [{"Key": f"jobs/source=dou/details_raw/dt={ds}/job_42.html"}]
    }

    from lib.dou.tasks import parse_dou_detail_pages

    parse_dou_detail_pages(ds=ds)

    put_calls = mock_s3.put_object.call_args_list
    keys = [c.kwargs["Key"] for c in put_calls]
    assert any("details_parsed" in k and ".json" in k for k in keys), f"parsed JSON not uploaded; keys={keys}"
