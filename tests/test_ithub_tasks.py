from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

ENV_VARS = {
    "MINIO_ENDPOINT": "http://minio:9000",
    "MINIO_ACCESS_KEY": "minioadmin",
    "MINIO_SECRET_KEY": "minioadmin",
    "BRONZE_BUCKET": "jobs-bronze",
}

SEARCH_PAGE_HTML = """
<div class="view-content">
  <div class="cardbox">
    <h2><a href="/jobs/data-engineer-42"> Data Engineer </a></h2>
    <div class="base"><a>Acme Corp</a></div>
    <div class="jobscompanytype">Product company</div>
    <div class="eventinfo jobsinfo">Kyiv • Remote</div>
    <div class="jobdate">1 day ago</div>
    <div class="cardboxdopinfo">Build pipelines with Python and SQL</div>
    <ul class="eventmaintag"><li>Python</li><li>SQL</li></ul>
  </div>
</div>
"""

EMPTY_PAGE_HTML = "<html><body></body></html>"


def make_response(text: str = ""):
    resp = MagicMock()
    resp.text = text
    resp.raise_for_status = MagicMock()
    return resp


@pytest.fixture()
def mock_s3():
    with patch("lib.ithub.tasks.get_s3_client") as mock_factory:
        yield mock_factory.return_value


@patch.dict("os.environ", ENV_VARS)
def test_fetch_ithub_jobs_uploads_search_results(mock_s3):
    """fetch_ithub_jobs parses cards and uploads search_results.json."""
    session_mock = MagicMock()
    session_mock.get.side_effect = [
        make_response(text=SEARCH_PAGE_HTML),
        make_response(text=EMPTY_PAGE_HTML),  # page 2 — no cards, stop
    ]

    with patch("lib.ithub.tasks.build_session", return_value=session_mock):
        with patch("lib.ithub.tasks.time.sleep"):
            from lib.ithub.tasks import fetch_ithub_jobs

            fetch_ithub_jobs(ds="2026-04-20")

    keys = [c.kwargs["Key"] for c in mock_s3.put_object.call_args_list]
    assert any("search_results.json" in k for k in keys), f"search_results.json not uploaded; keys={keys}"
    assert any("search_page_1.html" in k for k in keys), f"raw HTML not uploaded; keys={keys}"


@patch.dict("os.environ", ENV_VARS)
def test_fetch_ithub_jobs_uploads_empty_list_on_first_empty_page(mock_s3):
    """fetch_ithub_jobs uploads an empty JSON list when first page has no cards."""
    session_mock = MagicMock()
    session_mock.get.return_value = make_response(text=EMPTY_PAGE_HTML)

    with patch("lib.ithub.tasks.build_session", return_value=session_mock):
        from lib.ithub.tasks import fetch_ithub_jobs

        fetch_ithub_jobs(ds="2026-04-20")

    json_calls = [
        c for c in mock_s3.put_object.call_args_list
        if "search_results.json" in c.kwargs.get("Key", "")
    ]
    assert json_calls, "search_results.json was not uploaded"
    body = json.loads(json_calls[0].kwargs["Body"])
    assert body == [], f"Expected empty list, got {body}"


@patch.dict("os.environ", ENV_VARS)
def test_fetch_ithub_jobs_deduplicates_urls(mock_s3):
    """Duplicate job URLs across pages are deduplicated before upload."""
    session_mock = MagicMock()
    session_mock.get.side_effect = [
        make_response(text=SEARCH_PAGE_HTML),   # page 1 — one card
        make_response(text=SEARCH_PAGE_HTML),   # page 2 — same card, deduped → stop
    ]

    with patch("lib.ithub.tasks.build_session", return_value=session_mock):
        with patch("lib.ithub.tasks.time.sleep"):
            from lib.ithub.tasks import fetch_ithub_jobs

            fetch_ithub_jobs(ds="2026-04-20")

    json_calls = [
        c for c in mock_s3.put_object.call_args_list
        if "search_results.json" in c.kwargs.get("Key", "")
    ]
    assert json_calls
    body = json.loads(json_calls[0].kwargs["Body"])
    urls = [item["job_url"] for item in body]
    assert len(urls) == len(set(urls)), "Duplicate URLs were not deduplicated"
    assert len(urls) == 1


@patch.dict("os.environ", ENV_VARS)
def test_fetch_ithub_jobs_http_error_stops_pagination(mock_s3):
    """HTTP error breaks pagination and still uploads whatever was collected."""
    import requests as req_lib

    session_mock = MagicMock()
    session_mock.get.side_effect = [
        make_response(text=SEARCH_PAGE_HTML),                        # page 1 — ok
        req_lib.exceptions.RequestException("connection refused"),   # page 2 — error
    ]

    with patch("lib.ithub.tasks.build_session", return_value=session_mock):
        with patch("lib.ithub.tasks.time.sleep"):
            from lib.ithub.tasks import fetch_ithub_jobs

            fetch_ithub_jobs(ds="2026-04-20")  # must not raise

    json_calls = [
        c for c in mock_s3.put_object.call_args_list
        if "search_results.json" in c.kwargs.get("Key", "")
    ]
    assert json_calls
    body = json.loads(json_calls[0].kwargs["Body"])
    assert len(body) == 1, "Card from page 1 should be in the upload"


@patch.dict("os.environ", ENV_VARS)
def test_fetch_ithub_jobs_http_error_on_first_page(mock_s3):
    """HTTP error on page 1 uploads an empty list without raising."""
    import requests as req_lib

    session_mock = MagicMock()
    session_mock.get.side_effect = req_lib.exceptions.RequestException("timeout")

    with patch("lib.ithub.tasks.build_session", return_value=session_mock):
        from lib.ithub.tasks import fetch_ithub_jobs

        fetch_ithub_jobs(ds="2026-04-20")  # must not raise

    json_calls = [
        c for c in mock_s3.put_object.call_args_list
        if "search_results.json" in c.kwargs.get("Key", "")
    ]
    assert json_calls, "search_results.json should still be uploaded"
    body = json.loads(json_calls[0].kwargs["Body"])
    assert body == []


@patch.dict("os.environ", ENV_VARS)
def test_fetch_ithub_jobs_respects_page_limit(mock_s3):
    """Pagination stops at page 20 even if pages keep returning cards."""
    session_mock = MagicMock()
    # Always return a page with a unique card — but URLs repeat after first,
    # so we need unique URLs per page. We patch parse_ithub_search_page instead.
    session_mock.get.return_value = make_response(text=SEARCH_PAGE_HTML)

    def fake_parse(html, fetched_at, dt, page, base_url):
        return [{"job_url": f"https://ithub.ua/jobs/job-{page}/", "source": "ithub"}]

    with patch("lib.ithub.tasks.build_session", return_value=session_mock):
        with patch("lib.ithub.tasks.parse_ithub_search_page", side_effect=fake_parse):
            with patch("lib.ithub.tasks.time.sleep"):
                from lib.ithub.tasks import fetch_ithub_jobs

                fetch_ithub_jobs(ds="2026-04-20")

    assert session_mock.get.call_count == 20, (
        f"Expected 20 page requests (limit), got {session_mock.get.call_count}"
    )
