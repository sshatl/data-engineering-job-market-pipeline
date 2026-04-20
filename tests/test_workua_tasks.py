from __future__ import annotations

import json
from io import BytesIO
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

ENV_VARS = {
    "MINIO_ENDPOINT": "http://minio:9000",
    "MINIO_ACCESS_KEY": "minioadmin",
    "MINIO_SECRET_KEY": "minioadmin",
    "BRONZE_BUCKET": "jobs-bronze",
}

SEARCH_PAGE_HTML = """
<html><body>
<div class="card job-link">
  <h2><a href="/jobs/12345678/">Data Engineer</a></h2>
  <div class="mt-xs"><span class="strong-600">Acme Corp</span> Kyiv</div>
  <p class="ellipsis">Build pipelines with Python and SQL</p>
  <time>20 April 2026</time>
</div>
</body></html>
"""

DETAIL_HTML = """
<html><body>
  <h1 class="add-top-sm">Data Engineer</h1>
  <div id="job-description"><p>Full job description here.</p></div>
</body></html>
"""


def make_response(text: str = "", status: int = 200):
    resp = MagicMock()
    resp.status_code = status
    resp.text = text
    resp.raise_for_status = MagicMock()
    return resp


@pytest.fixture()
def mock_s3():
    with patch("lib.workua.tasks.get_s3_client") as mock_factory:
        yield mock_factory.return_value


@patch.dict("os.environ", ENV_VARS)
def test_fetch_workua_jobs_uploads_search_results(mock_s3):
    """fetch_workua_jobs parses each query page and uploads parsed JSON + raw HTML."""
    # Two queries exist (data_engineer, inzhener_danykh); each gets page 1 with cards
    # then page 2 empty to stop pagination
    empty_resp = make_response(text="<html><body></body></html>")

    session_mock = MagicMock()
    session_mock.get.side_effect = [
        make_response(text=SEARCH_PAGE_HTML),  # query 1, page 1
        empty_resp,                             # query 1, page 2 — stop
        make_response(text=SEARCH_PAGE_HTML),  # query 2, page 1 (same URL → deduped, no upload)
        empty_resp,                             # query 2, page 2 — stop
    ]

    with patch("lib.workua.tasks.build_session", return_value=session_mock):
        from lib.workua.tasks import fetch_workua_jobs

        fetch_workua_jobs(ds="2026-04-20")

    put_calls = mock_s3.put_object.call_args_list
    keys = [c.kwargs["Key"] for c in put_calls]

    # At least one query should produce a JSON upload and raw HTML upload
    assert any("search_results_page_1.json" in k for k in keys), f"search results JSON not uploaded; keys={keys}"
    assert any("search_page_1.html" in k for k in keys), f"raw HTML not uploaded; keys={keys}"


@patch.dict("os.environ", ENV_VARS)
def test_fetch_workua_jobs_deduplicates_across_queries(mock_s3):
    """Same job URL appearing in two queries is only included once globally."""
    session_mock = MagicMock()
    # Two queries, each returns the same single card then an empty page
    session_mock.get.side_effect = [
        make_response(text=SEARCH_PAGE_HTML),  # query 1, page 1 — has cards
        make_response(text="<html></html>"),   # query 1, page 2 — empty, stop
        make_response(text=SEARCH_PAGE_HTML),  # query 2, page 1 — same card, deduped
        make_response(text="<html></html>"),   # query 2, page 2 — empty, stop
    ]

    with patch("lib.workua.tasks.build_session", return_value=session_mock):
        from lib.workua.tasks import fetch_workua_jobs

        fetch_workua_jobs(ds="2026-04-20")

    json_calls = [
        c for c in mock_s3.put_object.call_args_list
        if "search_results_page_1.json" in c.kwargs.get("Key", "")
    ]
    # First query uploads; second query produces no new cards → no upload
    assert len(json_calls) == 1, f"Expected 1 JSON upload, got {len(json_calls)}"


@patch.dict("os.environ", ENV_VARS)
def test_fetch_workua_jobs_http_error_skips_query(mock_s3):
    """HTTP error on a search page causes that query to stop, not raise."""
    import requests as req_lib

    session_mock = MagicMock()
    session_mock.get.side_effect = req_lib.exceptions.RequestException("timeout")

    with patch("lib.workua.tasks.build_session", return_value=session_mock):
        from lib.workua.tasks import fetch_workua_jobs

        fetch_workua_jobs(ds="2026-04-20")  # must not raise

    mock_s3.put_object.assert_not_called()


@patch.dict("os.environ", ENV_VARS)
def test_fetch_workua_detail_pages_uploads_html(mock_s3):
    """fetch_workua_detail_pages fetches each job URL and uploads raw HTML."""
    ds = "2026-04-20"
    cards = [{"job_url": "https://www.work.ua/jobs/12345678/"}]

    def get_object_side_effect(Bucket, Key):  # noqa: N803
        if "search_results_page_1.json" in Key:
            return {"Body": BytesIO(json.dumps(cards).encode())}
        raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")

    mock_s3.get_object.side_effect = get_object_side_effect

    detail_resp = make_response(text=DETAIL_HTML)
    session_mock = MagicMock()
    session_mock.get.return_value = detail_resp

    with patch("lib.workua.tasks.build_session", return_value=session_mock):
        from lib.workua.tasks import fetch_workua_detail_pages

        fetch_workua_detail_pages(ds=ds)

    put_calls = mock_s3.put_object.call_args_list
    keys = [c.kwargs["Key"] for c in put_calls]
    assert any("details_raw" in k and ".html" in k for k in keys), f"detail HTML not uploaded; keys={keys}"


@patch.dict("os.environ", ENV_VARS)
def test_fetch_workua_detail_pages_skips_on_http_error(mock_s3):
    """fetch_workua_detail_pages skips a card on HTTP error without raising."""
    import requests as req_lib

    ds = "2026-04-20"
    cards = [
        {"job_url": "https://www.work.ua/jobs/111/"},
        {"job_url": "https://www.work.ua/jobs/222/"},
    ]

    def get_object_side_effect(Bucket, Key):  # noqa: N803
        if "search_results_page_1.json" in Key:
            return {"Body": BytesIO(json.dumps(cards).encode())}
        raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")

    mock_s3.get_object.side_effect = get_object_side_effect

    session_mock = MagicMock()
    session_mock.get.side_effect = [
        req_lib.exceptions.RequestException("fail"),
        make_response(text=DETAIL_HTML),
    ]

    with patch("lib.workua.tasks.build_session", return_value=session_mock):
        from lib.workua.tasks import fetch_workua_detail_pages

        fetch_workua_detail_pages(ds=ds)

    assert mock_s3.put_object.call_count == 1, "Only successful card should be uploaded"


@patch.dict("os.environ", ENV_VARS)
def test_parse_workua_detail_pages_uploads_json(mock_s3):
    """parse_workua_detail_pages reads raw HTML from S3 and uploads parsed JSON."""
    ds = "2026-04-20"

    def get_object_side_effect(Bucket, Key):  # noqa: N803
        return {"Body": BytesIO(DETAIL_HTML.encode())}

    mock_s3.get_object.side_effect = get_object_side_effect
    mock_s3.list_objects_v2.return_value = {
        "Contents": [{"Key": f"jobs/source=workua/details_raw/dt={ds}/job_12345678.html"}]
    }

    from lib.workua.tasks import parse_workua_detail_pages

    parse_workua_detail_pages(ds=ds)

    put_calls = mock_s3.put_object.call_args_list
    keys = [c.kwargs["Key"] for c in put_calls]
    assert any("details_parsed" in k and ".json" in k for k in keys), f"parsed JSON not uploaded; keys={keys}"


@patch.dict("os.environ", ENV_VARS)
def test_parse_workua_detail_pages_empty_prefix(mock_s3):
    """parse_workua_detail_pages does nothing when no raw files exist."""
    mock_s3.list_objects_v2.return_value = {"Contents": []}

    from lib.workua.tasks import parse_workua_detail_pages

    parse_workua_detail_pages(ds="2026-04-20")

    mock_s3.put_object.assert_not_called()
