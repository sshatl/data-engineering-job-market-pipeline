from __future__ import annotations

from lib.dou.parser import parse_dou_detail_html, parse_dou_search_cards
from lib.ithub.parser import parse_ithub_search_page
from lib.workua.parser import parse_workua_detail_html, parse_workua_search_cards

# ─────────────────────────────────────────────
# Shared helpers
# ─────────────────────────────────────────────

COMMON_KWARGS = dict(fetched_at="2026-04-24T10:00:00Z", dt="2026-04-24", page=1)
WORKUA_KWARGS = dict(**COMMON_KWARGS, query_name="data_engineer", query_text="data engineer", role_family="data_engineer")


# ─────────────────────────────────────────────
# DOU search parser edge cases
# ─────────────────────────────────────────────

def test_dou_search_empty_html_returns_empty_list():
    assert parse_dou_search_cards("", **COMMON_KWARGS) == []


def test_dou_search_completely_invalid_html_returns_empty_list():
    assert parse_dou_search_cards("<<<!not html at all>>>###", **COMMON_KWARGS) == []


def test_dou_search_no_vacancy_elements_returns_empty_list():
    html = "<html><body><p>No jobs here</p></body></html>"
    assert parse_dou_search_cards(html, **COMMON_KWARGS) == []


def test_dou_search_missing_optional_fields_still_returns_card():
    """Card with only href — no company, location, date, snippet."""
    html = """
    <ul>
        <li class="l-vacancy">
            <a class="vt" href="/vacancies/data-engineer-99/">Data Engineer</a>
        </li>
    </ul>
    """
    result = parse_dou_search_cards(html, **COMMON_KWARGS)
    assert len(result) == 1
    assert result[0]["job_url"] == "https://jobs.dou.ua/vacancies/data-engineer-99/"
    assert result[0]["company"] == ""
    assert result[0]["location"] == ""
    assert result[0]["snippet"] == ""


def test_dou_search_multiple_valid_cards_returned():
    html = """
    <ul>
        <li class="l-vacancy">
            <a class="vt" href="/vacancies/job-1/">Job One</a>
        </li>
        <li class="l-vacancy">
            <a class="vt" href="/vacancies/job-2/">Job Two</a>
        </li>
        <li class="l-vacancy">
            <a class="vt" href="/vacancies/job-3/">Job Three</a>
        </li>
    </ul>
    """
    result = parse_dou_search_cards(html, **COMMON_KWARGS)
    assert len(result) == 3


# ─────────────────────────────────────────────
# DOU detail parser edge cases
# ─────────────────────────────────────────────

def test_dou_detail_empty_html_returns_record_with_empty_fields():
    result = parse_dou_detail_html(html="", job_id="x", job_url="https://example.com", ds="2026-04-24")
    assert result["job_id"] == "x"
    assert result["source"] == "dou"
    assert result["description_full"] == ""


def test_dou_detail_completely_invalid_html_does_not_raise():
    result = parse_dou_detail_html(
        html="<<<broken>>>", job_id="y", job_url="https://example.com", ds="2026-04-24"
    )
    assert isinstance(result, dict)
    assert result["source"] == "dou"


def test_dou_detail_missing_title_returns_empty_string():
    html = "<html><body><div class='b-typo vacancy-section'><p>Description only.</p></div></body></html>"
    result = parse_dou_detail_html(html=html, job_id="z", job_url="https://example.com", ds="2026-04-24")
    assert result["title"] == ""
    assert "Description only" in result["description_full"]


# ─────────────────────────────────────────────
# Work.ua search parser edge cases
# ─────────────────────────────────────────────

def test_workua_search_empty_html_returns_empty_list():
    assert parse_workua_search_cards("", **WORKUA_KWARGS) == []


def test_workua_search_invalid_html_returns_empty_list():
    assert parse_workua_search_cards("<<<not html>>>", **WORKUA_KWARGS) == []


def test_workua_search_no_cards_in_valid_html_returns_empty_list():
    html = "<html><body><p>No jobs found</p></body></html>"
    assert parse_workua_search_cards(html, **WORKUA_KWARGS) == []


def test_workua_search_card_without_job_link_is_skipped():
    """A card whose link points to /companies/ (not /jobs/) is ignored."""
    html = """
    <div class="card job-link">
        <h2><a href="/companies/acme/">Acme Corp page</a></h2>
        <div class="mt-xs"><span class="strong-600">Acme</span> Kyiv</div>
    </div>
    """
    assert parse_workua_search_cards(html, **WORKUA_KWARGS) == []


def test_workua_search_missing_optional_fields_still_returns_card():
    """Card with only the job link — no company, snippet, date."""
    html = """
    <div class="card job-link">
        <h2><a href="/jobs/9999999/">Data Engineer</a></h2>
    </div>
    """
    result = parse_workua_search_cards(html, **WORKUA_KWARGS)
    assert len(result) == 1
    assert result[0]["job_id"] == "9999999"
    assert result[0]["company"] == ""


# ─────────────────────────────────────────────
# Work.ua detail parser edge cases
# ─────────────────────────────────────────────

def test_workua_detail_empty_html_returns_record_with_empty_fields():
    result = parse_workua_detail_html(html="", job_id="0", job_url="https://example.com", ds="2026-04-24")
    assert result["job_id"] == "0"
    assert result["source"] == "workua"
    assert result["description_full"] == ""


def test_workua_detail_invalid_html_does_not_raise():
    result = parse_workua_detail_html(
        html="<<<broken>>>", job_id="1", job_url="https://example.com", ds="2026-04-24"
    )
    assert isinstance(result, dict)
    assert result["source"] == "workua"


def test_workua_detail_missing_description_section():
    """Page without #job-description — title is extracted, description falls back to body text."""
    html = "<html><body><h1>Data Engineer</h1></body></html>"
    result = parse_workua_detail_html(html=html, job_id="2", job_url="https://example.com", ds="2026-04-24")
    assert result["title"] == "Data Engineer"
    assert isinstance(result["description_full"], str)  # graceful fallback, no crash


# ─────────────────────────────────────────────
# ITHub search parser edge cases
# ─────────────────────────────────────────────

def test_ithub_search_empty_html_returns_empty_list():
    assert parse_ithub_search_page(html="", **COMMON_KWARGS) == []


def test_ithub_search_invalid_html_returns_empty_list():
    assert parse_ithub_search_page(html="<<<not html>>>", **COMMON_KWARGS) == []


def test_ithub_search_no_cardbox_returns_empty_list():
    html = "<html><body><p>No jobs here</p></body></html>"
    assert parse_ithub_search_page(html=html, **COMMON_KWARGS) == []


def test_ithub_search_card_without_link_is_skipped():
    """cardbox with no <a href> in h2 is ignored."""
    html = """
    <div class="view-content">
        <div class="cardbox">
            <h2>No link here</h2>
            <div class="base"><a>Some Company</a></div>
        </div>
    </div>
    """
    assert parse_ithub_search_page(html=html, **COMMON_KWARGS) == []


def test_ithub_search_missing_optional_fields_still_returns_card():
    """Card with only the job link — no company, date, snippet, tags."""
    html = """
    <div class="view-content">
        <div class="cardbox">
            <h2><a href="/jobs/minimal-job-1">Minimal Job</a></h2>
        </div>
    </div>
    """
    result = parse_ithub_search_page(html=html, **COMMON_KWARGS)
    assert len(result) == 1
    assert "minimal-job-1" in result[0]["job_url"]
    assert result[0]["company"] == ""
    assert result[0]["skills_raw"] == []


def test_ithub_search_multiple_cards_all_returned():
    html = """
    <div class="view-content">
        <div class="cardbox">
            <h2><a href="/jobs/job-1">Job One</a></h2>
        </div>
        <div class="cardbox">
            <h2><a href="/jobs/job-2">Job Two</a></h2>
        </div>
    </div>
    """
    result = parse_ithub_search_page(html=html, **COMMON_KWARGS)
    assert len(result) == 2
