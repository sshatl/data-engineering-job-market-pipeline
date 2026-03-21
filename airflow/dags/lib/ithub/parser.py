from __future__ import annotations

from urllib.parse import urlencode, urljoin

from bs4 import BeautifulSoup

ITHUB_BASE_URL = "https://ithub.ua"

ITHUB_QUERY_NAME = "data_engineer"
ITHUB_QUERY_TEXT = "data engineer"
ITHUB_ROLE_FAMILY = "data_engineer"


def clean_text(value: str | None) -> str:
    if not value:
        return ""
    return " ".join(value.split())


def build_ithub_search_url(page: int) -> str:
    params = {
        "populate": ITHUB_QUERY_TEXT,
        "field_domainjob_tid": "All",
    }
    if page > 1:
        params["page"] = str(page - 1)

    return f"{ITHUB_BASE_URL}/jobs?{urlencode(params)}"


def parse_ithub_search_page(html: str, fetched_at: str, dt: str, page: int) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")

    results: list[dict] = []

    for card in soup.select("div.view-content > div.cardbox"):
        title_el = card.select_one("h2 a")
        if not title_el:
            continue

        href = title_el.get("href", "").strip()
        if not href:
            continue

        title = clean_text(title_el.get_text(" ", strip=True))
        job_url = urljoin(ITHUB_BASE_URL, href)

        company_el = card.select_one(".base a")
        company = clean_text(company_el.get_text(" ", strip=True)) if company_el else ""

        company_type_el = card.select_one(".jobscompanytype")
        company_type = clean_text(company_type_el.get_text(" ", strip=True)) if company_type_el else ""

        meta_el = card.select_one(".eventinfo.jobsinfo")
        meta_text = clean_text(meta_el.get_text(" ", strip=True)) if meta_el else ""

        date_el = card.select_one(".jobdate")
        published_text = clean_text(date_el.get_text(" ", strip=True)) if date_el else ""

        description_container = card.select_one(".cardboxdopinfo")
        description_full = clean_text(description_container.get_text(" ", strip=True)) if description_container else ""

        skill_items = [
            clean_text(li.get_text(" ", strip=True))
            for li in card.select(".eventmaintag li")
            if clean_text(li.get_text(" ", strip=True))
        ]

        results.append(
            {
                "job_url": job_url,
                "title": title,
                "company": company,
                "company_type": company_type,
                "meta_text": meta_text,
                "published_text": published_text,
                "description_full": description_full,
                "skills_raw": skill_items,
                "source": "ithub",
                "query_name": ITHUB_QUERY_NAME,
                "query_text": ITHUB_QUERY_TEXT,
                "role_family": ITHUB_ROLE_FAMILY,
                "dt": dt,
                "page": page,
                "fetched_at": fetched_at,
            }
        )

    return results