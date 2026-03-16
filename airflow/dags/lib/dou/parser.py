from __future__ import annotations

import re
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup


DOU_BASE_URL = "https://jobs.dou.ua"


def clean_text(value: str | None) -> str:
    if not value:
        return ""
    return " ".join(value.split())


def extract_dou_job_id(job_url: str) -> str:
    parsed = urlparse(job_url)
    path = parsed.path.strip("/")
    path = re.sub(r"[^A-Za-z0-9_-]+", "_", path)
    return path or "unknown"


def extract_text_from_html(html: str) -> str:
    if not html:
        return ""

    html = re.sub(r"(?is)<script.*?>.*?</script>", " ", html)
    html = re.sub(r"(?is)<style.*?>.*?</style>", " ", html)
    html = re.sub(r"(?s)<[^>]+>", " ", html)

    return clean_text(html)


def parse_dou_search_cards(
    html: str,
    *,
    fetched_at: str,
    dt: str,
    page: int,
) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")

    cards: list[dict] = []

    for vacancy in soup.select("li.l-vacancy"):
        title_el = vacancy.select_one("a.vt")
        if not title_el:
            continue

        href = title_el.get("href", "").strip()
        if not href:
            continue

        job_url = urljoin(DOU_BASE_URL, href)
        title = clean_text(title_el.get_text(" ", strip=True))

        company_el = vacancy.select_one(".company")
        company = clean_text(company_el.get_text(" ", strip=True)) if company_el else ""

        city_el = vacancy.select_one(".cities")
        location = clean_text(city_el.get_text(" ", strip=True)) if city_el else ""

        date_el = vacancy.select_one(".date")
        published_text = clean_text(date_el.get_text(" ", strip=True)) if date_el else ""

        snippet_el = vacancy.select_one(".sh-info")
        snippet = clean_text(snippet_el.get_text(" ", strip=True)) if snippet_el else ""

        cards.append(
            {
                "source": "dou",
                "role_family": "data_engineer",
                "job_url": job_url,
                "title": title,
                "company": company,
                "location": location,
                "published_text": published_text,
                "snippet": snippet,
                "fetched_at": fetched_at,
                "dt": dt,
                "page": page,
            }
        )

    return cards


def parse_dou_detail_html(html: str, job_id: str, job_url: str, ds: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")

    page_text = extract_text_from_html(html)

    title = ""
    title_el = soup.select_one("h1.g-h2, h1")
    if title_el:
        title = clean_text(title_el.get_text(" ", strip=True))

    company = ""
    company_el = soup.select_one(".b-compinfo .l-n a, .b-compinfo .l-n, .b-vacancy-title .company")
    if company_el:
        company = clean_text(company_el.get_text(" ", strip=True))
        company = re.sub(r"\s+Всі вакансії компанії.*$", "", company, flags=re.IGNORECASE).strip()

    if not company:
        company_link = soup.select_one("a[href*='/companies/']")
        if company_link:
            company = clean_text(company_link.get_text(" ", strip=True))
            company = company.replace("Всі вакансії компанії", "").strip()

    location = ""
    location_el = soup.select_one(".b-vacancy .sh-info, .sh-info")
    if location_el:
        location = clean_text(location_el.get_text(" ", strip=True))

    if not location:
        breadcrumbs = clean_text(page_text[:1000])
        loc_match = re.search(
            r"(віддалено|за кордоном|Київ|Львів|Дніпро|Одеса|Харків|Івано-Франківськ)",
            breadcrumbs,
            flags=re.IGNORECASE,
        )
        if loc_match:
            location = clean_text(loc_match.group(1))

    published_text = ""
    date_el = soup.select_one(".date")
    if date_el:
        published_text = clean_text(date_el.get_text(" ", strip=True))

    if not published_text:
        pub_match = re.search(r"(\d{1,2}\s+[а-яіїєґA-Za-z]+\s+\d{4})", page_text)
        if pub_match:
            published_text = clean_text(pub_match.group(1))

    description_full = ""
    desc_candidates = [
        ".b-typo.vacancy-section",
        ".l-vacancy",
        ".b-vacancy .text",
        ".b-vacancy .vacancy-section",
        ".b-typo",
    ]

    for selector in desc_candidates:
        el = soup.select_one(selector)
        if el:
            value = clean_text(el.get_text(" ", strip=True))
            if len(value) > 400:
                description_full = value
                break

    if not description_full:
        paragraphs = [clean_text(x.get_text(" ", strip=True)) for x in soup.select("p, li")]
        paragraphs = [x for x in paragraphs if len(x) > 40]
        description_full = clean_text(" ".join(paragraphs))

    if not description_full:
        description_full = page_text[:5000]

    return {
        "job_id": job_id,
        "job_url": job_url,
        "title": title,
        "company": company,
        "location": location,
        "published_text": published_text,
        "description_full": description_full,
        "page_text": page_text,
        "source": "dou",
        "dt": ds,
    }