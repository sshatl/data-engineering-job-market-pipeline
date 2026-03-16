from __future__ import annotations

import re
from urllib.parse import quote_plus, urljoin

from bs4 import BeautifulSoup


WORKUA_BASE_URL = "https://www.work.ua"

WORKUA_QUERIES = [
    {
        "query_name": "data_engineer",
        "query_text": "data engineer",
        "role_family": "data_engineer",
    },
    {
        "query_name": "inzhener_danykh",
        "query_text": "інженер даних",
        "role_family": "data_engineer",
    },
]


def clean_text(value: str | None) -> str:
    if not value:
        return ""
    return " ".join(value.split())


def extract_text_from_html(html: str) -> str:
    if not html:
        return ""

    html = re.sub(r"(?is)<script.*?>.*?</script>", " ", html)
    html = re.sub(r"(?is)<style.*?>.*?</style>", " ", html)
    html = re.sub(r"(?s)<[^>]+>", " ", html)

    return clean_text(html)


def build_workua_search_url(query_text: str, page: int) -> str:
    query_slug = quote_plus(query_text)
    if page == 1:
        return f"{WORKUA_BASE_URL}/jobs-{query_slug}/"
    return f"{WORKUA_BASE_URL}/jobs-{query_slug}/?page={page}"


def extract_workua_job_id(job_url: str) -> str:
    match = re.search(r"/jobs/(\d+)/?", job_url)
    return match.group(1) if match else ""


def parse_workua_search_cards(
    html: str,
    *,
    query_name: str,
    query_text: str,
    role_family: str,
    fetched_at: str,
    dt: str,
    page: int,
) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")

    cards: list[dict] = []
    seen_job_ids: set[str] = set()

    job_cards = soup.select("div.card.job-link")

    for card in job_cards:
        title_el = card.select_one("h2 a[href^='/jobs/']")
        if not title_el:
            continue

        href = clean_text(title_el.get("href"))
        if not re.match(r"^/jobs/\d+/?$", href):
            continue

        job_url = urljoin(WORKUA_BASE_URL, href)
        job_id = extract_workua_job_id(job_url)

        if not job_id or job_id in seen_job_ids:
            continue

        title = clean_text(title_el.get_text(" ", strip=True))
        if not title:
            continue

        company = ""
        location = ""
        snippet = ""
        published_text = ""

        company_location_block = card.select_one("div.mt-xs")
        if company_location_block:
            company_el = company_location_block.select_one(".strong-600")
            if company_el:
                company = clean_text(company_el.get_text(" ", strip=True))

            block_text = clean_text(company_location_block.get_text(" ", strip=True))
            if company and block_text.startswith(company):
                location = clean_text(block_text[len(company):].lstrip(" ,"))
            else:
                location = block_text

        snippet_el = card.select_one("p.ellipsis")
        if snippet_el:
            snippet = clean_text(snippet_el.get_text(" ", strip=True))

        time_el = card.select_one("time")
        if time_el:
            published_text = clean_text(time_el.get_text(" ", strip=True))

        cards.append(
            {
                "source": "workua",
                "query_name": query_name,
                "query_text": query_text,
                "role_family": role_family,
                "job_id": job_id,
                "job_url": job_url,
                "title": title,
                "company": company,
                "location": location,
                "snippet": snippet,
                "published_text": published_text,
                "fetched_at": fetched_at,
                "dt": dt,
                "page": page,
            }
        )

        seen_job_ids.add(job_id)

    return cards


def parse_workua_detail_html(html: str, job_id: str, job_url: str, ds: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")

    page_text = extract_text_from_html(html)

    title = ""
    title_el = soup.select_one("h1")
    if title_el:
        title = clean_text(title_el.get_text(" ", strip=True))

    company = ""
    company_candidates = [
        "a[href*='/company/']",
        "div.card a[href*='/company/']",
        ".text-default-7 a[title]",
        ".text-default-7 strong",
        ".text-default-7 b",
        "[data-testid='company-name']",
    ]
    for selector in company_candidates:
        el = soup.select_one(selector)
        if el:
            value = clean_text(el.get_text(" ", strip=True))
            if value:
                company = value
                break

    description_full = ""
    main_content_candidates = [
        "div#job-description",
        "div[data-testid='job-description']",
        "div.wordwrap",
        "div.text-break",
        "div.overflow-wrap",
    ]

    for selector in main_content_candidates:
        el = soup.select_one(selector)
        if el:
            value = clean_text(el.get_text(" ", strip=True))
            if len(value) > 300:
                description_full = value
                break

    if not description_full:
        paragraphs = [clean_text(x.get_text(" ", strip=True)) for x in soup.select("p, li")]
        paragraphs = [x for x in paragraphs if len(x) > 40]
        description_full = clean_text(" ".join(paragraphs))

    if not description_full:
        description_full = page_text[:5000]

    location = ""
    employment_type = ""
    experience_text = ""
    education_text = ""
    published_text = ""

    meta_patterns = [
        r"(Повна зайнятість|Неповна зайнятість|Стажування|Часткова зайнятість)",
        r"(Досвід роботи від [^.]+)",
        r"(Вища освіта|Незакінчена вища освіта|Середня спеціальна освіта)",
        r"(Дистанційна робота|Віддалена робота|Гібридний формат роботи)",
    ]

    matches = []
    for pattern in meta_patterns:
        match = re.search(pattern, page_text, flags=re.IGNORECASE)
        if match:
            matches.append(clean_text(match.group(0)))

    for value in matches:
        lowered = value.lower()
        if "зайнятість" in lowered or "стажування" in lowered:
            employment_type = value
        elif "досвід роботи" in lowered:
            experience_text = value
        elif "освіта" in lowered:
            education_text = value
        elif "дистан" in lowered or "віддал" in lowered or "гібрид" in lowered:
            location = value

    if not location:
        loc_match = re.search(
            r"(Дистанційна робота|Віддалена робота|Гібридний формат роботи|Київ|Львів|Дніпро|Одеса|Харків|Івано-Франківськ|Вся Україна|за кордоном)",
            page_text,
            flags=re.IGNORECASE,
        )
        if loc_match:
            location = clean_text(loc_match.group(1))

    date_match = re.search(r"(Вакансія від\s+[^\n]+)", page_text, flags=re.IGNORECASE)
    if date_match:
        published_text = clean_text(date_match.group(1))

    return {
        "job_id": job_id,
        "job_url": job_url,
        "title": title,
        "company": company,
        "location": location,
        "published_text": published_text,
        "employment_type": employment_type,
        "experience_text": experience_text,
        "education_text": education_text,
        "description_full": description_full,
        "page_text": page_text,
        "source": "workua",
        "dt": ds,
    }