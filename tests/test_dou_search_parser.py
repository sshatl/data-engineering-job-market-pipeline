from lib.dou.parser import parse_dou_search_cards


def test_parse_dou_search_cards_returns_expected_card() -> None:
    html = """
    <ul>
        <li class="l-vacancy">
            <a class="vt" href="/vacancies/data-engineer-1/">Data Engineer</a>
            <div class="company">Test Company</div>
            <div class="cities">Kyiv</div>
            <div class="date">21 March 2026</div>
            <div class="sh-info">Build pipelines with Python and SQL</div>
        </li>
    </ul>
    """

    result = parse_dou_search_cards(
        html,
        fetched_at="2026-03-21T10:00:00Z",
        dt="2026-03-21",
        page=1,
    )

    assert len(result) == 1

    job = result[0]

    assert job["source"] == "dou"
    assert job["role_family"] == "data_engineer"
    assert job["job_url"] == "https://jobs.dou.ua/vacancies/data-engineer-1/"
    assert job["title"] == "Data Engineer"
    assert job["company"] == "Test Company"
    assert job["location"] == "Kyiv"
    assert job["published_text"] == "21 March 2026"
    assert job["snippet"] == "Build pipelines with Python and SQL"
    assert job["fetched_at"] == "2026-03-21T10:00:00Z"
    assert job["dt"] == "2026-03-21"
    assert job["page"] == 1


def test_parse_dou_search_cards_skips_cards_without_link() -> None:
    html = """
    <ul>
        <li class="l-vacancy">
            <div class="company">Broken Company</div>
        </li>
        <li class="l-vacancy">
            <a class="vt" href="/vacancies/data-engineer-2/">Data Engineer 2</a>
            <div class="company">Valid Company</div>
        </li>
    </ul>
    """

    result = parse_dou_search_cards(
        html,
        fetched_at="2026-03-21T10:00:00Z",
        dt="2026-03-21",
        page=1,
    )

    assert len(result) == 1
    assert result[0]["job_url"] == "https://jobs.dou.ua/vacancies/data-engineer-2/"
    assert result[0]["company"] == "Valid Company"