from lib.workua.parser import parse_workua_search_cards


def test_parse_workua_search_cards_returns_expected_card() -> None:
    html = """
    <div class="card job-link">
        <h2>
            <a href="/jobs/1234567/">Data Engineer</a>
        </h2>

        <div class="mt-xs">
            <span class="strong-600">Test Company</span>
            Kyiv, remote
        </div>

        <p class="ellipsis">Build ETL pipelines and maintain warehouse models</p>

        <time>21 March 2026</time>
    </div>
    """

    result = parse_workua_search_cards(
        html,
        query_name="data_engineer",
        query_text="data engineer",
        role_family="data_engineer",
        fetched_at="2026-03-21T10:00:00Z",
        dt="2026-03-21",
        page=1,
    )

    assert len(result) == 1

    job = result[0]

    assert job["source"] == "workua"
    assert job["query_name"] == "data_engineer"
    assert job["query_text"] == "data engineer"
    assert job["role_family"] == "data_engineer"
    assert job["job_id"] == "1234567"
    assert job["job_url"] == "https://www.work.ua/jobs/1234567/"
    assert job["title"] == "Data Engineer"
    assert job["company"] == "Test Company"
    assert job["location"] == "Kyiv, remote"
    assert job["snippet"] == "Build ETL pipelines and maintain warehouse models"
    assert job["published_text"] == "21 March 2026"
    assert job["fetched_at"] == "2026-03-21T10:00:00Z"
    assert job["dt"] == "2026-03-21"
    assert job["page"] == 1


def test_parse_workua_search_cards_skips_duplicates_and_invalid_links() -> None:
    html = """
    <div class="card job-link">
        <h2><a href="/jobs/1234567/">Data Engineer</a></h2>
        <div class="mt-xs">
            <span class="strong-600">Company A</span>
            Kyiv
        </div>
    </div>

    <div class="card job-link">
        <h2><a href="/jobs/1234567/">Duplicate Data Engineer</a></h2>
        <div class="mt-xs">
            <span class="strong-600">Company B</span>
            Lviv
        </div>
    </div>

    <div class="card job-link">
        <h2><a href="/companies/test-company/">Not a job link</a></h2>
        <div class="mt-xs">
            <span class="strong-600">Company C</span>
            Odesa
        </div>
    </div>
    """

    result = parse_workua_search_cards(
        html,
        query_name="data_engineer",
        query_text="data engineer",
        role_family="data_engineer",
        fetched_at="2026-03-21T10:00:00Z",
        dt="2026-03-21",
        page=1,
    )

    assert len(result) == 1
    assert result[0]["job_id"] == "1234567"
    assert result[0]["company"] == "Company A"