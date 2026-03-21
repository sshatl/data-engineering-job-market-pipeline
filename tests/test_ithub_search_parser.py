from lib.ithub.parser import parse_ithub_search_page


def test_parse_ithub_search_page_returns_expected_job_card() -> None:
    html = """
    <div class="view-content">
        <div class="cardbox">
            <h2>
                <a href="/jobs/data-engineer-123"> Data Engineer </a>
            </h2>

            <div class="base">
                <a>Test Company</a>
            </div>

            <div class="jobscompanytype">Product company</div>

            <div class="eventinfo jobsinfo">Kyiv • Remote</div>

            <div class="jobdate">3 days ago</div>

            <div class="cardboxdopinfo">
                Build ETL pipelines with Python and SQL
            </div>

            <ul class="eventmaintag">
                <li>Python</li>
                <li>SQL</li>
                <li>Airflow</li>
            </ul>
        </div>
    </div>
    """

    result = parse_ithub_search_page(
        html=html,
        fetched_at="2026-03-21T10:00:00Z",
        dt="2026-03-21",
        page=1,
    )

    assert len(result) == 1

    job = result[0]

    assert job["job_url"] == "https://ithub.ua/jobs/data-engineer-123"
    assert job["title"] == "Data Engineer"
    assert job["company"] == "Test Company"
    assert job["company_type"] == "Product company"
    assert job["meta_text"] == "Kyiv • Remote"
    assert job["published_text"] == "3 days ago"
    assert job["description_full"] == "Build ETL pipelines with Python and SQL"
    assert job["skills_raw"] == ["Python", "SQL", "Airflow"]

    assert job["source"] == "ithub"
    assert job["query_name"] == "data_engineer"
    assert job["query_text"] == "data engineer"
    assert job["role_family"] == "data_engineer"
    assert job["dt"] == "2026-03-21"
    assert job["page"] == 1
    assert job["fetched_at"] == "2026-03-21T10:00:00Z"