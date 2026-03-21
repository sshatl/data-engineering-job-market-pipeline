from lib.dou.parser import parse_dou_detail_html


def test_parse_dou_detail_html_returns_expected_fields() -> None:
    html = """
    <html>
        <body>
            <h1 class="g-h2">Senior Data Engineer</h1>

            <div class="b-compinfo">
                <div class="l-n">
                    <a>Test DOU Company</a>
                </div>
            </div>

            <div class="sh-info">Kyiv</div>
            <div class="date">21 March 2026</div>

            <div class="b-typo vacancy-section">
                <p>
                    We are looking for a Data Engineer with strong Python, SQL,
                    Airflow, and Spark experience to build and maintain scalable
                    data pipelines for analytics and operational use cases.
                </p>
                <p>
                    The candidate will work with cloud storage, orchestration,
                    transformation layers, and modern data stack tools in a
                    production-like environment.
                </p>
                <p>
                    Experience with ETL, warehousing, and data quality checks
                    will be a strong plus for this role.
                </p>
            </div>
        </body>
    </html>
    """

    result = parse_dou_detail_html(
        html=html,
        job_id="dou_job_1",
        job_url="https://jobs.dou.ua/vacancies/data-engineer-1/",
        ds="2026-03-21",
    )

    assert result["job_id"] == "dou_job_1"
    assert result["job_url"] == "https://jobs.dou.ua/vacancies/data-engineer-1/"
    assert result["title"] == "Senior Data Engineer"
    assert result["company"] == "Test DOU Company"
    assert result["location"] == "Kyiv"
    assert result["published_text"] == "21 March 2026"
    assert "Python" in result["description_full"]
    assert "Spark" in result["description_full"]
    assert "Senior Data Engineer" in result["page_text"]
    assert result["source"] == "dou"
    assert result["dt"] == "2026-03-21"


def test_parse_dou_detail_html_falls_back_when_primary_fields_missing() -> None:
    html = """
    <html>
        <body>
            <h1>Data Engineer</h1>

            <a href="/companies/test-company/">Fallback Company</a>

            <p>
                Львів
            </p>

            <p>
                21 March 2026
            </p>

            <p>
                This vacancy includes building ETL pipelines, maintaining data
                transformations, and working with Python and SQL in production.
            </p>
            <p>
                Candidates should understand orchestration, warehousing,
                monitoring, and scalable processing patterns across the stack.
            </p>
        </body>
    </html>
    """

    result = parse_dou_detail_html(
        html=html,
        job_id="dou_job_2",
        job_url="https://jobs.dou.ua/vacancies/data-engineer-2/",
        ds="2026-03-21",
    )

    assert result["title"] == "Data Engineer"
    assert result["company"] == "Fallback Company"
    assert result["location"] == "Львів"
    assert result["published_text"] == "21 March 2026"
    assert "Python and SQL" in result["description_full"]