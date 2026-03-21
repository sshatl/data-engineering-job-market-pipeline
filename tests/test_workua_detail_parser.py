from lib.workua.parser import parse_workua_detail_html


def test_parse_workua_detail_html_returns_expected_fields() -> None:
    html = """
    <html>
        <body>
            <h1>Data Engineer</h1>

            <a href="/company/test-company/">Test WorkUA Company</a>

            <div id="job-description">
                <p>
                    We are looking for a Data Engineer to develop ETL pipelines,
                    maintain warehouse models, and support analytics workloads.
                </p>
                <p>
                    The role includes Python, SQL, Airflow, Spark, and cloud
                    data platform responsibilities across a growing team.
                </p>
                <p>
                    Experience with orchestration, monitoring, and data quality
                    controls is highly desirable for this position.
                </p>
            </div>

            <div>
                Дистанційна робота
                Досвід роботи від 2 років
                Вища освіта
                Повна зайнятість
                Вакансія від 21 March 2026
            </div>
        </body>
    </html>
    """

    result = parse_workua_detail_html(
        html=html,
        job_id="1234567",
        job_url="https://www.work.ua/jobs/1234567/",
        ds="2026-03-21",
    )

    assert result["job_id"] == "1234567"
    assert result["job_url"] == "https://www.work.ua/jobs/1234567/"
    assert result["title"] == "Data Engineer"
    assert result["company"] == "Test WorkUA Company"
    assert result["location"] == "Дистанційна робота"
    assert result["published_text"] == "Вакансія від 21 March 2026"
    assert result["employment_type"] == "Повна зайнятість"
    assert result["experience_text"] == "Досвід роботи від 2 років"
    assert result["education_text"] == "Вища освіта"
    assert "Python" in result["description_full"]
    assert "Airflow" in result["description_full"]
    assert result["source"] == "workua"
    assert result["dt"] == "2026-03-21"


def test_parse_workua_detail_html_uses_fallback_description_from_paragraphs() -> None:
    html = """
    <html>
        <body>
            <h1>Junior Data Engineer</h1>

            <div class="text-default-7">
                <strong>Fallback WorkUA Company</strong>
            </div>

            <p>
                Київ
            </p>
            <p>
                Вакансія від 21 March 2026
            </p>
            <p>
                This role supports SQL development, pipeline maintenance,
                warehousing, and analytics data preparation across the business.
            </p>
            <p>
                The engineer will also help with documentation, monitoring,
                and transformation support for internal data workflows.
            </p>
        </body>
    </html>
    """

    result = parse_workua_detail_html(
        html=html,
        job_id="7654321",
        job_url="https://www.work.ua/jobs/7654321/",
        ds="2026-03-21",
    )

    assert result["title"] == "Junior Data Engineer"
    assert result["company"] == "Fallback WorkUA Company"
    assert result["location"] == "Київ"
    assert result["published_text"] == "Вакансія від 21 March 2026"
    assert "SQL development" in result["description_full"]