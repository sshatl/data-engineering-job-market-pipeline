from jobs.common.job_enrichment import (
    detect_remote_type,
    detect_seniority,
    detect_skills,
)


def test_detect_skills_returns_sorted_unique_skills() -> None:
    text = """
    We use Python, SQL, Apache Spark, Airflow, Docker, GitHub Actions,
    PostgreSQL, and Python again in our modern data stack.
    """

    result = detect_skills(text)

    assert result == [
        "airflow",
        "ci/cd",
        "docker",
        "git",
        "postgres",
        "python",
        "spark",
        "sql",
    ]


def test_detect_skills_returns_empty_list_for_empty_text() -> None:
    assert detect_skills("") == []
    assert detect_skills(None) == []


def test_detect_remote_type_prefers_location_signal() -> None:
    result = detect_remote_type(
        location="Remote, Europe",
        text="Office-based work in Kyiv",
    )

    assert result == "remote"


def test_detect_remote_type_detects_hybrid_from_text() -> None:
    result = detect_remote_type(
        location="Kyiv",
        text="We offer a hybrid work model with 2 office days per week.",
    )

    assert result == "hybrid"


def test_detect_remote_type_detects_onsite_from_text() -> None:
    result = detect_remote_type(
        location="Kyiv",
        text="This is an office-based role with on-site collaboration.",
    )

    assert result == "onsite"


def test_detect_remote_type_returns_unknown_when_no_signal_found() -> None:
    result = detect_remote_type(
        location="Kyiv",
        text="Data engineering role focused on ETL and warehousing.",
    )

    assert result == "unknown"


def test_detect_seniority_detects_lead_from_title() -> None:
    result = detect_seniority(
        title="Lead Data Engineer",
        experience_text="",
        text="",
    )

    assert result == "lead"


def test_detect_seniority_detects_senior_from_title() -> None:
    result = detect_seniority(
        title="Senior Data Engineer",
        experience_text="",
        text="",
    )

    assert result == "senior"


def test_detect_seniority_detects_middle_from_title() -> None:
    result = detect_seniority(
        title="Middle Data Engineer",
        experience_text="",
        text="",
    )

    assert result == "middle"


def test_detect_seniority_detects_junior_from_title() -> None:
    result = detect_seniority(
        title="Junior Data Engineer",
        experience_text="",
        text="",
    )

    assert result == "junior"


def test_detect_seniority_detects_senior_from_experience() -> None:
    result = detect_seniority(
        title="Data Engineer",
        experience_text="5+ years of experience",
        text="Building ETL pipelines and data platforms.",
    )

    assert result == "senior"


def test_detect_seniority_detects_middle_from_ukrainian_experience() -> None:
    result = detect_seniority(
        title="Data Engineer",
        experience_text="Досвід роботи від 3 років",
        text="Warehouse and orchestration responsibilities.",
    )

    assert result == "middle"


def test_detect_seniority_returns_unknown_for_low_experience_without_title_signal() -> None:
    result = detect_seniority(
        title="Data Engineer",
        experience_text="1+ years of experience",
        text="General data engineering tasks.",
    )

    assert result == "unknown"