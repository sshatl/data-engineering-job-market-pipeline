import pytest
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


# ---- Edge cases: None, empty strings, whitespace ----


@pytest.mark.parametrize("value", [None, "", "   ", "\n\t"])
def test_detect_skills_returns_empty_for_blank_inputs(value):
    assert detect_skills(value) == []


@pytest.mark.parametrize("value", [None, "", "   "])
def test_detect_remote_type_returns_unknown_for_blank_inputs(value):
    assert detect_remote_type(location=value, text=value) == "unknown"


@pytest.mark.parametrize("value", [None, "", "   "])
def test_detect_seniority_returns_unknown_for_blank_inputs(value):
    assert detect_seniority(title=value, experience_text=value, text=value) == "unknown"


# ---- Edge cases: Unicode, Ukrainian, mixed languages ----


def test_detect_skills_case_insensitive():
    assert "python" in detect_skills("PYTHON and SPARK and SQL")
    assert "spark" in detect_skills("PYTHON and SPARK and SQL")


def test_detect_remote_type_ukrainian_remote():
    assert detect_remote_type("Віддалена робота", "") == "remote"


def test_detect_remote_type_ukrainian_hybrid():
    assert detect_remote_type("Гібридний формат", "") == "hybrid"


def test_detect_remote_type_ukrainian_office():
    assert detect_remote_type("Робота в офісі", "") == "onsite"


def test_detect_seniority_ukrainian_senior():
    assert detect_seniority("Старший інженер даних", "", "") == "senior"


def test_detect_seniority_ukrainian_lead():
    assert detect_seniority("Керівник команди даних", "", "") == "lead"


def test_detect_seniority_ukrainian_junior():
    assert detect_seniority("Молодший інженер даних", "", "") == "junior"


def test_detect_seniority_from_text_not_title():
    """Seniority detected from body text when title has no signal."""
    result = detect_seniority(
        title="Data Engineer",
        experience_text="",
        text="We are looking for a Senior specialist to join our team.",
    )
    assert result == "senior"


def test_detect_seniority_experience_in_ukrainian_text():
    """'від 5 років' in experience_text triggers senior."""
    result = detect_seniority(
        title="Data Engineer",
        experience_text="Досвід від 5 років у сфері data engineering",
        text="",
    )
    assert result == "senior"


# ---- Edge cases: mixed languages in one field ----


def test_detect_skills_mixed_languages():
    text = "Потрібен досвід з Python та Apache Spark, знання SQL обов'язково"
    skills = detect_skills(text)
    assert "python" in skills
    assert "spark" in skills
    assert "sql" in skills


def test_detect_remote_type_mixed_language():
    """Ukrainian text with English keyword 'remote' is detected."""
    result = detect_remote_type("", "Ми пропонуємо remote формат роботи з будь-якого міста")
    assert result == "remote"


# ---- Edge cases: special characters, long text ----


def test_detect_skills_with_special_chars():
    """Skill names surrounded by special characters are still detected."""
    text = "(Python), [SQL]; Airflow/Spark — Docker!!!"
    skills = detect_skills(text)
    assert "python" in skills
    assert "sql" in skills
    assert "airflow" in skills
    assert "spark" in skills
    assert "docker" in skills


def test_detect_remote_type_ignores_text_beyond_1500_chars():
    """Remote keyword after 1500 chars of body text is NOT detected."""
    padding = "a " * 800  # 1600 chars
    result = detect_remote_type("", padding + "remote work available")
    assert result == "unknown"


def test_detect_seniority_ignores_text_beyond_1500_chars():
    """Seniority keyword after 1500 chars of body text is NOT detected."""
    padding = "a " * 800
    result = detect_seniority("Data Engineer", "", padding + "senior level required")
    assert result == "unknown"