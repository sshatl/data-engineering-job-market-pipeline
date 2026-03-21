from __future__ import annotations

import re

SKILL_PATTERNS = [
    ("sql", [r"\bsql\b", r"postgresql", r"mysql", r"t-sql", r"pl/sql", r"\bms sql\b", r"\bmssql\b"]),
    ("python", [r"\bpython\b"]),
    ("spark", [r"\bspark\b", r"apache spark", r"\bpyspark\b"]),
    ("airflow", [r"\bairflow\b", r"apache airflow"]),
    ("dbt", [r"\bdbt\b"]),
    ("kafka", [r"\bkafka\b"]),
    ("aws", [r"\baws\b", r"amazon web services", r"amazon s3", r"aws glue", r"aws lambda"]),
    ("gcp", [r"\bgcp\b", r"google cloud"]),
    ("azure", [r"\bazure\b", r"azure data factory", r"azure synapse", r"microsoft azure"]),
    ("docker", [r"\bdocker\b"]),
    ("kubernetes", [r"\bkubernetes\b", r"\bk8s\b"]),
    ("terraform", [r"\bterraform\b"]),
    ("databricks", [r"\bdatabricks\b", r"azure databricks"]),
    ("snowflake", [r"\bsnowflake\b"]),
    ("postgres", [r"\bpostgres\b", r"\bpostgresql\b"]),
    ("etl", [r"\betl\b"]),
    ("elt", [r"\belt\b"]),
    ("power bi", [r"power bi", r"microsoft power bi"]),
    ("tableau", [r"\btableau\b"]),
    ("bigquery", [r"\bbigquery\b"]),
    ("redshift", [r"\bredshift\b", r"amazon redshift", r"aws redshift"]),
    ("git", [r"\bgit\b", r"github"]),
    ("ci/cd", [r"\bci/cd\b", r"\bcicd\b", r"gitlab ci", r"github actions", r"jenkins", r"azure devops"]),
    ("clickhouse", [r"\bclickhouse\b"]),
    ("java", [r"\bjava\b"]),
    ("scala", [r"\bscala\b"]),
    ("hadoop", [r"\bhadoop\b"]),
    ("flink", [r"\bflink\b", r"apache flink"]),
    ("bash", [r"\bbash\b", r"\bshell scripting\b"]),
    ("linux", [r"\blinux\b"]),
    ("mongodb", [r"\bmongodb\b", r"\bmongo\b"]),
    ("nosql", [r"\bnosql\b"]),
    ("oracle", [r"\boracle\b"]),
    ("looker", [r"\blooker\b"]),
    ("superset", [r"\bsuperset\b"]),
]


def _normalize_text(text: str | None) -> str:
    if not text:
        return ""
    return text.lower()


def detect_skills(text: str | None) -> list[str]:
    normalized = _normalize_text(text)
    if not normalized:
        return []

    found: list[str] = []

    for skill_name, patterns in SKILL_PATTERNS:
        if any(re.search(pattern, normalized) for pattern in patterns):
            found.append(skill_name)

    return sorted(set(found))


def detect_remote_type(location: str | None, text: str | None) -> str:
    location_norm = _normalize_text(location)
    text_norm = _normalize_text(text)

    remote_patterns = [
        r"\bremote\b",
        r"віддален",
        r"дистанц",
        r"remotely",
        r"work from home",
        r"home office",
        r"fully remote",
    ]
    hybrid_patterns = [
        r"\bhybrid\b",
        r"гібрид",
        r"комбінован",
        r"змішан",
    ]
    onsite_patterns = [
        r"\bonsite\b",
        r"\boffice\b",
        r"офіс",
        r"office-based",
        r"on-site",
    ]

    def _match_any(patterns: list[str], value: str) -> bool:
        return any(re.search(pattern, value) for pattern in patterns)

    if _match_any(remote_patterns, location_norm):
        return "remote"
    if _match_any(hybrid_patterns, location_norm):
        return "hybrid"
    if _match_any(onsite_patterns, location_norm):
        return "onsite"

    head_text = text_norm[:1500]

    if _match_any(remote_patterns, head_text):
        return "remote"
    if _match_any(hybrid_patterns, head_text):
        return "hybrid"
    if _match_any(onsite_patterns, head_text):
        return "onsite"

    return "unknown"


def detect_seniority(title: str | None, experience_text: str | None, text: str | None) -> str:
    title_norm = _normalize_text(title)
    exp_norm = _normalize_text(experience_text)
    text_norm = _normalize_text(text)
    head_text = text_norm[:1500]

    lead_patterns = [
        r"\blead\b",
        r"team lead",
        r"tech lead",
        r"chapter lead",
        r"head of",
        r"керівник",
        r"тімлід",
        r"лід\b",
    ]
    senior_patterns = [
        r"\bsenior\b",
        r"\bsr\b",
        r"\bsr\.\b",
        r"старший",
    ]
    middle_patterns = [
        r"\bmiddle\b",
        r"\bmid\b",
        r"\bmid-level\b",
    ]
    junior_patterns = [
        r"\bjunior\b",
        r"\bjr\b",
        r"\bjr\.\b",
        r"молодший",
        r"intern",
        r"trainee",
    ]

    def _match_any(patterns: list[str], value: str) -> bool:
        return any(re.search(pattern, value) for pattern in patterns)
    if _match_any(lead_patterns, title_norm):
        return "lead"
    if _match_any(senior_patterns, title_norm):
        return "senior"
    if _match_any(middle_patterns, title_norm):
        return "middle"
    if _match_any(junior_patterns, title_norm):
        return "junior"
    if _match_any(lead_patterns, head_text):
        return "lead"
    if _match_any(senior_patterns, head_text):
        return "senior"
    if _match_any(middle_patterns, head_text):
        return "middle"
    if _match_any(junior_patterns, head_text):
        return "junior"
    exp_source = f"{exp_norm} {head_text[:800]}"
    if re.search(r"\b(5|6|7|8|9|10)\+?\s*(years|year|рок|роки|років)\b", exp_source):
        return "senior"
    if re.search(r"\b(3|4)\+?\s*(years|year|рок|роки|років)\b", exp_source):
        return "middle"
    if re.search(r"\b(1|2)\+?\s*(years|year|рік|роки|років)\b", exp_source):
        return "unknown"
    if re.search(r"від\s*(5|6|7|8|9|10)\s*рок", exp_source):
        return "senior"
    if re.search(r"від\s*(3|4)\s*рок", exp_source):
        return "middle"
    if re.search(r"від\s*(1|2)\s*рок", exp_source):
        return "unknown"

    return "unknown"