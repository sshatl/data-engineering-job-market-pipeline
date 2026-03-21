from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
AIRFLOW_DAGS_PATH = PROJECT_ROOT / "airflow" / "dags"
SPARK_PATH = PROJECT_ROOT / "spark"

sys.path.insert(0, str(AIRFLOW_DAGS_PATH))
sys.path.insert(0, str(SPARK_PATH))