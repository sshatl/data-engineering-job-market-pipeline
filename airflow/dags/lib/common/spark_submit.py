from __future__ import annotations


def build_spark_submit_cmd(app_path: str, env_vars: dict[str, str]) -> str:
    """
    Build docker-exec spark-submit command for compose-based Spark cluster.
    Assumes shared Python code is baked into the Spark image.
    """
    container = "jm_spark_master"
    env_flags = " ".join([f'-e {k}="{v}"' for k, v in env_vars.items()])

    cmd = f"""
docker exec -i {env_flags} {container} bash -lc '
cd /opt/spark/work-dir && \
/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.executorEnv.PYTHONPATH=/opt/spark/work-dir \
  --conf spark.driverEnv.PYTHONPATH=/opt/spark/work-dir \
  --conf spark.executorEnv.PYSPARK_PYTHON=python3 \
  --conf spark.pyspark.python=python3 \
  {app_path}
'
""".strip()

    return cmd