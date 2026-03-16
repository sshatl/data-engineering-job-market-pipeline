FROM apache/airflow:2.9.3

USER root

RUN apt-get update \
 && apt-get install -y ca-certificates curl gnupg \
 && install -m 0755 -d /etc/apt/keyrings \
 && curl -fsSL https://download.docker.com/linux/debian/gpg -o /etc/apt/keyrings/docker.asc \
 && chmod a+r /etc/apt/keyrings/docker.asc \
 && echo \
    "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/debian \
    $(. /etc/os-release && echo $VERSION_CODENAME) stable" \
    > /etc/apt/sources.list.d/docker.list \
 && apt-get update \
 && apt-get install -y docker-ce-cli \
 && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir \
    boto3==1.34.162 \
    botocore==1.34.162 \
    requests==2.32.3 \
    beautifulsoup4==4.12.3 \
    psycopg2-binary==2.9.9 \
    dbt-postgres==1.8.2