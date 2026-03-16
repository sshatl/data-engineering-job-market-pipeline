FROM spark:3.5.0

USER root

RUN mkdir -p /opt/spark/jars_extra
RUN mkdir -p /opt/spark/work-dir

RUN apt-get update \
 && apt-get install -y curl ca-certificates python3 \
 && rm -rf /var/lib/apt/lists/*

RUN curl -L -o /opt/spark/jars_extra/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

RUN curl -L -o /opt/spark/jars_extra/aws-java-sdk-bundle-1.12.262.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

RUN curl -L -o /opt/spark/jars_extra/postgresql-42.7.3.jar \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar

RUN cp /opt/spark/jars_extra/*.jar /opt/spark/jars/

COPY spark/jobs /opt/spark/work-dir/jobs

RUN chown -R spark:spark /opt/spark/work-dir

ENV SPARK_CLASSPATH="/opt/spark/jars_extra/*"
ENV PYTHONPATH="/opt/spark/work-dir"
ENV PYSPARK_PYTHON="python3"
ENV PYSPARK_DRIVER_PYTHON="python3"

WORKDIR /opt/spark/work-dir

USER spark