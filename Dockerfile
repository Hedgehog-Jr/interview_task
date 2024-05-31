FROM openjdk:11-jdk-slim

COPY ./app /app
COPY requirements.txt /app/requirements.txt
COPY config.yaml /app/config.yaml

ENV SPARK_XML_VERSION=0.15.0
ENV INPUT_DIR="/app/input"
ENV OUTPUT_DIR="/app/output"
ENV TMP_DIR="/app/tmp"
ENV JAR_PATH="/jars/spark-xml_2.12-${SPARK_XML_VERSION}.jar"

RUN apt-get update && \
    apt-get install -y --no-install-recommends python3 python3-pip && \
    apt-get install -y wget && \
    apt-get clean

RUN mkdir -p /jars && \
    wget -P /jars https://repo1.maven.org/maven2/com/databricks/spark-xml_2.12/${SPARK_XML_VERSION}/spark-xml_2.12-${SPARK_XML_VERSION}.jar

VOLUME ["input", "output"]
WORKDIR /app

RUN pip install -r requirements.txt

CMD ["python3", "main.py"]
