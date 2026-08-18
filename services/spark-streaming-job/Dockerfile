FROM docker.io/apache/spark:3.5.9-scala2.12-java17-python3-ubuntu

USER root
WORKDIR /opt/translation/spark-streaming-job

COPY requirements.txt .
RUN python3 -m pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/

USER spark

ENTRYPOINT ["/opt/spark/bin/spark-submit"]
CMD ["--master", "local[*]", "/opt/translation/spark-streaming-job/src/main.py"]
