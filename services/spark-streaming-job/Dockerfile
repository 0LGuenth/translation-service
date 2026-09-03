FROM docker.io/apache/spark:3.5.9-scala2.12-java17-python3-ubuntu

USER root
WORKDIR /opt/translation/spark-streaming-job

COPY requirements.txt .
RUN python3 -m pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/

# Executors run as `spark` and fetch dependency jars into this workdir, so it
# must be writable by that user (COPY above created it root-owned).
RUN chown -R spark /opt/translation/spark-streaming-job

USER spark

# Keep the base image's /opt/entrypoint.sh: Spark-launched executor pods rely on
# it to dispatch the "executor" arg. The driver sets its own command in its manifest.
