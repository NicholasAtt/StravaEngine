FROM jupyter/pyspark-notebook:spark-3.5.0

USER root

COPY requirements.txt /tmp/requirements.txt
RUN pip install --default-timeout=100 --no-cache-dir -r /tmp/requirements.txt

RUN mkdir -p /tmp/spark-checkpoints/raw \
             /tmp/spark-checkpoints/agg \
             /tmp/spark-checkpoints/raw_es \
             /tmp/.ivy2 \
             /opt/spark-data/models && \
    chmod -R 777 /tmp/spark-checkpoints /tmp/.ivy2 /opt/spark-data

USER jovyan