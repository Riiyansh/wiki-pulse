FROM apache/spark:3.5.1
USER root
RUN pip install --no-cache-dir psycopg2-binary
COPY job.py /opt/spark/work-dir/job.py
WORKDIR /opt/spark/work-dir
CMD ["/opt/spark/bin/spark-submit", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.6.0", \
     "--master", "local[2]", \
     "job.py"]
