FROM apache/airflow:3.1.1

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1


RUN pip install --no-cache-dir \
    boto3 \
    pandas \
    numpy \
    psycopg2-binary