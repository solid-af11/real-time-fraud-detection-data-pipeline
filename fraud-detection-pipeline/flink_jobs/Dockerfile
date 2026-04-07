FROM python:3.11-slim

WORKDIR /jobs

RUN pip install --no-cache-dir \
    kafka-python==2.0.2 \
    redis==5.0.1 \
    scikit-learn==1.4.0 \
    numpy==1.26.3 \
    psycopg2-binary==2.9.9

COPY . .
