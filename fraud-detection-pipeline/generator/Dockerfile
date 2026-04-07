FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    kafka-python==2.0.2 \
    faker==22.2.0

COPY transaction_producer.py .

CMD ["python", "-u", "transaction_producer.py"]
