FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    fastapi==0.109.2 \
    uvicorn[standard]==0.27.1 \
    kafka-python==2.0.2 \
    redis==5.0.1 \
    psycopg2-binary==2.9.9 \
    prometheus-client==0.19.0

COPY fraud_alert_service.py .

CMD ["uvicorn", "fraud_alert_service:app", "--host", "0.0.0.0", "--port", "8000"]
