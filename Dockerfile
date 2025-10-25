FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ /app/

ENV PYTHONUNBUFFERED=1

# Default to runner; override for prepare/baseline
ENTRYPOINT ["python", "/app/runner.py"]
