FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt --no-cache-dir -r requirements.txt
COPY scripts/ ./scripts/
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--workers", "1", "--threads", "8", "--timeout", "30", "--access-logfile", "-", "--error-logfile", "-", "scripts.main:app"]