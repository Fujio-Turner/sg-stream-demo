FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY changes_worker.py .
COPY config.json .

ENTRYPOINT ["python", "changes_worker.py"]
CMD ["--config", "config.json"]
