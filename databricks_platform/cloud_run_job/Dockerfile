FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY cloud_run_job/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY src ./src
COPY cloud_run_job/direct_sync_job.py ./direct_sync_job.py

CMD ["python", "direct_sync_job.py"]
