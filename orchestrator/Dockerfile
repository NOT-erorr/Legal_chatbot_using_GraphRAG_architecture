FROM python:3.11-slim

WORKDIR /app

# Upgrade pip and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy source code into a subfolder so absolute imports like 'from orchestrator.config' work
COPY . /app/orchestrator

# Make python aware of the project root
ENV PYTHONPATH=/app

# Expose standard Cloud Run port
EXPOSE 8080

CMD ["uvicorn", "orchestrator.app:app", "--host", "0.0.0.0", "--port", "8080"]
