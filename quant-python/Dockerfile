# syntax=docker/dockerfile:1
FROM python:3.11-slim

WORKDIR /app

# System deps needed by LightGBM and pandas
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc g++ libgomp1 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

# Cache mount keeps downloaded wheels between builds so retries are instant
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --timeout=300 --retries=5 -r requirements.txt

COPY . .

# Directory for persisted ML model (mounted as a volume in compose)
RUN mkdir -p models

EXPOSE 8050

CMD ["python", "main.py"]
