FROM apache/airflow:2.10.5-python3.11

USER root

# Install system dependencies for DBC decompression and PostgreSQL
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

USER airflow

ENV PYTHONPATH=/opt/qimed

WORKDIR /opt/qimed

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY src/ ./src/
COPY dags/ ./dags/
COPY config/ ./config/
