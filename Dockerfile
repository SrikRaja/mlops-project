FROM python:3.9-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    default-jdk \
    wget \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set Java home
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$PATH:$JAVA_HOME/bin

# Copy requirements
COPY requirements.txt .

# Install Python packages (lightweight subset for serving)
RUN pip install --no-cache-dir \
    fastapi==0.110.0 \
    uvicorn==0.29.0 \
    scikit-learn==1.4.2 \
    xgboost==2.0.3 \
    pandas==2.2.1 \
    numpy==1.26.4 \
    mlflow==2.12.1 \
    prometheus-client==0.20.0 \
    pydantic==2.6.4 \
    python-multipart==0.0.9

# Copy source code
COPY src/serving/ ./src/serving/
COPY src/models/ ./src/models/

# Expose API port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Run FastAPI
CMD ["uvicorn", "src.serving.app:app", "--host", "0.0.0.0", "--port", "8000"]
