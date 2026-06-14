#!/bin/bash
set -e

echo "=========================================="
echo "Setting up MLOps Project Environment"
echo "=========================================="

# Upgrade pip
pip install --upgrade pip

# Install MLOps packages
echo "Installing MLOps packages..."
pip install \
    apache-airflow==2.8.4 \
    mlflow==2.12.1 \
    pyspark==3.5.1 \
    fastapi==0.110.0 \
    uvicorn==0.29.0 \
    dvc==3.49.0 \
    prometheus-client==0.20.0 \
    scikit-learn==1.4.2 \
    xgboost==2.0.3 \
    imbalanced-learn==0.12.2 \
    kafka-python==2.0.2 \
    pydantic==2.6.4 \
    python-multipart==0.0.9 \
    psycopg2-binary==2.9.9 \
    pandas==2.2.1 \
    numpy==1.26.4 \
    matplotlib==3.8.4 \
    seaborn==0.13.2 \
    torch==2.2.2 \
    torchvision==0.17.2 \
    pytest==8.1.1 \
    httpx==0.27.0

# Create Airflow directory
echo "Setting up Airflow..."
mkdir -p /workspaces/mlops-project/airflow/dags
mkdir -p /workspaces/mlops-project/airflow/logs
mkdir -p /workspaces/mlops-project/airflow/plugins

# Initialize Airflow DB
export AIRFLOW_HOME=/workspaces/mlops-project/airflow
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////workspaces/mlops-project/airflow/airflow.db
airflow db init

# Create admin user
airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@mlops.com

# Create project directories
echo "Creating project structure..."
mkdir -p /workspaces/mlops-project/{data/raw,data/processed,src/data_pipeline,src/models,src/serving,monitoring,notebooks,tests,.github/workflows}

echo "=========================================="
echo "Setup Complete!"
echo "=========================================="
echo "Services to start:"
echo "  Airflow:  airflow standalone"
echo "  MLflow:   mlflow ui"
echo "  FastAPI:  uvicorn src.serving.app:app --reload"
echo "=========================================="
