# MLOps End-to-End Pipeline Project

## Overview
End-to-end MLOps pipeline for Customer Churn Prediction using Apache Airflow, PySpark, MLflow, FastAPI, DVC, and Prometheus.

## Architecture
```
Data Source → PySpark Pipeline → MLflow Training → FastAPI Serving
                    ↓                                      ↓
              Apache Airflow                         Prometheus
              (Orchestration)                        (Monitoring)
                    ↓
                  DVC
              (Versioning)
```

## Tech Stack
| Component | Tool | Version |
|-----------|------|---------|
| Orchestration | Apache Airflow | 2.8.4 |
| Data Processing | Apache Spark | 3.5.1 |
| Experiment Tracking | MLflow | 2.12.1 |
| Model Serving | FastAPI | 0.110.0 |
| Versioning | DVC | 3.49.0 |
| Monitoring | Prometheus | 0.20.0 |
| CI/CD | GitHub Actions | - |
| Containerization | Docker | - |

## Quick Start

### Option A — GitHub Codespaces (Recommended)
1. Click **Code → Codespaces → Create codespace on main**
2. Wait for environment setup (~5 minutes)
3. Start services (see below)

### Option B — Local Setup
```bash
conda create -n mlops_project python=3.9
conda activate mlops_project
pip install -r requirements.txt
```

## Running Services

### Start Airflow
```bash
export AIRFLOW_HOME=/workspaces/mlops-project/airflow
airflow standalone
# Open http://localhost:8080 — admin/admin
```

### Start MLflow
```bash
mlflow ui --host 0.0.0.0 --port 5000
# Open http://localhost:5000
```

### Start FastAPI
```bash
uvicorn src.serving.app:app --host 0.0.0.0 --port 8000 --reload
# Open http://localhost:8000/docs
```

### Run Pipeline
```bash
python src/data_pipeline/ingest.py
python src/data_pipeline/preprocess.py
python src/models/train.py
```

## Docker Execution
```bash
# Build image
docker build -t mlops-project .

# Run full stack
docker-compose up -d

# Check services
docker-compose ps
```

## API Usage
```bash
# Health check
curl http://localhost:8000/health

# Predict
curl -X POST http://localhost:8000/predict \
  -H "Content-Type: application/json" \
  -d '{"features": [0.5, 1.2, 3.4, 0.8]}'
```

## Project Structure
```
mlops-project/
├── .devcontainer/          # GitHub Codespaces config
├── .github/workflows/      # CI/CD pipelines
├── airflow/dags/           # Airflow DAG definitions
├── data/
│   ├── raw/                # Raw data (DVC tracked)
│   └── processed/          # Processed data (DVC tracked)
├── src/
│   ├── data_pipeline/      # PySpark ingestion & preprocessing
│   ├── models/             # Model training with MLflow
│   └── serving/            # FastAPI application
├── monitoring/             # Prometheus config
├── notebooks/              # EDA notebooks
├── tests/                  # Unit tests
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── README.md
```

## CI/CD
GitHub Actions workflow triggers on push to main:
1. Run tests
2. Build Docker image
3. Push to registry

## Monitoring
- Prometheus metrics at `http://localhost:9090`
- API latency and prediction logging included
- Drift detection via MLflow model monitoring
