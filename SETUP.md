# MLOps Project — Setup & Execution Guide
**Roll No:** da25m624  
**Course:** DA5402W — MLOps Lab

---

## Quickest Way to Run (Recommended for TAs)

### GitHub Codespaces — Zero Installation Required

1. Go to: `https://github.com/SrikRaja/mlops-project`
2. Click green **Code** button → **Codespaces** tab
3. Click **Create codespace on main**
4. Wait ~8-10 minutes for automatic setup
5. In the terminal that opens, run:

```bash
~/start_all.sh
```

6. Click the forwarded port links:

| Service | Port | Credentials |
|---------|------|-------------|
| Airflow UI | 8080 | admin / admin |
| MLflow UI | 5000 | - |
| FastAPI Docs | 8000 | - |
| Spark UI | 4040 | - |
| Kafka | 9092 | - |

---

## Option B — Docker (Local Machine)

### Requirements
- Docker Desktop installed
- Docker Compose installed
- Git installed

### Steps
```bash
# Clone repo
git clone https://github.com/SrikRaja/mlops-project.git
cd mlops-project

# Start all services
docker-compose up -d

# Check services are running
docker-compose ps

# View logs
docker-compose logs -f
```

### Services started by Docker Compose
- FastAPI model serving → http://localhost:8000
- MLflow tracking → http://localhost:5000
- Prometheus monitoring → http://localhost:9090
- Kafka broker → localhost:9092
- Zookeeper → localhost:2181

---

## Option C — Conda (Linux/Mac Only)

> ⚠️ Airflow does NOT run natively on Windows

### Requirements
- Anaconda or Miniconda
- Python 3.9
- Linux or macOS

### Steps
```bash
# Clone repo
git clone https://github.com/SrikRaja/mlops-project.git
cd mlops-project

# Create conda environment
conda env create -f environment.yml
conda activate mlops_project

# OR install from requirements
pip install -r requirements.txt

# Initialize Airflow
export AIRFLOW_HOME=$(pwd)/airflow
airflow db init
airflow users create \
    --username admin --password admin \
    --firstname Admin --lastname User \
    --role Admin --email admin@mlops.com

# Start services in separate terminals
airflow standalone          # Terminal 1 → http://localhost:8080
mlflow ui --host 0.0.0.0    # Terminal 2 → http://localhost:5000
uvicorn src.serving.app:app --reload --host 0.0.0.0  # Terminal 3
```

---

## Running Assignment 1

### Prerequisites
- Kafka must be running (`~/start_kafka.sh` in Codespaces)
- Topic `sensor_da25m624` must exist

### Part A — Kafka
```bash
# Create topic and run full Kafka demo
python assignment1/kafka.py
```

### Part B — Spark Structured Streaming
```bash
# Start Kafka first, then run producer, then:
python assignment1/spark_streaming.py

# In another terminal, run producer:
python assignment1/producer.py --topic sensor_da25m624 --records 2000 --rate 50
```

### Part C — Airflow DAG
```bash
# Copy DAG to Airflow dags folder
cp assignment1/airflow_dag.py airflow/dags/

# Open Airflow UI → http://localhost:8080
# Find DAG: da25m624_sensor_pipeline
# Toggle ON → Trigger manually
```

---

## Running End-Term Project Pipeline

```bash
# Step 1: Ingest and process data
python src/data_pipeline/ingest.py

# Step 2: Preprocess
python src/data_pipeline/preprocess.py

# Step 3: Train models (tracked in MLflow)
python src/models/train.py

# Step 4: Serve model
uvicorn src.serving.app:app --host 0.0.0.0 --port 8000

# Step 5: Test API
curl -X POST http://localhost:8000/predict \
  -H "Content-Type: application/json" \
  -d '{"features": [0.5, 1.2, 3.4, 0.8, 0.1, 0.9]}'
```

---

## Project Structure
```
mlops-project/
├── .devcontainer/          # GitHub Codespaces auto-setup
│   ├── devcontainer.json
│   └── setup.sh            # Installs all dependencies automatically
├── .github/workflows/
│   └── ci.yml              # GitHub Actions CI/CD
├── assignment1/
│   ├── producer.py         # Professor's Kafka producer
│   ├── kafka.py            # Part A: Consumer + metrics
│   ├── spark_streaming.py  # Part B: Spark Structured Streaming
│   └── airflow_dag.py      # Part C: Airflow DAG
├── src/
│   ├── data_pipeline/      # PySpark ingestion & preprocessing
│   ├── models/             # MLflow tracked training
│   └── serving/            # FastAPI model serving
├── data/raw/               # Raw data (DVC tracked)
├── data/processed/         # Processed data (DVC tracked)
├── monitoring/
│   └── prometheus.yml      # Prometheus scrape config
├── notebooks/              # EDA notebooks
├── tests/                  # Unit tests
├── Dockerfile              # FastAPI container
├── docker-compose.yml      # Full infrastructure stack
├── requirements.txt        # Python dependencies
├── environment.yml         # Conda environment
└── README.md               # Project overview
```

---

## Troubleshooting

| Issue | Fix |
|-------|-----|
| Airflow won't start on Windows | Use Codespaces or Docker |
| Kafka connection refused | Run `~/start_kafka.sh` first |
| Spark UI not showing | Check `http://localhost:4040` after first Spark job |
| MLflow not tracking | Set `MLFLOW_TRACKING_URI=http://localhost:5000` |
| Port already in use | Kill process: `lsof -ti:8080 \| xargs kill` |

---

## Tech Stack
| Component | Tool | Version |
|-----------|------|---------|
| Orchestration | Apache Airflow | 2.8.4 |
| Data Processing | Apache Spark | 3.5.1 |
| Stream Ingestion | Apache Kafka | 3.7.0 |
| Experiment Tracking | MLflow | 2.12.1 |
| Model Serving | FastAPI | 0.110.0 |
| Versioning | DVC | 3.49.0 |
| Monitoring | Prometheus | 0.20.0 |
| CI/CD | GitHub Actions | - |
| Containerization | Docker | - |
