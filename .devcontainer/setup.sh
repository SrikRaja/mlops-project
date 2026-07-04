#!/bin/bash
set -e

echo "=========================================="
echo "  MLOps Complete Environment Setup"
echo "  End-Term Project + Assignment 1"
echo "=========================================="

# ── 1. System packages ──────────────────────
echo "[1/7] Installing system packages..."
sudo apt-get update -q
sudo apt-get install -y -q \
    default-jdk \
    wget \
    curl \
    unzip \
    netcat \
    procps \
    lsof

# Verify Java
java -version
echo "JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))"
export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))

# ── 2. Python packages ───────────────────────
echo "[2/7] Installing Python packages..."
pip install --upgrade pip --quiet

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
    httpx==0.27.0 \
    black==24.3.0 \
    jupyter==1.0.0

echo "Python packages installed successfully"

# ── 3. Kafka Installation ────────────────────
echo "[3/7] Installing Apache Kafka..."
KAFKA_VERSION="3.7.0"
SCALA_VERSION="2.13"
KAFKA_TGZ="kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"
KAFKA_URL="https://downloads.apache.org/kafka/${KAFKA_VERSION}/${KAFKA_TGZ}"

wget -q "${KAFKA_URL}" -O /tmp/kafka.tgz
sudo mkdir -p /opt/kafka
sudo tar -xzf /tmp/kafka.tgz -C /opt/
sudo mv /opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION}/* /opt/kafka/
sudo chmod -R 755 /opt/kafka
rm /tmp/kafka.tgz

# Add Kafka to PATH permanently
echo 'export KAFKA_HOME=/opt/kafka' >> ~/.bashrc
echo 'export PATH=$PATH:$KAFKA_HOME/bin' >> ~/.bashrc
echo 'export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))' >> ~/.bashrc
export PATH=$PATH:/opt/kafka/bin

echo "Kafka installed at /opt/kafka"

# ── 4. Spark-Kafka connector jar ─────────────
echo "[4/7] Downloading Spark-Kafka connector..."
SPARK_KAFKA_JAR="spark-sql-kafka-0-10_2.12-3.5.1.jar"
SPARK_JARS_DIR="/usr/local/lib/python3.9/dist-packages/pyspark/jars"

# Download required jars for Spark Structured Streaming + Kafka
wget -q "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/${SPARK_KAFKA_JAR}" \
    -O "${SPARK_JARS_DIR}/${SPARK_KAFKA_JAR}" || echo "Warning: Could not download Spark-Kafka jar"

wget -q "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar" \
    -O "${SPARK_JARS_DIR}/kafka-clients-3.4.1.jar" || echo "Warning: Could not download kafka-clients jar"

echo "Spark-Kafka connector ready"

# ── 5. Airflow Setup ─────────────────────────
echo "[5/7] Setting up Airflow..."
mkdir -p /workspaces/mlops-project/airflow/{dags,logs,plugins}

export AIRFLOW_HOME=/workspaces/mlops-project/airflow
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////workspaces/mlops-project/airflow/airflow.db
export AIRFLOW__WEBSERVER__SECRET_KEY=mlops-secret-key-2026

airflow db init

airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@mlops.com

echo "Airflow initialized — login: admin/admin"

# ── 6. Project Directory Structure ───────────
echo "[6/7] Creating project structure..."

mkdir -p /workspaces/mlops-project/{
data/raw,
data/processed,
src/data_pipeline,
src/models,
src/serving,
monitoring,
notebooks,
tests,
assignment1/partA,
assignment1/partB,
assignment1/partC,
docker
}

# Placeholder files
touch /workspaces/mlops-project/data/raw/.gitkeep
touch /workspaces/mlops-project/data/processed/.gitkeep

# ── 7. Helper Scripts ────────────────────────
echo "[7/7] Creating helper scripts..."

# Kafka start script
cat > ~/start_kafka.sh << 'KAFKA_EOF'
#!/bin/bash
echo "Starting Zookeeper..."
/opt/kafka/bin/zookeeper-server-start.sh -daemon /opt/kafka/config/zookeeper.properties
sleep 8
echo "Starting Kafka broker..."
/opt/kafka/bin/kafka-server-start.sh -daemon /opt/kafka/config/server.properties
sleep 8

# Verify
if /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092 > /dev/null 2>&1; then
    echo "✅ Kafka is running on localhost:9092"
else
    echo "❌ Kafka failed to start — check logs in /opt/kafka/logs"
fi
KAFKA_EOF
chmod +x ~/start_kafka.sh

# Kafka stop script
cat > ~/stop_kafka.sh << 'KAFKA_EOF'
#!/bin/bash
echo "Stopping Kafka..."
/opt/kafka/bin/kafka-server-stop.sh
sleep 3
echo "Stopping Zookeeper..."
/opt/kafka/bin/zookeeper-server-stop.sh
echo "Kafka stopped"
KAFKA_EOF
chmod +x ~/stop_kafka.sh

# Start all services script
cat > ~/start_all.sh << 'ALL_EOF'
#!/bin/bash
echo "=========================================="
echo "  Starting All MLOps Services"
echo "=========================================="

# Kafka
echo "Starting Kafka..."
~/start_kafka.sh

# MLflow
echo "Starting MLflow..."
mlflow ui --host 0.0.0.0 --port 5000 &
sleep 3

# Airflow
echo "Starting Airflow..."
export AIRFLOW_HOME=/workspaces/mlops-project/airflow
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////workspaces/mlops-project/airflow/airflow.db
export AIRFLOW__WEBSERVER__SECRET_KEY=mlops-secret-key-2026
airflow standalone &
sleep 5

echo "=========================================="
echo "✅ All services started!"
echo ""
echo "  Airflow:  http://localhost:8080  (admin/admin)"
echo "  MLflow:   http://localhost:5000"
echo "  Kafka:    localhost:9092"
echo ""
echo "  To start FastAPI:"
echo "  uvicorn src.serving.app:app --host 0.0.0.0 --port 8000 --reload"
echo "=========================================="
ALL_EOF
chmod +x ~/start_all.sh

# Kafka topic creation helper for Assignment 1
cat > ~/create_assignment_topic.sh << 'TOPIC_EOF'
#!/bin/bash
ROLLNO=$1
if [ -z "$ROLLNO" ]; then
    echo "Usage: ~/create_assignment_topic.sh <your_rollno>"
    exit 1
fi
TOPIC="sensor_${ROLLNO}"
echo "Creating topic: ${TOPIC}"
/opt/kafka/bin/kafka-topics.sh \
    --create \
    --topic "${TOPIC}" \
    --bootstrap-server localhost:9092 \
    --partitions 3 \
    --replication-factor 1

echo "✅ Topic created: ${TOPIC}"
echo "Verify with:"
echo "  kafka-topics.sh --describe --topic ${TOPIC} --bootstrap-server localhost:9092"
TOPIC_EOF
chmod +x ~/create_assignment_topic.sh

echo ""
echo "=========================================="
echo "✅ Setup Complete!"
echo ""
echo "Quick Start:"
echo "  ~/start_all.sh          — Start all services"
echo "  ~/start_kafka.sh        — Start Kafka only"
echo "  ~/stop_kafka.sh         — Stop Kafka"
echo ""
echo "Assignment 1:"
echo "  ~/create_assignment_topic.sh <rollno>"
echo ""
echo "Services:"
echo "  Airflow:  http://localhost:8080  (admin/admin)"
echo "  MLflow:   http://localhost:5000"
echo "  FastAPI:  http://localhost:8000/docs"
echo "  Spark UI: http://localhost:4040"
echo "  Kafka:    localhost:9092"
echo "=========================================="
