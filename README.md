# Personalized Healthcare Recommendation System

A big data and machine learning pipeline that recommends medical treatments and
medications based on patient symptoms, history, and lifestyle — built on
Hadoop 3.2.1, Hive 2.3.2, Spark 3.2.2 (YARN), and Airflow 2.3.3.

---

## Prerequisites

| Requirement | Version | Notes |
|---|---|---|
| Docker + Docker Compose | Latest | All cluster services run in Docker |
| Java | 11+ | Required on host for Synthea data generation only |
| Make | Any | Used to run all project commands |
| RAM | 16 GB minimum | Recommended 20 GB |

---

## Quickstart

### Step 1 — Clone the mrugankray cluster repo and this project

```bash
# Clone the base cluster repo
git clone https://github.com/mrugankray/Big-Data-Cluster.git
cd Big-Data-Cluster

# Copy this project's files into the cluster directory
cp -r /path/to/this/project/* .
```

> **Note:** The project files sit alongside the cluster's compose file.
> The `docker-compose.override.yml` extends the base cluster automatically.

### Step 2 — Generate the dataset (on host machine)

```bash
make generate-data
```

This runs Synthea with a fixed seed to produce ~10,000 synthetic patients.
Output lands in `data/raw/`. Takes 3–6 minutes.

### Step 3 — Start the cluster

```bash
make up
```

Wait ~2 minutes for all containers to become healthy:

```bash
make ps
```

All containers should show `Up`. If any shows `Restarting`, check logs:

```bash
docker logs <container_name>
```

### Step 4 — First-boot setup (run once after first `make up`)

```bash
# 1. Verify installed versions match expectations
make verify

# 2. Install the healthcare-rec conda environment inside namenode
make install-conda

# 3. Initialise Airflow DB, create admin user, register SSH connection
make init-airflow
```

> After `make init-airflow`, update the `hive_server_ssh` password in the
> Airflow UI: **Admin → Connections → hive_server_ssh**

### Step 5 — Load data into HDFS

```bash
make upload-hdfs
```

### Step 6 — Create Hive tables

```bash
make create-hive-tables
```

### Step 7 — Trigger the full pipeline via Airflow

```bash
make pipeline
```

Monitor at **http://localhost:3000** (login: `admin` / `admin`)

---

## Port Reference

| Service | URL |
|---|---|
| Airflow UI | http://localhost:3000 |
| HDFS Namenode UI | http://localhost:9870 |
| YARN ResourceManager | http://localhost:8088 |
| Job History Server | http://localhost:19888 |
| Spark Driver UI | http://localhost:4040 |
| pgAdmin 4 | http://localhost:5000 |
| MLflow UI | http://localhost:5001 |
| Flask API | http://localhost:5050 |
| Streamlit Dashboard | http://localhost:8501 |

---

## Project Structure

```
.
├── hive-sqoop-postgres-cassandra-docker-compose.yaml  # base cluster
├── docker-compose.override.yml                        # project additions
├── hadoop.env                                         # cluster env vars
├── Makefile                                           # all commands
├── .env                                               # secrets (gitignored)
├── configs/                                           # cluster config overrides
├── conda/environment.yml                              # Python 3.9 dependencies
├── dags/                                              # Airflow DAGs (6 total)
├── data/
│   ├── raw/                                           # Synthea CSVs (gitignored)
│   └── synthea/                                       # generation script
├── docker/app/                                        # Flask + Streamlit Dockerfile
├── docker/mlflow/                                     # MLflow Dockerfile
├── src/
│   ├── ingestion/                                     # CSV validation + HDFS upload
│   ├── hive/                                          # HiveQL scripts
│   ├── processing/                                    # PySpark ETL (Milestone 2)
│   ├── models/                                        # ML models (Milestone 3)
│   ├── api/                                           # Flask REST API (Milestone 4)
│   ├── recommender/                                   # Recommendation engine (M4)
│   ├── dashboard/                                     # Streamlit UI (Milestone 4)
│   └── utils/                                         # config.py, spark_session.py
├── scripts/                                           # operational helper scripts
├── notebooks/                                         # exploration notebooks
├── models/                                            # saved model artifacts
├── tests/                                             # unit + integration tests
└── mlflow/                                            # MLflow artifact store
```

---

## Important constraints

- **All PySpark scripts use `master("yarn")`** — never `local[*]` in production
- **All HDFS paths use `hdfs://namenode:9000/...`** — never local container paths
- **Airflow runs inside the namenode** — DAG files in `dags/` are volume-mounted
- **Every BashOperator task activates conda** before running Python:
  ```bash
  source /root/anaconda/etc/profile.d/conda.sh && conda activate healthcare-rec
  ```
- **Two Postgres instances** — never write to `hive-metastore-postgresql`

---

## Milestone status

| Milestone | Status |
|---|---|
| 1 — Cluster setup, data ingestion, Hive tables | ✅ In progress |
| 2 — Feature engineering | 🔲 Pending |
| 3 — Model training + MLflow | 🔲 Pending |
| 4 — API + Dashboard | 🔲 Pending |
| 5 — Evaluation + Documentation | 🔲 Pending |