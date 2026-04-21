# =============================================================================
# Makefile
# Single entry point for all project operations.
# Run from the project root.
# =============================================================================

COMPOSE_FILE := docker-compose.yaml

SERVICES := namenode datanode resourcemanager nodemanager historyserver \
            hive-server hive-metastore hive-metastore-postgresql \
            external_postgres_db external_pgadmin mlflow app

.PHONY: help generate-data up down ps logs \
        install-conda \
        init-airflow init-airflow-2 init-airflow-3 \
        verify upload-hdfs create-hive-tables \
        pipeline clean-hdfs

# ---------------------------------------------------------------------------
# Default
# ---------------------------------------------------------------------------
help:
	@echo ""
	@echo "  Personalized Healthcare Recommendation System"
	@echo "  ============================================="
	@echo ""
	@echo "  Setup:"
	@echo "    make generate-data      Generate Synthea patient dataset on host"
	@echo "    make up                 Start all required cluster services"
	@echo "    make down               Stop all containers"
	@echo "    make ps                 Show running containers"
	@echo "    make logs               Follow namenode logs"
	@echo ""
	@echo "  First-boot (run in this order after 'make up'):"
	@echo "    make verify             Verify versions inside namenode"
	@echo "    make install-conda      Install healthcare-rec conda env (staged)"
	@echo "    make init-airflow       Step 1 — Airflow DB init"
	@echo "    make init-airflow-2     Step 2 — Create admin user"
	@echo "    make init-airflow-3     Step 3 — SSH connection + start daemons"
	@echo ""
	@echo "  Data pipeline:"
	@echo "    make upload-hdfs        Validate CSVs and upload to HDFS"
	@echo "    make create-hive-tables Run create_tables.hql via beeline"
	@echo "    make pipeline           Trigger dag_ingest_to_hdfs in Airflow"
	@echo ""
	@echo "  Maintenance:"
	@echo "    make clean-hdfs         Remove all HDFS project data (destructive)"
	@echo ""

# ---------------------------------------------------------------------------
# Data generation (runs on HOST)
# ---------------------------------------------------------------------------
generate-data:
	@echo ">>> Generating Synthea dataset (population=10000, seed=42)..."
	@bash data/synthea/generate_data.sh

# ---------------------------------------------------------------------------
# Cluster lifecycle
# ---------------------------------------------------------------------------
up:
	@echo ">>> Starting cluster services..."
	@docker-compose -f $(COMPOSE_FILE) up $(SERVICES) -d
	@echo ""
	@echo "Wait ~2 minutes then run: make ps"

down:
	@docker-compose -f $(COMPOSE_FILE) down

ps:
	@docker-compose -f $(COMPOSE_FILE) ps

logs:
	@docker-compose -f $(COMPOSE_FILE) logs -f namenode

# ---------------------------------------------------------------------------
# Conda (staged install to avoid OOM kill / exit 137)
# ---------------------------------------------------------------------------
install-conda:
	@echo ">>> Installing healthcare-rec conda environment (staged)..."
	@docker exec -it namenode bash /home/scripts/install_conda_env.sh

# ---------------------------------------------------------------------------
# Airflow (three separate steps to avoid OOM kill / exit 137)
# ---------------------------------------------------------------------------
init-airflow:
	@echo ">>> Airflow Step 1: DB init..."
	@docker exec -it namenode bash /home/scripts/init_airflow.sh

init-airflow-2:
	@echo ">>> Airflow Step 2: Create admin user..."
	@docker exec -it namenode bash /home/scripts/init_airflow_step2.sh

init-airflow-3:
	@echo ">>> Airflow Step 3: SSH connection + start daemons..."
	@docker exec -it namenode bash /home/scripts/init_airflow_step3.sh

# ---------------------------------------------------------------------------
# Verify
# ---------------------------------------------------------------------------
verify:
	@echo ">>> Verifying versions inside namenode..."
	@docker exec -it namenode bash /home/scripts/verify_versions.sh

# ---------------------------------------------------------------------------
# Data pipeline
# ---------------------------------------------------------------------------
upload-hdfs:
	@echo ">>> Validating and uploading CSVs to HDFS..."
	@bash scripts/upload_to_hdfs.sh

create-hive-tables:
	@echo ">>> Creating Hive tables..."
	@bash scripts/create_hive_tables.sh

# ---------------------------------------------------------------------------
# Airflow trigger
# ---------------------------------------------------------------------------
pipeline:
	@echo ">>> Triggering dag_ingest_to_hdfs..."
	@docker exec -it namenode bash -c \
		"source /root/anaconda/etc/profile.d/conda.sh && \
		 conda activate healthcare-rec && \
		 airflow dags trigger dag_ingest_to_hdfs"
	@echo ""
	@echo "Monitor at: http://localhost:3000"

# ---------------------------------------------------------------------------
# Maintenance
# ---------------------------------------------------------------------------
clean-hdfs:
	@echo ">>> WARNING: This will delete all project data from HDFS."
	@read -p "Are you sure? [y/N] " confirm && [ "$$confirm" = "y" ]
	@docker exec -it namenode bash -c \
		"hdfs dfs -rm -r -skipTrash hdfs://namenode:9000/data || true"
	@echo "HDFS data cleared."