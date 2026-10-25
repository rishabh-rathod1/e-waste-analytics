# Makefile for E-Waste Big Data Analytics Platform

.PHONY: all setup start stop ingest profile spark sql mapreduce forecast mongo dashboard run clean help

all: run

help:
	@echo "Available commands:"
	@echo "  make start      - Start HDFS, YARN, and MongoDB services"
	@echo "  make stop       - Stop all cluster services and the dashboard gracefully"
	@echo "  make ingest     - Ingest FutuRaM CSV into HDFS"
	@echo "  make profile    - Run dataset profiler & data quality audit"
	@echo "  make spark      - Run PySpark preprocessing & Parquet generation"
	@echo "  make sql        - Run Spark SQL analytical queries"
	@echo "  make mapreduce  - Run Hadoop MapReduce streaming job"
	@echo "  make forecast   - Run Spark MLlib baseline regression forecast"
	@echo "  make mongo      - Seed MongoDB recycling facilities collection"
	@echo "  make dashboard  - Launch Streamlit interactive dashboard"
	@echo "  make run        - Execute full end-to-end pipeline"

start:
	./start_services.sh

stop:
	./stop_pipeline.sh

ingest:
	./scripts/ingest_hdfs.sh

profile:
	.venv/bin/python scripts/profile_dataset.py

spark:
	./scripts/spark_process.py

sql:
	./scripts/spark_sql.py

mapreduce:
	./scripts/run_mapreduce.sh

forecast:
	./scripts/forecasting.py

mongo:
	.venv/bin/python mongodb/seed/seed_facilities.py

dashboard:
	./start_dashboard.sh

run:
	./run_pipeline.sh
