# 🚀 shop-data-metadata-driven

An enterprise-grade, **Metadata-Driven Data Pipeline & Analytics Framework** built on **Databricks** utilizing the full **Medallion Architecture (Bronze, Silver, and Gold Layers)**. 

This repository marks a major architectural upgrade from the baseline `shop-data-pipeline-medallion` project. By transitioning to **Object-Oriented Programming (OOP)**, the architecture removes hardcoded ingestion rules, making the system highly reusable and scalable via structured configurations.

---

## 🏗️ Architectural Overview

The pipeline automates data ingestion, standardizes schema validation, and computes analytical insights through three unified layers:

* **🟫 Bronze Layer (`bronze_pipeline.py`)**
    * Handles dynamic data ingestion from Raw CSV / Parquet.
    * Appends system management and audit metadata tracking.
* **⬜ Silver Layer (`silver_pipeline.py`)**
    * Executes complex data quality rules and cleanses incoming data.
    * Partitions data into **Good Records (`table_name_silver`)** (Strict Type Casting passed) and **Bad Records (`table_name_bad`)** (Isolated anomalies with error reason logs).
* **🟨 Gold Layer (`Gold Layer.py`)**
    * Performs business aggregations and strategic sales analytics.

---

## 📂 Repository Structure

The codebase is organized into modular components to enforce a clean separation of concerns:

```text
├── config/
│   ├── ddl.py                     # Environment provisioning (Schemas & Unity Catalog Volumes)
│   ├── lib.py                     # Centralized PySpark library imports and ecosystem dependencies
│   └── pipeline_table_config.py   # Metadata repository managing schema configurations and keys
├── framework/
│   ├── bronze_framework.py        # OOP Blueprint for standardized Bronze layer extraction
│   └── silver_framework.py        # OOP Blueprint for automated data cleansing & rule enforcement
├── pipeline/
│   ├── bronze_pipeline.py         # Parameterized pipeline notebook executing the Bronze process
│   ├── silver_pipeline.py         # Parameterized pipeline notebook executing the Silver process
├── run_process.py                 # Master Orchestrator managing batch execution via dbutils.notebook.run
└── Gold Layer.py                  # End-user business aggregation and analytics delivery
---
## 📂 Repository Structure

The codebase is organized into modular components to enforce a clean separation of concerns:

```text
├── config/
│   ├── ddl.py                     # Environment provisioning (Schemas & Unity Catalog Volumes)
│   ├── lib.py                     # Centralized PySpark library imports and ecosystem dependencies
│   └── pipeline_table_config.py   # Metadata repository managing schema configurations and keys
├── framework/
│   ├── bronze_framework.py        # OOP Blueprint for standardized Bronze layer extraction
│   └── silver_framework.py        # OOP Blueprint for automated data cleansing & rule enforcement
├── pipeline/
│   ├── bronze_pipeline.py         # Parameterized pipeline notebook executing the Bronze process
│   ├── silver_pipeline.py         # Parameterized pipeline notebook executing the Silver process
├── run_process.py                 # Master managing batch execution via dbutils.notebook.run
└── Gold Layer.py                  # End-user business aggregation and analytics delivery
