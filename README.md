# 🚀 Metadata-Driven Data Pipeline & Analytics (Medallion Architecture)

An upgrade from the baseline `shop-data-pipeline-medallion` project. This enterprise-grade repository introduces a fully decoupled, **Metadata-Driven Data Pipeline** built on **Databricks** utilizing the complete **Medallion Architecture (Bronze, Silver, and Gold Layers)**. 

By transitioning to **Object-Oriented Programming (OOP)**, the architecture removes hardcoded ingestion rules, making the system highly reusable and scalable via structured configurations.

## 🏗️ Architectural Overview

The pipeline automates data ingestion, standardizes schema validation, and computes analytical insights from retail data sources through three unified layers:

[ Raw CSV / Parquet ]
│
▼ (Bronze Framework)
┌────────────────────────────────┐
│      bronze_pipeline.py        │ ──► Dynamic Ingestion + Audit Metadata Tracking
└────────────────────────────────┘
│
▼ (Silver Framework: OOP Cleansing & Integrity Checks)
┌────────────────────────────────┐
│      silver_pipeline.py        │ ──► Executes Data Quality Rules & Partitions Records
└────────────────────────────────┘
├──► Good Records (table_name_silver) ──► Strict Type Casting & Cleansed Data
└──► Bad Records (table_name_bad)     ──► Isolated Anomalies with Error Reason Logs
│
▼ (Gold Layer: Business Analytics)
┌────────────────────────────────┐
│         Gold Layer.py          │ ──► Strategic Sales Quantity Insights
└────────────────────────────────┘

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
