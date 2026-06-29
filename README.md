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

Modular code design with a strict separation of concerns.:

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
```
## 🛠️ Core Engineering & Technical Features

### 1. Production-Grade Metadata-Driven Design
* **The Mechanism:** Table schemas, storage paths, and processing constraints are completely decoupled into a centralized control catalog (`project_pipeline.config_table`).
* **The Value:** Zero hardcoded rules. The exact same generic PySpark engine dynamically adapts and processes entirely different datasets purely via configuration lookup, unlocking infinite pipeline scalability.

### 2. Parameterized Orchestration
* **The Mechanism:** Operates utilizing localized task injection managed programmatically through `dbutils.notebook.run` parameters.
* **The Value:** Guarantees strict, predictable layer-by-layer execution sequences while centralizing batch run tracking and avoiding operational memory overhead.

### 3. Encapsulated Data Quality (DQ) Isolation
* **The Mechanism:** Abstracted within an OOP `SilverFramework` class leveraging high-performance set operations (Left-Anti Joins) instead of costly row-by-row iteration.
* **The Value:** Instantly intercepts corrupt data and isolates it into an audit-ready `table_name_bad` layer—appending exact failure reason flags (`_is_row_dup`, `_is_key_dup`) while committing compliant records to `table_name_silver`.

## 🚀 Step-by-Step Execution Guide

To successfully run this metadata-driven pipeline, execute the notebooks sequentially according to the following orchestration flow:

### Step 1: Environment Provisioning (`config/ddl.py`)

Run the initialization notebook to establish the baseline data environment.

* **What it does:** It provisions the target schema `workspace.project_pipeline` and sets up the Unity Catalog Volume named `manual_file_project` to serve as the landing zone for incoming raw files.

### Step 2: Meta-Table Initialization & Data Seeding (`config/pipeline_table_config.py`)

Run this notebook to build the control parameters repository.

* **What it does:** It creates the centralized control table `project_pipeline.config_table` and performs an upsert (**`MERGE`**) to inject metadata configurations for your target entities:
1. `shop_table` (manages the `shop_mock.csv` pipeline configuration)
2. `fact_sales` (manages the `fact_sales.parquet` pipeline configuration)


* **Parameters Captured:** It stores vital operational variables including `file_path`, `schema_detail`, target keys (`primary_keys`), and operational `write_mode`.

### Step 3: Upload Raw Data

Upload your source data files into the designated Unity Catalog Volumes paths defined in your metadata store:

* **Shop Data:** `dbfs:/Volumes/workspace/project_pipeline/manual_file_project/shop_mock.csv`
* **Sales Data:** `dbfs:/Volumes/workspace/project_pipeline/manual_file_project/fact_sales.parquet`

### Step 4: Core Pipeline (`run_process.py`)

Run this master orchestrator script to trigger batch processing. It relies heavily on parameter passing to dynamically instruct downstream tasks.

* **Inside the Orchestrator:** This controller systematically executes downstream components by passing the `pipeline_name` parameter via `dbutils.notebook.run`:
  
**1. Bronze Ingestion Phase:**
* Runs shop ingestion: `dbutils.notebook.run('.../pipeline/bronze_pipeline', 0, {'pipeline_name': 'shop_table'})`
* Runs sales ingestion: `dbutils.notebook.run('.../pipeline/bronze_pipeline', 0, {'pipeline_name': 'fact_sales'})`
* *Behind the Scenes:* The `bronze_pipeline.py` consumes the incoming `pipeline_name` widget, extracts the matching rules from the control metadata table, and instantiates the `BronzeLayer` framework class to ingest raw data into the matching `_bronze` table while appending operational audit flags.


**2. Silver Cleansing & Partitioning Phase:**
* Runs shop validation: `dbutils.notebook.run('.../pipeline/silver_pipeline', 0, {'pipeline_name': 'shop_table'})`
* Runs sales validation: `dbutils.notebook.run('.../pipeline/silver_pipeline', 0, {'pipeline_name': 'fact_sales'})`
* *Behind the Scenes:* The `silver_pipeline.py` reads data from the corresponding `_bronze` table, applies custom Data Quality (DQ) criteria using the `SilverFramework` class, and dynamically splits rows into **`table_name_silver` (Cleaned Data)** or **`table_name_bad` (Error Audit Logs)**.


### Step 5: Gold Layer Analytics Generation (`Gold Layer.py`)

Execute the final reporting layer notebook to transform your structural components into business-ready intelligence.

* **What it does:** It consumes verified silver structures, filters operational timelines (e.g., `'2025-11-30'`), and performs a `LEFT JOIN` between `fact_sales_silver` and `shop_table_silver` on `shop_id`. It aggregates the underlying metrics to generate an executive-level summary of absolute sales velocity categorized by storefront names (`shop_name`).

---

*Powered by PySpark, Delta Lake, and Databricks Widgets.*

