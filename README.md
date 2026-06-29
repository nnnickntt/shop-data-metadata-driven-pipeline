# 🚀 shop-data-metadata-driven

Enterprise-grade, **Metadata-Driven Data Pipeline & Analytics Framework** built on **Databricks** utilizing the full **Medallion Architecture (Bronze, Silver, and Gold Layers)**. 

This repository marks a major architectural upgrade from the baseline `shop-data-pipeline-medallion` project. By transitioning to **Object-Oriented Programming (OOP)**, the architecture removes hardcoded ingestion rules, making the system highly reusable and scalable via structured configurations.

## 🏗️ Architectural Overview

The pipeline automates data ingestion, standardizes schema validation, and computes analytical insights from retail data sources through three unified layers:

```mermaid
graph TD
    %% Define Styles
    classDef framework fill:#2c3e50,stroke:#34495e,stroke-width:2px,color:#fff;
    classDef data fill:#ecf0f1,stroke:#bdc3c7,stroke-width:1px,color:#2c3e50;
    classDef good fill:#2ecc71,stroke:#27ae60,stroke-width:1px,color:#fff;
    classDef bad fill:#e74c3c,stroke:#c0392b,stroke-width:1px,color:#fff;

    %% Nodes Definitions
    Raw["📄 Raw CSV / Parquet"]:::data
    
    subgraph Bronze_Layer ["🟫 Bronze Layer"]
        Bronze["💻 bronze_pipeline.py<br>• Dynamic Ingestion<br>• Audit Metadata Tracking"]:::framework
    end

    subgraph Silver_Layer ["⬜ Silver Layer"]
        Silver["💻 silver_pipeline.py<br>• Executes Data Quality Rules<br>• Partitions Records"]:::framework
        Good["✅ Good Records (table_name_silver)<br>• Strict Type Casting<br>• Cleansed Data"]:::good
        Bad["❌ Bad Records (table_name_bad)<br>• Isolated Anomalies<br>• Error Reason Logs"]:::bad
    end

    subgraph Gold_Layer ["🟨 Gold Layer"]
        Gold["💻 Gold Layer.py<br>• Strategic Sales Quantity Insights<br>• Business Aggregation"]:::framework
    end

    %% Flow Connections
    Raw --> Bronze
    Bronze --> Silver
    Silver --> Good
    Silver --> Bad
    Good --> Gold
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
