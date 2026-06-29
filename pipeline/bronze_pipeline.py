# Databricks notebook source
# MAGIC %run ../config/lib

# COMMAND ----------

# MAGIC %run ../framework/bronze_framework

# COMMAND ----------

# MAGIC %md
# MAGIC **Input Pipeline Name**

# COMMAND ----------

dbutils.widgets.text("pipeline_name", "Input Pipeline Name")
pipeline_name = dbutils.widgets.get("pipeline_name").strip()

# COMMAND ----------

# MAGIC %md
# MAGIC **Load pipeline configuration from the config table (`../config/pipeline_table_config`)**

# COMMAND ----------

bronze_config = (
    spark.table('project_pipeline.config_table')
    .filter(col('pipeline_name')==pipeline_name)
    .select('pipeline_name','header','delimiter','file_path','table_name','write_mode')
    .first()
)

# COMMAND ----------

bronze_pipeline = BronzeLayer(
    pipeline_name = bronze_config.pipeline_name,
    header = bronze_config.header,
    delimiter = bronze_config.delimiter,
    file_path = bronze_config.file_path,
    table_name = bronze_config.table_name,
    write_mode = bronze_config.write_mode
)

raw_df = bronze_pipeline.read_raw_data()
bronze_pipeline.save_bronze(raw_df)