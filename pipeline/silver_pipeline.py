# Databricks notebook source
# MAGIC %run ../config/lib

# COMMAND ----------

# MAGIC %run ../framework/silver_framework

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

silver_config = (
    spark.table('project_pipeline.config_table')
    .filter(col('pipeline_name')==pipeline_name)
    .select('pipeline_name','schema_detail','table_name','write_mode','keys')
    .first()
)

# COMMAND ----------

print(silver_config.pipeline_name)

# COMMAND ----------

silver_pipeline = SilverLayer(
    pipeline_name=silver_config.pipeline_name,
    schema_detail = silver_config.schema_detail,
    table_name = silver_config.table_name,
    keys=silver_config.keys,
    write_mode=silver_config.write_mode
)

bronze_df = silver_pipeline.read_from_bronze_sk()
invalid_df = silver_pipeline.get_invalid_record(bronze_df=bronze_df)
null_df = silver_pipeline.get_null_record(bronze_df=bronze_df)
dup_df = silver_pipeline.get_duplicate_record(bronze_df=bronze_df,null_df=null_df)
bad_df = silver_pipeline.get_all_bad_record(invalid_df=invalid_df, null_df=null_df, dup_df=dup_df)
good_df = silver_pipeline.get_all_good_record(bronze_df = bronze_df,bad_df= bad_df)
silver_pipeline.load_bad_record(bad_df)
silver_pipeline.load_good_record(good_df)