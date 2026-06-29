# Databricks notebook source
# MAGIC %run ./lib

# COMMAND ----------

# MAGIC %md
# MAGIC **Table Config**

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table project_pipeline.config_table (
# MAGIC     pipeline_name string
# MAGIC   , file_path string
# MAGIC   , header string
# MAGIC   , delimiter string
# MAGIC   , table_name string
# MAGIC   , schema_detail map<string,string>
# MAGIC   , keys array<string>
# MAGIC   , write_mode string
# MAGIC )

# COMMAND ----------

def upsert_table(df:DataFrame,table_name:str,keys:list)->DataFrame:
    delta_obj = DeltaTable.forName(spark,table_name)
    return(
        delta_obj.alias("t")
        .merge(
            df.alias("s"), " AND ".join([f"t.{k} = s.{k}" for k in keys])
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    

# COMMAND ----------

# MAGIC %md
# MAGIC **Merge**

# COMMAND ----------

data = [{
    "pipeline_name": "shop_table",
    "file_path": "dbfs:/Volumes/workspace/project_pipeline/manual_file_project/shop_mock.csv",
    "header": "true",
    "delimiter": "|",
    "table_name": "project_pipeline.shop_table",
    "schema_detail": {"shop_id": "int", "shop_name": "string", "branch_name": "string", "file_dt": "date"},
    "keys": ["shop_id"],
    "write_mode": "overwrite"
}]

schema = StructType([
    StructField("pipeline_name", StringType(), True),
    StructField("file_path", StringType(), True),
    StructField("header", StringType(), True),
    StructField("delimiter", StringType(), True),
    StructField("table_name", StringType(), True),
    StructField("schema_detail", MapType(StringType(), StringType()), True),
    StructField("keys", ArrayType(StringType()), True),
    StructField("write_mode", StringType(), True)
])

mock_df = spark.createDataFrame(data, schema)
upsert_table(mock_df,"project_pipeline.config_table",["pipeline_name"])

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, MapType, ArrayType

data = [{
    "pipeline_name": "fact_sales",
    "file_path": "dbfs:/Volumes/workspace/project_pipeline/manual_file_project/fact_sales.parquet",
    "header": None,
    "delimiter":None,
    "table_name": "project_pipeline.fact_sales",
    "schema_detail": {"transaction_id": "int","shop_id": "int","sales_qty": "int","sales_amt": "string","sales_date": "date"},
    "keys": ["transaction_id","shop_id"],
    "write_mode": "overwrite"
}]

schema = StructType([
    StructField("pipeline_name", StringType(), True),
    StructField("file_path", StringType(), True),
    StructField("header", StringType(), True),
    StructField("delimiter", StringType(), True),
    StructField("table_name", StringType(), True),
    StructField("schema_detail", MapType(StringType(), StringType()), True),
    StructField("keys", ArrayType(StringType()), True),
    StructField("write_mode", StringType(), True)
])

mock_df = spark.createDataFrame(data, schema)
upsert_table(mock_df,"project_pipeline.config_table",["pipeline_name"])

# COMMAND ----------

# MAGIC %md
# MAGIC