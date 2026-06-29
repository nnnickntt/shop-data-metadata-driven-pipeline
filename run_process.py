# Databricks notebook source
# MAGIC %md
# MAGIC **Execute Bronze Pipeline**
# MAGIC
# MAGIC - Pass the `pipeline_name` configuration to `bronze_pipeline` (`../pipeline/bronze_pipeline`) via the `dbutils.notebook.run` parameter
# MAGIC - `bronze_pipeline` triggers `bronze_framework` (`../framework/bronze_framework`)

# COMMAND ----------

dbutils.notebook.run('/Workspace/Users/nicknicknuttiwut@gmail.com/2_Nick/End To End Project With Table Config (On Github)/pipeline/bronze_pipeline',0,{'pipeline_name':'shop_table'})
dbutils.notebook.run('/Workspace/Users/nicknicknuttiwut@gmail.com/2_Nick/End To End Project With Table Config (On Github)/pipeline/bronze_pipeline',0,{'pipeline_name':'fact_sales'})

# COMMAND ----------

# MAGIC %md
# MAGIC **Execute Silver Pipeline**
# MAGIC
# MAGIC - Pass the `pipeline_name` configuration to `silver_pipeline` (`../pipeline/silver_pipeline`) via the `dbutils.notebook.run` parameter
# MAGIC - `silver_pipeline` triggers `silver_framework` (`../framework/silver_framework`)

# COMMAND ----------

dbutils.notebook.run('/Workspace/Users/nicknicknuttiwut@gmail.com/2_Nick/End To End Project With Table Config (On Github)/pipeline/silver_pipeline', 0, {'pipeline_name': 'shop_table'})
dbutils.notebook.run('/Workspace/Users/nicknicknuttiwut@gmail.com/2_Nick/End To End Project With Table Config (On Github)/pipeline/silver_pipeline', 0, {'pipeline_name': 'fact_sales'})