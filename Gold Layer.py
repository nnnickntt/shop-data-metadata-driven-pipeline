# Databricks notebook source
# MAGIC %run ./config/lib

# COMMAND ----------

# MAGIC %md
# MAGIC **Sales Quantity Analysis by Shop Name**

# COMMAND ----------

fact_sales_df = (
    spark.table("project_pipeline.fact_sales_silver")
    .select('shop_id','sales_qty')
    .filter(col('sales_date') == '2025-11-30')
    )

shop_table_df =(
    spark.table('project_pipeline.shop_table_silver')
    .select('shop_id','shop_name')
)

# COMMAND ----------

final_result_df =(
    fact_sales_df.alias('sales')
    .join(
        shop_table_df.alias('shop'),[col('sales.shop_id')==col('shop.shop_id')],how='left'
        )
    .select('sales.shop_id','sales.sales_qty','shop.shop_name')
    .groupBy('shop.shop_name')
    .agg(
        sum('sales.sales_qty').alias('sales_qty')
        )
    .filter(col('shop_name').isNotNull())
)

# COMMAND ----------

final_result_df.display()