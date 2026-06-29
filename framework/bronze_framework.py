# Databricks notebook source
# MAGIC %run ../config/ddl

# COMMAND ----------

# MAGIC %run ../config/lib

# COMMAND ----------

@dataclass
class BronzeLayer:
    pipeline_name : str
    header :bool
    delimiter : str
    file_path : str
    table_name : str
    write_mode : str

    def __post_init__(self):
        print(f'Starting Pipeline {self.pipeline_name} Bronze Layer')
        self.format_file = self.file_path.split('.')[-1]
        self.bronze_table_name = f'{self.table_name}_bronze'
    
    def read_raw_data(self)->DataFrame:
        return (
            spark.read
            .format(self.format_file)
            .option('header', self.header)
            .option('delimiter', self.delimiter)
            .load(self.file_path)
            .withColumn('_load_dt',current_date())
            .withColumn('_load_dttm',current_timestamp())
            .withColumn('_file_name',col('_metadata.file_name'))
            .withColumn('_file_path',col('_metadata.file_path'))
            .withColumn('_file_size',col('_metadata.file_size'))
            .withColumn("_file_mod",col("_metadata.file_modification_time"))
        )
    
    def save_bronze(self, df:DataFrame):
        (
            df.write
            .mode(self.write_mode)
            .saveAsTable(self.bronze_table_name)
        )
        print(f'Save Raw Data to {self.bronze_table_name} success')

