# Databricks notebook source
# MAGIC %run ../config/lib

# COMMAND ----------

def get_reason(df:DataFrame)->DataFrame:

    ids_columns = [column for column in df.columns if not column.startswith('_') or column == '_sk']
    values_columns = [column for column in df.columns if column.startswith('_') and column != '_sk']
    filter_columns = ' or '.join(values_columns)

    return (
        df.filter(filter_columns)
        .melt(
            ids=ids_columns,
            values=values_columns,
            variableColumnName='reason',
            valueColumnName='status'
        )
        .filter(col('status')==True)
        .groupBy(
            *ids_columns
        ).agg(
            collect_list('reason').alias('reason')
        )
        .orderBy('_sk')
    )


# COMMAND ----------

@dataclass
class SilverLayer:
    pipeline_name:str
    table_name:str
    schema_detail:dict[str,str]
    keys:list[str]
    write_mode:str

    def __post_init__(self):
        print(f'Starting Pipeline {self.pipeline_name} Silver Layer')
        self.bronze_table_name = f'{self.table_name}_bronze'
        self.silver_table_name =  f'{self.table_name}_silver'
        self.bad_table_name = f'{self.table_name}_bad'
        self.data_columns = list(self.schema_detail.keys())
    
    def read_from_bronze_sk(self):
        return (
            spark.table(self.bronze_table_name)
            .select(
                monotonically_increasing_id().alias('_sk'),
                *self.data_columns,
            )
        )
    
    def get_invalid_record(self,bronze_df:DataFrame)->DataFrame:
        invalid_rule = {
            'int' : '^[0-9]+$',
            'date' : '\\d{4}-\\d{2}-\\d{2}$'
        }
        invalid_column ={
            f'_is_{column_name}_invalid':coalesce(~col(column_name).rlike(invalid_rule[column_type]),lit(False)) for column_name,column_type in self.schema_detail.items() if column_type != 'string'
        }
        return (
            bronze_df.withColumns(invalid_column)
            .transform(get_reason)
        )

    def get_null_record (self,bronze_df:DataFrame)->DataFrame:
        null_column ={
            f'_is_{column}_null' : col(column).isNull() for column in self.keys
        }
        return (
            bronze_df.withColumns(null_column)
            .transform(get_reason)
        )

    def get_duplicate_record(self,bronze_df:DataFrame,null_df:DataFrame)->DataFrame:
        
        row_dup_partition = Window.partitionBy(*self.data_columns).orderBy('_sk')
        key_dup_partition = Window.partitionBy(*self.keys)
        bronze_df_not_null =(
            bronze_df.join(self.get_null_record(bronze_df=bronze_df),['_sk'],how='left_anti')
        )

        row_dup_df = (
            bronze_df_not_null.withColumn(
                'rn',
                row_number().over(row_dup_partition)
            )
            .filter(col('rn')>1)
            .drop('rn')
            .withColumn('reason',array(lit('_is_row_dup')))
        )
        key_dup_df = (
            bronze_df_not_null.join(row_dup_df,['_sk'],how='left_anti')
            .withColumn(
                'rn',
                count('*').over(key_dup_partition)
            )
            .filter(col('rn')>1)
            .drop('rn')
            .withColumn('reason',array(lit('_is_key_dup')))
        )

        return (
            row_dup_df.unionByName(key_dup_df)
        )

    def get_all_bad_record(self,invalid_df:DataFrame,null_df:DataFrame,dup_df:DataFrame)->DataFrame:
        return (
            invalid_df
            .unionByName(null_df)
            .unionByName(dup_df)
            .groupBy('_sk',*self.data_columns)
            .agg(
                flatten(collect_list('reason')).alias('reason')
            )
        )

    def get_all_good_record(self,bronze_df:DataFrame,bad_df:DataFrame)->DataFrame:
        cast_column =[col(column_name).cast(column_type) for column_name,column_type in self.schema_detail.items()]
        control_column ={
            'load_dt' : current_date(),
            'load_dttm' : current_timestamp()
        }
        return (
            bronze_df.join(bad_df,['_sk'],how='left_anti')
            .select(*cast_column)
            .withColumns(control_column)
        )

    def load_bad_record(self,bad_df:DataFrame):
        (
            bad_df
            .write
            .mode(self.write_mode)
            .saveAsTable(self.bad_table_name)
        )
        print(f'Bad Records Loaded to {self.bad_table_name} success')

    def load_good_record(self,good_df:DataFrame):
        (
            good_df
            .write
            .mode(self.write_mode)
            .saveAsTable(self.silver_table_name)
        )
        print(f'Good Records Loaded to {self.silver_table_name} success')
