from pyspark.sql import SparkSession
import numpy as np
import os
import sys

class df_spark:
    def __init__(self, json_file):
        self.spark = SparkSession.builder.getOrCreate()
        self.df = self.spark.read.json(json_file)

    def create_sql_table(self, tablename):
        self.df.createOrReplaceTempView(tablename)
  
def json2csv(df, dir_name):
    '''
    Spark JSON to CSV converter involving handling struct data type

    Params :
        df       : spark frame
        dir_name : directory_name
    '''
    fined_columns = []
    for column, dtype in df.dtypes:
        col = column+'.*' if 'struct' in dtype else column
        fined_columns.append(col)
    df.coalesce(1).select(fined_columns).write.option('header',True).csv(dir_name)
    
if __name__ == '__main__':
    # testing
    df_spark('datasets/yelp_academic_dataset_business.json')
    json2csv(df_spark.df, 'flatfile/business')
  
    
      
