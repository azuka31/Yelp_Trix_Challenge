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

def df2sqltext(df, name, mode='a', filename='yelp_createtable.sql'):
    '''
    Genereating dataframe to SQL language based on its columns and the datatypes

    Params
    ------
        df       : spark frame
        name     : table name for SQL language
    '''
	fined_columns = []
	for column, dtype in df.dtypes:
        col = column+'.*' if 'struct' in dtype else column
        fined_columns.append(col)
	col_dtype = df.select(fined_columns).dtypes
	# create SQL text
	with open(filename,mode) as f:
        f.write('create table df_{}(\n'.format(name))
        tmp = []
        for col, dtype in col_dtype:
            col = col.lower()
            dtype = dtype.replace('string','varchar').replace('double','float8')
            tmp.append('\t{} {}'.format(col, dtype))
        f.write(',\n'.join(tmp))
        f.write('\n);\n\n')
    
if __name__ == '__main__':
    # testing
    df_spark('datasets/yelp_academic_dataset_business.json')
    # json2csv(df_spark.df, 'flatfile/business')
    os.system('touch yelp_createtable.sql')
    df2sqltext(df_spark.df, 'business')
    
    query='''select
                business_id,    
                name,
                stars,
                case
                    when stars <= 2 then 'not recommended'
                    when stars <= 3 then 'average'
                    when stars <= 4 then 'recommended'
                    when stars <= 5 then 'very recommended'
                    else 'unknown'
                end as conclusion
            from business'''
    
    spark = SparkSession.builder.getOrCreate()
    business_summary = spark.sql(query)
    business_summary.show()
    
  
    
      
