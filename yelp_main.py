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

    Args:
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

    Args:
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

def prep_json(dir_name):
    '''
    Data preparation to read JSON files based on directory and keyword generator.

    Args:
        dir_name     : where JSON files are stored
	Returns:
		file_paths	: path of json files
		file_keys 	: keyword that distinguish every files
    '''
    files = os.listdir(dir_name)
    file_paths = ['{}/{}'.format(dir_name, file) for file in files]
    file_keys = [key.replace('yelp_academic_dataset_','').replace('.json','') for key in files]
    return file_paths, file_keys


if __name__ == '__main__':
    file_paths, file_keys = prep_json('datasets')

    df_sparks = file_keys
    df_sparks = list(map(df_spark, file_paths))
    os.system('touch yelp_createtable.sql')
    for df_spark, name in zip(df_sparks, file_keys):
        json2csv(df_spark.df, 'flatfile/{}'.format(name))
        df2sqltext(df_spark.df, name)
        df_spark.create_sql_table(name)

    # business summary
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
    json2csv(business_summary, 'flatfile/business_summary')
    df2sqltext(business_summary, 'business_summary')
