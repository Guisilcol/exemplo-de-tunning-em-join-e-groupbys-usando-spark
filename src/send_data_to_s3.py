"""
    Prepare and send the data created in create_data.py to S3. Some transformations are applied to the data before it is sent to S3. 
"""

#%%

import os
import warnings

from awsglue.context import GlueContext
import dotenv
from pyspark.context import SparkContext, SparkConf
from pyspark.sql.session import DataFrame
import pyspark.sql.functions as F
import pyspark.sql.types as T

warnings.filterwarnings('ignore')
dotenv.load_dotenv()


SPARK_CONF = (
    SparkConf()
    .set("spark.driver.memory", "6g")
    .set("spark.dynamicAllocation.enabled", "true")
    .set("spark.executor.cores", 4)
    .set("spark.dynamicAllocation.minExecutors","2")
    .set("spark.dynamicAllocation.maxExecutors","5")
)

SPARK_CONTEXT = SparkContext.getOrCreate(SPARK_CONF)
GLUE_CONTEXT = GlueContext(SPARK_CONTEXT)
SPARK = GLUE_CONTEXT.spark_session

MEASUREMENTS_FILEPATH               : str =   os.getenv('MEASUREMENTS_FILEPATH')
CITY_WEATHERS_TABLE_NAME            : str =   os.getenv('CITY_WEATHERS_TABLE_NAME')
CITY_WEATHERS_BUCKETED_TABLE_NAME   : str =   os.getenv('CITY_WEATHERS_BUCKETED_TABLE_NAME')
CITY_TAGS_TABLE_NAME                : str =   os.getenv('CITY_TAGS_TABLE_NAME')
CITY_TAGS_BUCKTED_TABLE_NAME        : str =   os.getenv('CITY_TAGS_BUCKTED_TABLE_NAME')
WORLD_CITIES_FILEPATH               : str =   os.getenv('WORLD_CITIES_FILEPATH')
WORLD_CITIES_TABLE_NAME             : str =   os.getenv('WORLD_CITIES_TABLE_NAME')
S3_PATH                             : str =   os.getenv('S3_PATH')

# %%
def compute_city_weathers_with_id():
    df = SPARK.read.csv(MEASUREMENTS_FILEPATH, header=False, sep=';', schema='city STRING, temperature DOUBLE')
    df = df.withColumn('id', F.monotonically_increasing_id())
    return df

def compute_city_tags(df_city_weathers: DataFrame):
    
    df = df_city_weathers.select(
        F.col('id').alias('city_weather_id'),
    )
    
    df = (
        df
        .withColumn('tag_quantity', F.round(F.rand(seed=23)*2, 0))
        .withColumn(
            'tag_quantity', 
            F.when(F.col('tag_quantity') == F.lit(0), F.lit(1))
            .otherwise(F.col('tag_quantity')))
        .withColumn('tag_quantity', F.col('tag_quantity').cast(T.IntegerType()))
    )
    
    df = (
        df 
        .withColumn("tags", F.sequence(F.lit(1), F.col("tag_quantity"))) 
        .withColumn("tag", F.explode(F.col("tags"))) 
        .drop('tags', 'tag_quantity')
    ) 
            
    df = (
        df
        .withColumn('tag', 
                    F.when(F.col('tag') == F.lit(1), F.lit('TAG A'))
                    .otherwise(F.lit('TAG B')))
    )
    
    return df

def compute_world_cities():
    return (SPARK
                .read
                .csv(
                    WORLD_CITIES_FILEPATH, 
                    header=True, 
                    sep=',', 
                    schema='name STRING, country STRING, subcountry STRING'))

def write_city_weather(df: DataFrame):
    (df 
        .write 
        .option('path', f'{S3_PATH}/{CITY_WEATHERS_TABLE_NAME}') 
        .saveAsTable(CITY_WEATHERS_TABLE_NAME, 'parquet', 'overwrite'))
    
def write_city_tags(df: DataFrame):
    (df
        .write
        .option('path', f'{S3_PATH}/{CITY_TAGS_TABLE_NAME}')
        .saveAsTable(CITY_TAGS_TABLE_NAME, 'parquet', 'overwrite'))
    
def write_city_weather_bucketed(df: DataFrame):
    (df
        .write
        .option('path', f'{S3_PATH}/{CITY_WEATHERS_BUCKETED_TABLE_NAME}')
        .bucketBy(12, 'id')
        .sortBy('id')
        .saveAsTable(CITY_WEATHERS_BUCKETED_TABLE_NAME, 'parquet', 'overwrite'))

def write_city_tags_bucketed(df: DataFrame):
    (df
        .write
        .option('path', f'{S3_PATH}/{CITY_TAGS_BUCKTED_TABLE_NAME}')
        .bucketBy(12, 'city_weather_id')
        .sortBy('city_weather_id')
        .saveAsTable(CITY_TAGS_BUCKTED_TABLE_NAME, 'parquet', 'overwrite'))

def write_world_cities(df: DataFrame):
    (df
        .write
        .option('path', f'{S3_PATH}/{WORLD_CITIES_TABLE_NAME}')
        .saveAsTable(WORLD_CITIES_TABLE_NAME, 'parquet', 'overwrite'))

#%% 

if __name__ == '__main__':
    df_city_weather = compute_city_weathers_with_id()
    df_city_tags = compute_city_tags(df_city_weather)
    df_world_cities = compute_world_cities()
    
    print('Writing world cities')
    write_world_cities(df_world_cities)
    
    print('Writing city_weather')
    write_city_weather(df_city_weather)
    
    print('Writing city_weather bucketed')
    write_city_weather_bucketed(df_city_weather)
    
    df_city_weather.write.csv('s3://bucket-name/city_weather.csv', header=True, sep=';')
    
    print('Writing city tags')
    write_city_tags(df_city_tags)
    
    print('Writing city tags bucketed')
    write_city_tags_bucketed(df_city_tags)
    
    
 