#%%

from timeit import default_timer as timer

import os

from awsglue.context import GlueContext
import dotenv
from pyspark.context import SparkContext, SparkConf
from pyspark.sql.session import DataFrame

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

CITY_WEATHERS_TABLE_NAME            : str =   os.getenv('CITY_WEATHERS_TABLE_NAME')
WORLD_CITIES_TABLE_NAME             : str =   os.getenv('WORLD_CITIES_TABLE_NAME')
OUTPUT_TABLE_NAME                   : str =   'shuffle_hash_small_join_table'
JOIN_HINT                           : str =   'shuffle_hash'
S3_PATH                             : str =   os.getenv('S3_PATH')

SPARK_CONTEXT.setLogLevel('WARN')

class Timer: 
    START: float
    END: float 
    TIME: float
    
    
    def start():
        Timer.START = timer()
        
    def stop():
        Timer.END = timer()
        Timer.TIME = Timer.END - Timer.START
        Timer.START = 0


def compute_city_weathers():
    return SPARK.read.table(CITY_WEATHERS_TABLE_NAME)
    
def compute_world_cities():
    return SPARK.read.table(WORLD_CITIES_TABLE_NAME)

    
def compute_join_small_table_join(df_city_weathers: DataFrame, df_world_cities: DataFrame):
    df_world_cities = df_world_cities.hint(JOIN_HINT)
    df = df_city_weathers.join(df_world_cities, df_city_weathers['city'] == df_world_cities['name'], 'left')
    
    return df

def write_parquet(df: DataFrame):
    df.write.option('path', f'{S3_PATH}/{OUTPUT_TABLE_NAME}').saveAsTable(OUTPUT_TABLE_NAME, format='parquet', mode='overwrite')


def write_statistics():
    with open(os.getenv('STATISTICS_FILEPATH'), 'a') as f:
        content = f'{Timer.TIME};{OUTPUT_TABLE_NAME};{JOIN_HINT}\n'
        f.write(content)
        
if __name__ == '__main__':
    Timer.start()
    
    df_city_weathers = compute_city_weathers()
    df_world_cities = compute_world_cities()
    df = compute_join_small_table_join(df_city_weathers, df_world_cities)
    
    df.explain()
    
    write_parquet(df)
    
    Timer.stop()
    
    write_statistics()