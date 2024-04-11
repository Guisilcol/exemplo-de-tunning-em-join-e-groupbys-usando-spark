# %% 
from awsglue.context import GlueContext
import dotenv
from pyspark.context import SparkContext, SparkConf
import pyspark.sql.functions as F

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


# %% 

df = SPARK.read.table('world_cities')

df.explain()

# %% 


df2 = SPARK.read.table('city_weathers_bucketed')

df1 = SPARK.read.table('city_weathers')
df1 = (
    df1
    .groupBy('id')
        .agg(
            F.max('temperature').alias('temperature'),
        )
)

df2 = (
    df2
    .groupBy('id')
        .agg(
            F.max('temperature').alias('temperature'),
        )
)


df1.explain()
df2.explain()

#%% 

df1.show()