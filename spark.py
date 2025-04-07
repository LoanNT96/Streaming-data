from pyspark.sql import SparkSession, SQLContext
from datetime import datetime
from pyspark import SparkContext

spark = (SparkSession.builder.appName("poc").
         config("spark.sql.execution.arrow.pyspark.enabled", "true").
         config("spark.sql.execution.arrow.pyspark.fallback.enabled", "true").
         config("spark.driver.memory", "60g").
         getOrCreate())
spark.sparkContext.setLogLevel("INFO")


# 0:05:30.328180
def save():
    df2 = spark.read.option("recursiveFileLookup", "true").parquet('PARQUET_FILE')
    df2.write.partitionBy('customer_id').mode('overwrite').parquet('PARQUET_FILE')

def save_join():
    df1 = spark.read.option("recursiveFileLookup", "true").parquet('PARQUET_FILE')
    df2 = spark.read.option("recursiveFileLookup", "true").parquet('PARQUET_FILE')
    df1.limit(100).createOrReplaceTempView('ENTRY')
    df2.limit(100).createOrReplaceTempView('ENTRY')
    result = spark.sql("")
    result = result.fillna(0, subset=['total'])
    result.write.saveAsTable('result')


def read_table(table):
    df = spark.read.option("recursiveFileLookup", "true").parquet(f'spark-warehouse/{table}')
    df.show()


def count():
    df = spark.read.parquet('PARQUET_FILE')
    print(df.count())

if __name__ == '__main__':
    start_time = datetime.now()

    count()

    print('process:', datetime.now() - start_time)
# https://stackoverflow.com/questions/68899346/slow-join-in-pyspark-tried-repartition