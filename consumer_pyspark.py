from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro

spark = SparkSession.builder.appName('realtime').config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-avro_2.12:3.5.1").getOrCreate()
# spark.sparkContext.setLogLevel('INFO')

df = (spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "SERVER")
      .option("kafka.sasl.mechanism", "SCRAM-SHA-256")
      .option("kafka.security.protocol", "SASL_SSL")
      .option("kafka.sasl.jaas.config",
              "CONFIG")
      .option("startingOffsets", "earliest")
      .option("subscribe", "SUBSCRIBE")
      .load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)"))
# df = spark.readStream.format("kafka").options(**options).load()

df.createOrReplaceTempView('SUBSCRIBE')


def process():
    print("--------------- read_message ------------")
    output = spark.sql("""
        select * from DATA_SOURCE
    """)
    # output.show()
    query = output.writeStream.outputMode('append').format('console').start()
    query.awaitTermination()


if __name__ == '__main__':
    process()