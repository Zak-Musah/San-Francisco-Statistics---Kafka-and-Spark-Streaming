import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf

RADIO_CODE_JSON_FILEPATH = "radio_code.json"
TOPIC = "crime_stats"

schema = StructType([
    StructField("crime_id", StringType(), False),
    StructField("original_crime_type_name", StringType(), True),
    StructField("report_date", TimestampType(), True),
    StructField("call_date", TimestampType(), True),
    StructField("offense_date", TimestampType(), True),
    StructField("call_time", StringType(), True),
    StructField("call_date_time", TimestampType(), True),
    StructField("disposition", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("agency_id", StringType(), True),
    StructField("address_type", StringType(), True),
    StructField("common_location", StringType(), True),
])

disposition_schema = StructType([
    StructField("disposition_code", StringType(), True),
    StructField("description", StringType(), True)
])


def run_spark_job(spark):

    spark.sparkContext.setLogLevel("INFO")

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", TOPIC) \
        .option("stopGracefullyOnShutdown", "true") \
        .option("maxOffsetPerTrigger", "200") \
        .option("startingOffsets", "earliest") \
        .load()

    df.printSchema()

    kafka_df = df.selectExpr("CAST(value AS STRING)")

    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("DF"))\
        .select("DF.*")

    distinct_table = service_table \
        .select(
            psf.col("call_date_time"),
            psf.col("original_crime_type_name"),
            psf.col("disposition")
        ) \
        .distinct()

    agg_df = distinct_table \
        .withWatermark("call_date_time", "1 hour") \
        .groupBy(
            psf.window(psf.col("call_date_time"), "1 hour", "10 minutes"),
            psf.col("original_crime_type_name"),
            psf.col("disposition")
        ) \
        .agg({"original_crime_type_name": "count"}) \
        .orderBy("count(original_crime_type_name)", ascending=False)

    query = agg_df \
        .writeStream \
        .trigger(processingTime="10 seconds") \
        .format('console') \
        .outputMode('Complete') \
        .option("truncate", "false") \
        .queryName("Count of crime type") \
        .start()

    print(query.lastProgress)
    print(query.status)

    radio_code_df = spark.read.option("multiline", "true").schema(
        disposition_schema).json(RADIO_CODE_JSON_FILEPATH)

    radio_code_df = radio_code_df \
        .withColumnRenamed("disposition_code", "disposition")

    join_df = agg_df.join(other=radio_code_df, on="disposition", how="left")

    join_query = join_df \
        .writeStream \
        .trigger(processingTime="12 seconds") \
        .format("console") \
        .outputMode("complete") \
        .option("truncate", "false") \
        .queryName("Count of crime type with disposition over time") \
        .start()

    print(join_query.lastProgress)
    print(join_query.status)

    join_query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    spark = SparkSession \
        .builder \
        .master("local") \
        .appName("KafkaSparkStructuredStreaming") \
        .getOrCreate()

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
