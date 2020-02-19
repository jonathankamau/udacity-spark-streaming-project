import logging
import json
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, from_json


schema = StructType([
    StructField("crime_id", StringType(), True),
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
    StructField("common_location", StringType(), True)
])
radio_schema = StructType([
        StructField("disposition_code", StringType(), True),
        StructField("description", StringType(), True)
    ])


def run_spark_job(spark):

    spark.sparkContext.setLogLevel("WARN")

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "org.spark.streaming") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 200) \
        .option("maxRatePerPartition", 10) \
        .option("stopGracefullyOnShutdown", "true") \
        .load()

    df.printSchema()

    kafka_df = df.selectExpr("CAST(value AS STRING)")

    service_table = kafka_df.select(
        from_json("value", schema).alias('data')).select('data.*')

    distinct_table = service_table \
        .select("original_crime_type_name", "disposition", "call_date_time") \
        .withWatermark("call_date_time", '20 minutes')

    agg_df = distinct_table.groupBy(
        "original_crime_type_name").count().sort("count", ascending=False)

    query = agg_df \
        .writeStream \
        .trigger(processingTime="10 seconds") \
        .outputMode("Complete") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    query.awaitTermination()

    radio_code_json_filepath = f"{Path(__file__).parents[0]}/radio_code.json"
    radio_code_df = spark.read.json(
        radio_code_json_filepath, schema=radio_schema)

    radio_code_df = radio_code_df.withColumnRenamed(
        "disposition_code", "disposition")

    join_df = distinct_table.join(radio_code_df, "disposition", "left")

    join_query = join_df \
        .writeStream \
        .trigger(processingTime="10 seconds") \
        .outputMode("Update") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    join_query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("KafkaSparkStructuredStreaming") \
        .config('spark.ui.port', '3000') \
        .getOrCreate()

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
