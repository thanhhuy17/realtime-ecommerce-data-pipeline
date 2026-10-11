
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    TimestampType,
)

import os

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "customers"

customer_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("full_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("city", StringType(), True),
    StructField("created_at", StringType(), True),
])



spark = (
    SparkSession.builder
    .appName("EcommerceKafkaConsumer")
    .master("local[2]")
    .config("spark.driver.memory", "512m")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)



customers = (
    raw_stream
    .selectExpr("CAST(value AS STRING) AS json_value")
    .select(
        from_json(col("json_value"), customer_schema).alias("customer")
    )
    .select("customer.*")
    .withColumn("customer_id", col("customer_id").cast("long"))
    .withColumn("created_at", col("created_at").cast("timestamp"))
)




def write_to_postgres(batch_df, batch_id):
    if batch_df.isEmpty():
        print(f"Batch {batch_id}: no data")
        return

    print(f"Batch {batch_id}: writing to PostgreSQL")

    jdbc_url = "jdbc:postgresql://localhost:5432/ecommerce"

    batch_df.printSchema()
    batch_df.select(
    "customer_id", "created_at"
    ).show(5, truncate=False)

    (
        batch_df.write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "customers")
        .option("user", "ecommerce")
        .option("password", "ecommerce_dev_password")
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )

    print(f"Batch {batch_id}: PostgreSQL write completed")


query = (
    customers.writeStream
    .foreachBatch(write_to_postgres)
    .outputMode("append")
    .option(
        "checkpointLocation",
        "/tmp/ecommerce-customers-postgres-checkpoint"
    )
    .start()
)

query.awaitTermination()
