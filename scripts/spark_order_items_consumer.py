from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType

# Configuration
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "order_items"

JDBC_URL = "jdbc:postgresql://localhost:5432/ecommerce"
JDBC_USER = "ecommerce"
JDBC_PASSWORD = "ecommerce_dev_password"

CHECKPOINT_PATH = "/tmp/ecommerce-order-items-postgres-checkpoint-v1"

# Read JSON fields as strings first, then cast explicitly
order_item_schema = StructType([
    StructField("order_item_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", StringType(), True),
    StructField("unit_price", StringType(), True),
])

# Spark session
spark = (
    SparkSession.builder
    .appName("EcommerceOrderItemsKafkaConsumer")
    .master("local[2]")
    .config("spark.driver.memory", "512m")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Read Kafka stream
raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

# Parse and cast
order_items = (
    raw_stream
    .selectExpr("CAST(value AS STRING) AS json_value")
    .select(
        col("json_value"),
        from_json(col("json_value"), order_item_schema).alias("item")
    )
    .select("json_value", "item.*")
    .withColumn("order_item_id", col("order_item_id").cast("long"))
    .withColumn("order_id", col("order_id").cast("long"))
    .withColumn("product_id", col("product_id").cast("long"))
    .withColumn("quantity", col("quantity").cast("integer"))
    .withColumn("unit_price", col("unit_price").cast("decimal(12,2)"))
)


def write_to_postgres(batch_df, batch_id):
    if batch_df.isEmpty():
        print(f"Batch {batch_id}: no data")
        return

    print(f"\n========== BATCH {batch_id} ==========")
    print(f"Received {batch_df.count()} order-item messages")

    # Validate data before writing
    invalid_rows = batch_df.filter(
        col("order_item_id").isNull()
        | (col("order_item_id") <= 0)
        | col("order_id").isNull()
        | (col("order_id") <= 0)
        | col("product_id").isNull()
        | (col("product_id") <= 0)
        | col("quantity").isNull()
        | (col("quantity") <= 0)
        | col("unit_price").isNull()
        | (col("unit_price") < 0)
    )

    if not invalid_rows.isEmpty():
        print(f"Batch {batch_id}: invalid rows detected")
        invalid_rows.show(20, truncate=False, vertical=True)
        raise ValueError(f"Batch {batch_id} contains invalid order-item rows")

    staging_df = batch_df.drop("json_value")

    print(f"Batch {batch_id}: writing order_items_staging")

    (
        staging_df.write
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "order_items_staging")
        .option("user", JDBC_USER)
        .option("password", JDBC_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode("overwrite")
        .save()
    )

    upsert_sql = """
        INSERT INTO order_items (
            order_item_id,
            order_id,
            product_id,
            quantity,
            unit_price
        )
        SELECT DISTINCT ON (order_item_id)
            order_item_id,
            order_id,
            product_id,
            quantity,
            unit_price
        FROM order_items_staging
        ORDER BY order_item_id
        ON CONFLICT (order_item_id)
        DO UPDATE SET
            order_id = EXCLUDED.order_id,
            product_id = EXCLUDED.product_id,
            quantity = EXCLUDED.quantity,
            unit_price = EXCLUDED.unit_price;
    """

    connection = None
    statement = None

    try:
        connection = spark._jvm.java.sql.DriverManager.getConnection(
            JDBC_URL, JDBC_USER, JDBC_PASSWORD
        )
        connection.setAutoCommit(False)
        statement = connection.createStatement()

        affected_rows = statement.executeUpdate(upsert_sql)
        statement.execute("DROP TABLE IF EXISTS order_items_staging")
        connection.commit()

        print(
            f"Batch {batch_id}: UPSERT completed; "
            f"{affected_rows} rows affected"
        )

    except Exception:
        if connection is not None:
            connection.rollback()
        raise

    finally:
        if statement is not None:
            statement.close()
        if connection is not None:
            connection.close()


query = (
    order_items.writeStream
    .foreachBatch(write_to_postgres)
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .start()
)

query.awaitTermination()
