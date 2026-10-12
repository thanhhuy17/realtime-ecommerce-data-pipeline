from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, trim
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
)

# ============================================================
# CONFIGURATION
# ============================================================

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "orders"

JDBC_URL = "jdbc:postgresql://localhost:5432/ecommerce"
JDBC_USER = "ecommerce"
JDBC_PASSWORD = "ecommerce_dev_password"

CHECKPOINT_PATH = "/tmp/ecommerce-orders-postgres-checkpoint-v1"

# ============================================================
# ORDER JSON SCHEMA
# Read values as strings first, then cast explicitly.
# ============================================================

order_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("status", StringType(), True),
    StructField("total_amount", StringType(), True),
])

VALID_STATUSES = [
    "pending",
    "confirmed",
    "shipped",
    "delivered",
    "cancelled",
]

# ============================================================
# SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("EcommerceOrdersKafkaConsumer")
    .master("local[2]")
    .config("spark.driver.memory", "512m")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ============================================================
# READ KAFKA
# ============================================================

raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

# ============================================================
# PARSE JSON AND CAST TYPES
# ============================================================

orders = (
    raw_stream
    .selectExpr("CAST(value AS STRING) AS json_value")
    .select(
        col("json_value"),
        from_json(col("json_value"), order_schema).alias("order")
    )
    .select("json_value", "order.*")
    .withColumn("order_id", col("order_id").cast("long"))
    .withColumn("customer_id", col("customer_id").cast("long"))
    .withColumn("order_date", col("order_date").cast("timestamp"))
    .withColumn("status", trim(col("status")))
    .withColumn("total_amount", col("total_amount").cast("decimal(12,2)"))
)

# ============================================================
# WRITE EACH MICRO-BATCH TO POSTGRESQL
# ============================================================

def write_to_postgres(batch_df, batch_id):

    if batch_df.isEmpty():
        print(f"Batch {batch_id}: no data")
        return

    print(f"\n========== BATCH {batch_id} ==========")
    print(f"Received {batch_df.count()} order messages")

    # --------------------------------------------------------
    # VALIDATE
    # --------------------------------------------------------

    invalid_rows = batch_df.filter(
        col("order_id").isNull()
        | (col("order_id") <= 0)
        | col("customer_id").isNull()
        | (col("customer_id") <= 0)
        | col("order_date").isNull()
        | col("status").isNull()
        | (~col("status").isin(*VALID_STATUSES))
        | col("total_amount").isNull()
        | (col("total_amount") < 0)
    )

    if not invalid_rows.isEmpty():
        print(f"Batch {batch_id}: invalid order rows detected")

        invalid_rows.select(
            "json_value",
            "order_id",
            "customer_id",
            "order_date",
            "status",
            "total_amount",
        ).show(20, truncate=False, vertical=True)

        raise ValueError(
            f"Batch {batch_id} contains invalid order rows. "
            "Check the JSON output above."
        )

    # --------------------------------------------------------
    # WRITE STAGING TABLE
    # --------------------------------------------------------

    staging_df = batch_df.drop("json_value")

    print(f"Batch {batch_id}: writing orders_staging")

    (
        staging_df.write
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "orders_staging")
        .option("user", JDBC_USER)
        .option("password", JDBC_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode("overwrite")
        .save()
    )

    # --------------------------------------------------------
    # UPSERT INTO ORDERS
    # --------------------------------------------------------

    connection = None
    statement = None

    upsert_sql = """
        INSERT INTO orders (
            order_id,
            customer_id,
            order_date,
            status,
            total_amount
        )
        SELECT DISTINCT ON (order_id)
            order_id,
            customer_id,
            order_date,
            status,
            total_amount
        FROM orders_staging
        ORDER BY order_id, order_date DESC NULLS LAST
        ON CONFLICT (order_id)
        DO UPDATE SET
            customer_id = EXCLUDED.customer_id,
            order_date = EXCLUDED.order_date,
            status = EXCLUDED.status,
            total_amount = EXCLUDED.total_amount;
    """

    try:
        connection = spark._jvm.java.sql.DriverManager.getConnection(
            JDBC_URL,
            JDBC_USER,
            JDBC_PASSWORD,
        )

        connection.setAutoCommit(False)
        statement = connection.createStatement()

        affected_rows = statement.executeUpdate(upsert_sql)

        statement.execute("DROP TABLE IF EXISTS orders_staging")

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


# ============================================================
# START STREAMING QUERY
# ============================================================

query = (
    orders.writeStream
    .foreachBatch(write_to_postgres)
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .start()
)

query.awaitTermination()
