from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, trim
from pyspark.sql.types import StructType, StructField, StringType

# ============================================================
# CONFIGURATION
# ============================================================

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "payments"

JDBC_URL = "jdbc:postgresql://localhost:5432/ecommerce"
JDBC_USER = "ecommerce"
JDBC_PASSWORD = "ecommerce_dev_password"

CHECKPOINT_PATH = "/tmp/ecommerce-payments-postgres-checkpoint-v1"

VALID_METHODS = [
    "credit_card",
    "debit_card",
    "e_wallet",
    "bank_transfer",
]

VALID_STATUSES = [
    "pending",
    "success",
    "failed",
    "refunded",
]

# ============================================================
# JSON SCHEMA
# Read values as strings, then cast explicitly
# ============================================================

payment_schema = StructType([
    StructField("payment_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("amount", StringType(), True),
    StructField("status", StringType(), True),
    StructField("payment_date", StringType(), True),
])

# ============================================================
# SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("EcommercePaymentsKafkaConsumer")
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

payments = (
    raw_stream
    .selectExpr("CAST(value AS STRING) AS json_value")
    .select(
        col("json_value"),
        from_json(col("json_value"), payment_schema).alias("payment")
    )
    .select("json_value", "payment.*")
    .withColumn("payment_id", col("payment_id").cast("long"))
    .withColumn("order_id", col("order_id").cast("long"))
    .withColumn("payment_method", trim(col("payment_method")))
    .withColumn("amount", col("amount").cast("decimal(12,2)"))
    .withColumn("status", trim(col("status")))
    .withColumn("payment_date", col("payment_date").cast("timestamp"))
)

# ============================================================
# WRITE EACH MICRO-BATCH TO POSTGRESQL
# ============================================================

def write_to_postgres(batch_df, batch_id):

    if batch_df.isEmpty():
        print(f"Batch {batch_id}: no data")
        return

    print(f"\n========== BATCH {batch_id} ==========")
    print(f"Received {batch_df.count()} payment messages")

    # Validate data before writing
    invalid_rows = batch_df.filter(
        col("payment_id").isNull()
        | (col("payment_id") <= 0)
        | col("order_id").isNull()
        | (col("order_id") <= 0)
        | col("payment_method").isNull()
        | (~col("payment_method").isin(*VALID_METHODS))
        | col("amount").isNull()
        | (col("amount") <= 0)
        | col("status").isNull()
        | (~col("status").isin(*VALID_STATUSES))
        | col("payment_date").isNull()
    )

    if not invalid_rows.isEmpty():
        print(f"Batch {batch_id}: invalid payment rows detected")
        invalid_rows.show(20, truncate=False, vertical=True)
        raise ValueError(
            f"Batch {batch_id} contains invalid payment rows"
        )

    # Write staging table
    staging_df = batch_df.drop("json_value")

    print(f"Batch {batch_id}: writing payments_staging")

    (
        staging_df.write
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "payments_staging")
        .option("user", JDBC_USER)
        .option("password", JDBC_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode("overwrite")
        .save()
    )

    # UPSERT into the target table
    upsert_sql = """
        INSERT INTO payments (
            payment_id,
            order_id,
            payment_method,
            amount,
            status,
            payment_date
        )
        SELECT DISTINCT ON (payment_id)
            payment_id,
            order_id,
            payment_method,
            amount,
            status,
            payment_date
        FROM payments_staging
        ORDER BY payment_id, payment_date DESC NULLS LAST
        ON CONFLICT (payment_id)
        DO UPDATE SET
            order_id = EXCLUDED.order_id,
            payment_method = EXCLUDED.payment_method,
            amount = EXCLUDED.amount,
            status = EXCLUDED.status,
            payment_date = EXCLUDED.payment_date;
    """

    connection = None
    statement = None

    try:
        connection = spark._jvm.java.sql.DriverManager.getConnection(
            JDBC_URL,
            JDBC_USER,
            JDBC_PASSWORD,
        )

        connection.setAutoCommit(False)
        statement = connection.createStatement()

        affected_rows = statement.executeUpdate(upsert_sql)

        statement.execute("DROP TABLE IF EXISTS payments_staging")
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
    payments.writeStream
    .foreachBatch(write_to_postgres)
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .start()
)

query.awaitTermination()
