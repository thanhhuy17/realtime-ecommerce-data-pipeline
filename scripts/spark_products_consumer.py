
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, trim
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    BooleanType,
)

# ============================================================
# CONFIGURATION
# ============================================================

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "products"

JDBC_URL = "jdbc:postgresql://localhost:5432/ecommerce"
JDBC_USER = "ecommerce"
JDBC_PASSWORD = "ecommerce_dev_password"

#CHECKPOINT_PATH = "/tmp/ecommerce-products-postgres-checkpoint"
CHECKPOINT_PATH = "/tmp/ecommerce-products-postgres-checkpoint-v2"

# ============================================================
# PRODUCT JSON SCHEMA
# ============================================================

product_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", StringType(), True),
    StructField("is_active", BooleanType(), True),
    StructField("created_at", StringType(), True),
])

# ============================================================
# SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("EcommerceProductsKafkaConsumer")
    .master("local[2]")
    .config("spark.driver.memory", "512m")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ============================================================
# READ STREAM FROM KAFKA
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
# PARSE JSON AND CONVERT DATA TYPES
# ============================================================

products = (
    raw_stream
    .selectExpr("CAST(value AS STRING) AS json_value")
    .select(
        col("json_value"),
        from_json(col("json_value"), product_schema).alias("product")
    )
    .select("json_value", "product.*")
    .withColumn("product_id", col("product_id").cast("long"))
    .withColumn("product_name", trim(col("product_name")))
    .withColumn("category", trim(col("category")))
    .withColumn("price", col("price").cast("decimal(12,2)"))
    .withColumn("created_at", col("created_at").cast("timestamp"))
)

# ============================================================
# WRITE EACH MICRO-BATCH TO POSTGRESQL
# ============================================================

def write_to_postgres(batch_df, batch_id):

    if batch_df.isEmpty():
        print(f"Batch {batch_id}: no data")
        return

    print(f"\n========== BATCH {batch_id} ==========")
    print(f"Received {batch_df.count()} product messages")

    # --------------------------------------------------------
    # VALIDATE DATA
    # --------------------------------------------------------

    invalid_rows = batch_df.filter(
        col("product_id").isNull()
        | (col("product_id") <= 0)
        | col("product_name").isNull()
        | (col("product_name") == "")
        | col("category").isNull()
        | (col("category") == "")
        | col("price").isNull()
        | (col("price") < 0)
        | col("is_active").isNull()
        | col("created_at").isNull()
    )

    if not invalid_rows.isEmpty():

        print(
            f"Batch {batch_id}: invalid product rows detected"
        )

        invalid_rows.select(
            "json_value",
            "product_id",
            "product_name",
            "category",
            "price",
            "is_active",
            "created_at",
        ).show(20, truncate=False, vertical=True)

        # Fail fast: do not silently discard invalid messages.
        raise ValueError(
            f"Batch {batch_id} contains invalid product rows. "
            "Check the JSON output above."
        )

    # --------------------------------------------------------
    # WRITE TO STAGING TABLE
    # --------------------------------------------------------

    staging_df = batch_df.drop("json_value")

    print(
        f"Batch {batch_id}: writing products_staging"
    )

    (
        staging_df.write
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "products_staging")
        .option("user", JDBC_USER)
        .option("password", JDBC_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode("overwrite")
        .save()
    )

    # --------------------------------------------------------
    # UPSERT INTO PRODUCTS
    # --------------------------------------------------------

    connection = None
    statement = None

    upsert_sql = """
        INSERT INTO products (
            product_id,
            product_name,
            category,
            price,
            is_active,
            created_at
        )
        SELECT DISTINCT ON (product_id)
            product_id,
            product_name,
            category,
            price,
            is_active,
            created_at
        FROM products_staging
        ORDER BY product_id, created_at DESC NULLS LAST
        ON CONFLICT (product_id)
        DO UPDATE SET
            product_name = EXCLUDED.product_name,
            category = EXCLUDED.category,
            price = EXCLUDED.price,
            is_active = EXCLUDED.is_active,
            created_at = EXCLUDED.created_at;
    """

    try:
        connection = (
            spark._jvm.java.sql.DriverManager.getConnection(
                JDBC_URL,
                JDBC_USER,
                JDBC_PASSWORD,
            )
        )

        connection.setAutoCommit(False)
        statement = connection.createStatement()

        affected_rows = statement.executeUpdate(upsert_sql)

        statement.execute(
            "DROP TABLE IF EXISTS products_staging"
        )

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
    products.writeStream
    .foreachBatch(write_to_postgres)
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .start()
)

print(
    f"Streaming topic '{TOPIC}' to PostgreSQL. "
    "Press Ctrl+C to stop."
)

query.awaitTermination()
