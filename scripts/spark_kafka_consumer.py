from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "customers"

JDBC_URL = "jdbc:postgresql://localhost:5432/ecommerce"
JDBC_USER = "ecommerce"
JDBC_PASSWORD = "ecommerce_dev_password"

CHECKPOINT_PATH = "/tmp/ecommerce-customers-postgres-checkpoint"

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
    .load()
)

customers = (
    raw_stream
    .selectExpr("CAST(value AS STRING) AS json_value")
    .select(from_json(col("json_value"), customer_schema).alias("customer"))
    .select("customer.*")
    .withColumn("customer_id", col("customer_id").cast("long"))
    .withColumn("created_at", col("created_at").cast("timestamp"))
)


def write_to_postgres(batch_df, batch_id):
    if batch_df.isEmpty():
        print(f"Batch {batch_id}: no data")
        return

    print(f"Batch {batch_id}: writing to staging table")

    # Step 1: Write this batch to a staging table.
    # Overwrite applies only to the staging table, never the main table.
    (
        batch_df.write
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "customers_staging")
        .option("user", JDBC_USER)
        .option("password", JDBC_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode("overwrite")
        .save()
    )

    # Step 2: Upsert into the main table in a PostgreSQL transaction.
    connection = None
    statement = None

    upsert_sql = """
        INSERT INTO customers (
            customer_id,
            full_name,
            email,
            city,
            created_at
        )
        SELECT DISTINCT ON (customer_id)
            customer_id,
            full_name,
            email,
            city,
            created_at
        FROM customers_staging
        WHERE customer_id IS NOT NULL
        ORDER BY customer_id, created_at DESC NULLS LAST
        ON CONFLICT (customer_id)
        DO UPDATE SET
            full_name = EXCLUDED.full_name,
            email = EXCLUDED.email,
            city = EXCLUDED.city,
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

        # Drop staging only after the upsert succeeds.
        statement.execute("DROP TABLE IF EXISTS customers_staging")

        connection.commit()

        print(
            f"Batch {batch_id}: upsert completed, "
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
    customers.writeStream
    .foreachBatch(write_to_postgres)
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .start()
)

query.awaitTermination()