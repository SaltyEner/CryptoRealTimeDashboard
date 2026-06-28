"""
streaming_job.py — PASSO 3
==========================
Spark Structured Streaming: CONSUMA il topic Kafka `crypto-prices`, pulisce e
aggrega i dati, e li SCRIVE su PostgreSQL.

Flusso:
    Kafka (crypto-prices)  ->  Spark  ->  PostgreSQL
                                   |-- crypto_prices    (ogni tick pulito)
                                   '-- crypto_agg_1min  (media/min/max per 1 min)

Si esegue dentro il container Spark via spark-submit (vedi docker-compose.yml),
con i pacchetti: spark-sql-kafka (connettore Kafka) e il driver JDBC PostgreSQL.
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg, col, count, from_json, max as smax, min as smin, to_date, window,
)
from pyspark.sql.types import (
    DoubleType, LongType, StringType, StructField, StructType,
)

# --- Configurazione (dentro la rete Docker si usano i NOMI dei servizi) ------
KAFKA_BOOTSTRAP = "kafka:29092"          # listener INTERNO del broker
KAFKA_TOPIC = "crypto-prices"

JDBC_URL = "jdbc:postgresql://postgres:5432/crypto"
PG_USER = "pipeline"
PG_PASS = "pipeline"
PG_PROPS = {"user": PG_USER, "password": PG_PASS, "driver": "org.postgresql.Driver"}

# --- Data lake S3/MinIO (letto da env, default = MinIO locale) ---------------
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin")
S3_BUCKET = os.getenv("S3_BUCKET", "crypto-lake")

# --- Schema dei messaggi JSON prodotti dal producer (Passo 2) ----------------
SCHEMA = StructType([
    StructField("symbol", StringType()),
    StructField("price", DoubleType()),
    StructField("source", StringType()),
    StructField("event_time", StringType()),
    StructField("ingest_ts", LongType()),
])


def write_to_postgres(table: str):
    """Ritorna una funzione foreachBatch che scrive il micro-batch su `table`."""
    def _writer(batch_df, batch_id):
        n = batch_df.count()
        if n == 0:
            return  # niente da scrivere in questo batch
        (batch_df.write
            .jdbc(url=JDBC_URL, table=table, mode="append", properties=PG_PROPS))
        print(f"[spark] batch {batch_id}: scritte {n} righe in {table}")
    return _writer


def main() -> None:
    spark = (SparkSession.builder
             .appName("crypto-streaming")
             .config("spark.sql.session.timeZone", "UTC")
             # Single-node con pochi simboli: 4 partizioni bastano (default=200,
             # genererebbe centinaia di file di stato nel checkpoint).
             .config("spark.sql.shuffle.partitions", "4")
             # --- Configurazione S3A per puntare a MinIO (o AWS S3) ---
             .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
             .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
             .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
             # path-style: MinIO usa http://host/bucket (non bucket.host)
             .config("spark.hadoop.fs.s3a.path.style.access", "true")
             .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
             .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
             .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                     "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")  # meno rumore nei log

    # 1) LEGGE lo stream da Kafka -------------------------------------------------
    raw = (spark.readStream
           .format("kafka")
           .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
           .option("subscribe", KAFKA_TOPIC)
           .option("startingOffsets", "earliest")  # leggi anche lo storico già nel topic
           .option("maxOffsetsPerTrigger", 1000)   # batch limitati: niente primo batch enorme
           .load())

    # 2) PARSA il JSON e PULISCE --------------------------------------------------
    #    - value (bytes) -> stringa -> colonne tipizzate
    #    - event_time ricavato da ingest_ts (epoch ms) -> timestamp (robusto)
    #    - scarta righe senza symbol o prezzo
    parsed = (raw
              .selectExpr("CAST(value AS STRING) AS json")
              .select(from_json(col("json"), SCHEMA).alias("d"))
              .select("d.*")
              .withColumn("event_time", (col("ingest_ts") / 1000).cast("timestamp"))
              .filter(col("symbol").isNotNull() & col("price").isNotNull()))

    # 3a) SINK "raw": ogni tick pulito -> tabella crypto_prices -------------------
    raw_query = (parsed
                 .select("symbol", "price", "source", "event_time", "ingest_ts")
                 .writeStream
                 .outputMode("append")
                 .foreachBatch(write_to_postgres("crypto_prices"))
                 .option("checkpointLocation", "/tmp/checkpoint/raw")
                 .trigger(processingTime="10 seconds")
                 .start())

    # 3b) SINK "agg": media/min/max per simbolo ogni 1 minuto --------------------
    #     watermark = aspettiamo dati in ritardo fino a 30s, poi chiudiamo la finestra
    agg = (parsed
           .withWatermark("event_time", "30 seconds")
           .groupBy(window(col("event_time"), "1 minute"), col("symbol"))
           .agg(avg("price").alias("avg_price"),
                smin("price").alias("min_price"),
                smax("price").alias("max_price"),
                count("*").alias("n_ticks"))
           .select(col("symbol"),
                   col("window.start").alias("window_start"),
                   col("window.end").alias("window_end"),
                   "avg_price", "min_price", "max_price", "n_ticks"))

    agg_query = (agg
                 .writeStream
                 .outputMode("append")  # emette ogni finestra quando il watermark la chiude
                 .foreachBatch(write_to_postgres("crypto_agg_1min"))
                 .option("checkpointLocation", "/tmp/checkpoint/agg")
                 .trigger(processingTime="10 seconds")
                 .start())

    # 3c) SINK "data lake": tick grezzi -> Parquet su S3/MinIO ------------------
    #     Partizionato per data e simbolo -> letture future efficienti.
    #     Trigger di 1 minuto per raggruppare i dati e ridurre i file piccoli.
    to_lake = (parsed
               .select("symbol", "price", "source", "event_time", "ingest_ts")
               .withColumn("dt", to_date(col("event_time"))))

    s3_query = (to_lake
                .writeStream
                .format("parquet")
                .option("path", f"s3a://{S3_BUCKET}/crypto_prices/")
                .option("checkpointLocation", "/tmp/checkpoint/s3")
                .partitionBy("dt", "symbol")
                .outputMode("append")
                .trigger(processingTime="1 minute")
                .start())

    print("[spark] Streaming avviato (Postgres + data lake S3). In attesa di micro-batch...")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
