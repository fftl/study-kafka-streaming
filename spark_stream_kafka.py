from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, window, avg, sum, count
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# 1. SparkSession 생성 (Kafka 커넥터 패키지 자동 지정)
spark = SparkSession.builder \
    .appName("KafkaStructuredStreaming") \
    .master("local[2]") \
    .config("spark.driver.memory", "2g") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. JSON 스키마 정의 (앞서 producer_key.py에서 보낸 payload 구조)
schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("timestamp", DoubleType(), True),
])

# 3. Kafka로부터 스트림 데이터 수신 (readStream)
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "order-events") \
    .option("startingOffsets", "latest") \
    .load()

# 4. 데이터 역직렬화 및 파싱
# Kafka의 key와 value는 바이너리(binary)이므로 문자열 캐스팅 후 JSON 스키마 적용
parsed_df = kafka_stream.select(
    col("key").cast("string").alias("key_user_id"),
    from_json(col("value").cast("string"), schema).alias("data")
).select(
    "key_user_id",
    "data.order_id",
    "data.symbol",
    "data.amount",
    # Unix timestamp(초)를 Spark Timestamp 객체로 변환
    to_timestamp(col("data.timestamp")).alias("event_time")
)

# 5. 윈도우 집계 (30초 텀블링 윈도우)
# 심볼(BTC, ETH 등)별로 30초 단위 거래 건수 및 총 수량 집계
windowed_summary = parsed_df \
    .groupBy(
        window(col("event_time"), "30 seconds"),
        col("symbol")
    ).agg(
        count("order_id").alias("order_count"),
        sum("amount").alias("total_amount")
    )

# 6. 콘솔 싱크(Sink) 출력
# update 모드: 변경되거나 새로 계산된 윈도우 행만 출력
query = windowed_summary.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime="5 seconds") \
    .start()

print("[SPARK] Kafka 스트리밍 쿼리 가동 중 ('order-events' 수신 대기)...")
query.awaitTermination()