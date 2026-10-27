from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, window, current_timestamp

# 1. SparkSession 초기화
spark = SparkSession.builder \
    .appName("SocketStreamingWordCount") \
    .master("local[2]") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. 소켓 스트림 데이터 소스 정의 (localhost:9999 포트 수신)
# read 대신 readStream을 사용합니다.
lines = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# 3. 데이터 변환 (Transformation)
# 들어온 텍스트를 공백 기준으로 쪼개고, 각 단어에 현재 타임스탬프를 부여
words = lines.select(
    explode(split(lines.value, " ")).alias("word"),
    current_timestamp().alias("timestamp")
)

# 4. 10초 단위 고정 윈도우(Tumbling Window) 집계
# 10초 간격으로 유입된 단어별 빈도수를 카운트
windowed_counts = words.groupBy(
    window(words.timestamp, "10 seconds"),
    words.word
).count().orderBy("window")

# 5. 스트리밍 쿼리 실행 (Action - writeStream)
# outputMode("complete"): 매 배치마다 누적된 전체 집계 테이블을 갱신 출력
# trigger(processingTime="5 seconds"): 5초마다 마이크로배치를 트리거
query = windowed_counts.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime="5 seconds") \
    .start()

print("[STREAMING] 소켓 스트리밍 시작됨 (포트: 9999 대기 중)...")
query.awaitTermination()