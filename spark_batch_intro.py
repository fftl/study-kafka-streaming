from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum, count

# 1. SparkSession 생성 (로컬 모드, 2코어 할당)
spark = SparkSession.builder \
    .appName("BatchIntro") \
    .master("local[2]") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

# 로그 레벨 조정 (불필요한 INFO 제거)
spark.sparkContext.setLogLevel("WARN")

# 2. 샘플 가상 주문 데이터 생성 (앞서 Kafka로 쐈던 형태와 동일)
data = [
    ("ord_0001", "user_A", "BTC", 1.5, 95000000),
    ("ord_0002", "user_B", "ETH", 10.0, 3500000),
    ("ord_0003", "user_A", "BTC", 0.5, 96000000),
    ("ord_0004", "user_C", "XRP", 2000.0, 750),
    ("ord_0005", "user_B", "BTC", 0.8, 95500000),
    ("ord_0006", "user_C", "ETH", 5.0, 3520000),
]
columns = ["order_id", "user_id", "symbol", "amount", "price"]

df = spark.createDataFrame(data, columns)

# 3. 데이터 변환 (Transformation - 아직 실제 연산 실행 안 됨)
summary_df = df.groupBy("symbol").agg(
    count("order_id").alias("order_count"),
    sum("amount").alias("total_amount"),
    avg("price").alias("avg_price")
)

# 4. 물리적 실행 계획(Physical Plan) 확인 -> 분산 셔플(HashAggregate) 동작 확인
print("=== 실행 계획 (Catalyst Optimizer) ===")
summary_df.explain()

# 5. 액션 (Action - 이 시점에 실제 분산 연산 트리거)
print("\n=== 집계 결과 출력 ===")
summary_df.show()

# 세션 종료
spark.stop()