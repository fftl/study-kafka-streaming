import json
from kafka import KafkaConsumer

topic_name = "first-topic"

# 1. Consumer 초기화
consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=['localhost:9092'],
    group_id='test-consumer-group',          # 컨슈머 그룹 지정
    auto_offset_reset='earliest',             # 처음 시작할 땐 맨 처음(0번) 오프셋부터
    enable_auto_commit=True,                  # 자동으로 읽은 위치 커밋
    value_deserializer=lambda v: json.loads(v.decode('utf-8')), # bytes -> json dict 복원
    consumer_timeout_ms=5000                  # 5초 동안 새 메시지가 안 오면 반복문 종료 (테스트용)
)

print(f"[{topic_name}] 토픽 구독 시작 (메시지 수신 대기 중)...")

# 2. 메시지 수신 루프
message_count = 0
for message in consumer:
    message_count += 1
    print("-" * 50)
    print(f"메시지 수신 성공 (#{message_count})")
    print(f" - 파티션 번호 : {message.partition}")
    print(f" - 오프셋 번호 : {message.offset}")
    print(f" - 받은 데이터 : {message.value}")

if message_count == 0:
    print("수신할 새로운 메시지가 없습니다.")
else:
    print("-" * 50)
    print(f"총 {message_count}건의 메시지를 성공적으로 처리했습니다.")

# 컨슈머 안전 종료
consumer.close()