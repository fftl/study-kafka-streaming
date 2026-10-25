import json
import time
from kafka import KafkaProducer

# 1. Producer 객체 생성
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic_name = "first-topic"

# 2. 전송할 메시지 데이터 정의
payload = {
    "order_id": 1001,
    "user_id": "lee",
    "item": "keyboard",
    "amount": 89000,
    "timestamp": time.time()
}

print(f"[Producer] 메시지 전송 준비: {payload}")

# 3. 메시지 비동기 전송
future = producer.send(topic_name, value=payload)

# 4. 브로커 수신 완료 응답 대기 (동기식 검증)
record_metadata = future.get(timeout=10)

print("-" * 50)
print(f"[성공] 브로커에 정상 적재되었습니다.")
print(f" - 토픽명: {record_metadata.topic}")
print(f" - 파티션 번호: {record_metadata.partition}")
print(f" - 오프셋 번호: {record_metadata.offset}")
print("-" * 50)

# 버퍼 비우기
producer.flush()