import json
import random
import time
from kafka import KafkaProducer

def json_serializer(data):
    return json.dumps(data).encode("utf-8")

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    key_serializer=lambda k: str(k).encode("utf-8"),
    value_serializer=json_serializer,
    acks=1,
)

users = ["user_A", "user_B", "user_C", "user_D"]
items = ["BTC", "ETH", "SOL", "XRP"]

print("[PRODUCER] Key 기반 메시지 전송 시작...")

try:
    for i in range(1, 21):
        user_id = random.choice(users)
        payload = {
            "order_id": f"ord_{i:04d}",
            "user_id": user_id,
            "symbol": random.choice(items),
            "amount": round(random.uniform(0.01, 2.0), 4),
            "timestamp": time.time(),
        }

        # key와 value를 함께 전송하고 비동기 Future 객체 반환
        future = producer.send(topic="order-events", key=user_id, value=payload)
        
        # 메타데이터를 동기식으로 취득하여 실제 할당된 파티션 번호 확인
        record_metadata = future.get(timeout=10)

        print(
            f"[전송 완료] Key: {user_id:<7} | "
            f"Partition: {record_metadata.partition} | "
            f"Offset: {record_metadata.offset:<3} | "
            f"Payload: {payload['order_id']} ({payload['symbol']})"
        )
        time.sleep(0.5)

except KeyboardInterrupt:
    print("\n[PRODUCER] 전송 중단.")
finally:
    producer.flush()
    producer.close()
    print("[PRODUCER] 연결 종료.")