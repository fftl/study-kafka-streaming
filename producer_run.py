import json
import random
import time
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic_name = "first-topic"
items = ["keyboard", "mouse", "monitor", "laptop", "headset"]
users = ["user_a", "user_b", "user_c", "user_d"]

order_seq = 1000

print(f"[{topic_name}] 연속 스트리밍 데이터 전송 시작 (중단: Ctrl + C)...")

try:
    while True:
        order_seq += 1
        data = {
            "order_id": order_seq,
            "user_id": random.choice(users),
            "item": random.choice(items),
            "amount": random.randint(1, 20) * 10000,
            "timestamp": time.time()
        }
        
        future = producer.send(topic_name, value=data)
        record = future.get(timeout=10)
        
        print(f"[전송 완료] 오프셋: {record.offset} | 주문: {data['order_id']} | 품목: {data['item']} | 금액: {data['amount']:,}원")
        time.sleep(1) # 1초 주기로 전송
except KeyboardInterrupt:
    print("\n[안내] 전송을 안전하게 종료합니다.")
finally:
    producer.flush()
    producer.close()