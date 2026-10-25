import json
from kafka import KafkaConsumer

topic_name = "first-topic"

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=['localhost:9092'],
    group_id='stream-processing-group',       # 새로운 그룹 ID 부여
    auto_offset_reset='latest',               # 켜진 시점 이후의 실시간 데이터만 수신
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

print(f"[{topic_name}] 실시간 스트리밍 컨슈머 대기 중 (중단: Ctrl + C)...")

try:
    for message in consumer:
        val = message.value
        print(f"[수신] 파티션: {message.partition} | 오프셋: {message.offset} | 주문번호: {val['order_id']} | 고객: {val['user_id']} | {val['amount']:,}원")
except KeyboardInterrupt:
    print("\n[안내] 컨슈머를 안전하게 종료합니다.")
finally:
    consumer.close()