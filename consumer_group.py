import json
import sys
from kafka import KafkaConsumer

# 실행 시 컨슈머 인스턴스 식별용 ID 입력받기 (기본값: Worker-1)
worker_id = sys.argv[1] if len(sys.argv) > 1 else "Worker-1"

consumer = KafkaConsumer(
    "order-events",
    bootstrap_servers=["localhost:9092"],
    group_id="order-checker-group",
    auto_offset_reset="earliest",  # 처음 붙을 땐 가장 처음 오프셋부터
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    key_deserializer=lambda k: k.decode("utf-8") if k else None,
)

print(f"[{worker_id}] 구독 시작 및 코디네이터 연결 대기 중...")

try:
    for message in consumer:
        # 현재 할당된 파티션 목록 확인
        assigned_partitions = [p.partition for p in consumer.assignment()]
        
        print(
            f"[{worker_id}] (보유 파티션: {assigned_partitions}) | "
            f"수신 파티션: {message.partition} | "
            f"오프셋: {message.offset:<3} | "
            f"Key: {message.key:<7} | "
            f"주문 ID: {message.value['order_id']} ({message.value['symbol']})"
        )
except KeyboardInterrupt:
    print(f"\n[{worker_id}] 컨슈머 종료 중...")
finally:
    consumer.close()
    print(f"[{worker_id}] 연결 해제 완료.")