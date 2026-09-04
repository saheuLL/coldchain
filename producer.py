import json
import time
import random
import os
from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.errors import KafkaError

# ==========================================
# 1. 시뮬레이션 차량 기본 정보 및 경로 정의
# ==========================================
# 3대 차량의 이동 경로 (위도, 경도 좌표 리스트)
VEHICLES = [
    {
        "id": "TRUCK-101",
        "name": "의정부-노원 1호차 (냉동)",
        "target_temp": -20.0,
        "is_faulty": False,  # 정상 차량
        "route": [
            (37.7381, 127.0456), # 의정부역
            (37.7125, 127.0512), # 망월사역
            (37.6892, 127.0544), # 도봉산역
            (37.6542, 127.0605), # 노원역
        ]
    },
    {
        "id": "TRUCK-102",
        "name": "고양-마포 2호차 (냉동)",
        "target_temp": -22.0,
        "is_faulty": False,  # 정상 차량
        "route": [
            (37.6584, 126.8320), # 일산 킨텍스
            (37.6315, 126.8790), # 화정역
            (37.5926, 126.9038), # 디지털미디어시티역
            (37.5568, 126.9236), # 홍대입구역
        ]
    },
    {
        "id": "TRUCK-103",
        "name": "하남-강남 3호차 (냉각기 고장 시뮬레이션)",
        "target_temp": -19.0,
        "is_faulty": True,   # 냉각기 고장 발생 시나리오
        "route": [
            (37.5385, 127.2140), # 하남검단산역
            (37.5145, 127.1059), # 잠실역
            (37.5088, 127.0631), # 삼성역
            (37.4979, 127.0276), # 강남역
        ]
    }
]

# ==========================================
# 2. Kafka Producer 인스턴스 생성
# ==========================================

kafka_broker = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
producer = KafkaProducer(
    # Docker Compose 외부(VM 로컬)에서 접속하므로 localhost:9092 사용
    bootstrap_servers=[kafka_broker],
    api_version=(2, 0, 2),
    # [직렬화]: 파이썬 딕셔너리 -> JSON 문자열 -> UTF-8 바이트 스트림 변환
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    
    # [메시지 키 직렬화]: 차량 ID를 바이트로 변환 (파티셔닝 기준)
    key_serializer=lambda k: k.encode('utf-8'),
    
    # [신뢰성 옵션]: 1=리더 브로커가 메시지를 디스크 메모리에 기록했는지 확인 후 응답
    acks=1,
    
    # [전송 최적화]: 5ms 대기 후 메시지를 묶어서 전송 (배치 처리)
    linger_ms=5,
    retries=3
)

TOPIC_NAME = 'vehicle-telemetry'

print(f"[*] Kafka Producer 시작: 토픽 '{TOPIC_NAME}'으로 센서 데이터 전송 중... (종료: Ctrl+C)")

# 각 차량별 현재 경로 인덱스 및 누적 온도 상태 관리
state = {
    v["id"]: {
        "step": 0,
        "current_temp": v["target_temp"],
        "temp_drift": 0.0
    } for v in VEHICLES
}

try:
    step_count = 0
    while True:
        step_count += 1
        
        for vehicle in VEHICLES:
            v_id = vehicle["id"]
            route = vehicle["route"]
            v_state = state[v_id]
            
            # 1. GPS 위치 계산 (경로 순환 이동 + 미세 노이즈 추가)
            base_lat, base_lng = route[v_state["step"] % len(route)]
            lat = round(base_lat + random.uniform(-0.0005, 0.0005), 6)
            lng = round(base_lng + random.uniform(-0.0005, 0.0005), 6)
            
            # 2. 온도 시뮬레이션
            if vehicle["is_faulty"]:
                # 고장 차량은 루프가 돌수록 온도가 점진적으로 상승 (-19도 -> -10도 이상)
                v_state["temp_drift"] += round(random.uniform(0.3, 0.8), 2)
                current_temp = round(vehicle["target_temp"] + v_state["temp_drift"], 2)
            else:
                # 정상 차량은 목표 온도 기준 +-0.5도 범위에서 미세 변동
                current_temp = round(vehicle["target_temp"] + random.uniform(-0.5, 0.5), 2)

            # 3. 전송할 JSON 텔레메트리 페이로드 구성
            payload = {
                "vehicle_id": v_id,
                "vehicle_name": vehicle["name"],
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "latitude": lat,
                "longitude": lng,
                "temperature": current_temp,
                "humidity": round(random.uniform(45.0, 55.0), 1),
                "speed_kmh": random.randint(40, 80)
            }

            # 4. Kafka로 비동기 전송
            # key를 vehicle_id로 지정하여 같은 차량의 데이터는 항상 같은 파티션으로 인입되도록 보장
            future = producer.send(
                topic=TOPIC_NAME,
                key=v_id,
                value=payload
            )
            
            # 전송 메타데이터 확인 (비동기 콜백 스타일) 카프카에 전송됐는지 확인증 10초 기다려 확인
            record_metadata = future.get(timeout=10)
            
            status_indicator = "🚨 [이상 감지]" if current_temp > -18.0 else "✅ [정상]"
            print(f"{status_indicator} [{v_id}] 파티션:{record_metadata.partition} 오프셋:{record_metadata.offset} | 온도:{current_temp}°C | 위치:({lat}, {lng})")
            
            # 다음 경로 좌표로 한 칸 이동
            v_state["step"] += 1

        print("-" * 50)
        # 1초 주기로 전송
        time.sleep(1)

except KeyboardInterrupt:
    print("\n[!] Producer 종료 중...")
finally:
    # 버퍼에 남아있는 모든 잔여 메시지 flush 후 안전하게 연결 종료
    producer.flush()
    producer.close()
    print("[*] Producer 안전 종료 완료.")              