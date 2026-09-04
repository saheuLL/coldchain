import json
import os
import signal
import sys
import requests
import psycopg2
from psycopg2.extras import RealDictCursor
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import time

# ==========================================
# 1. 설정값 정의 (환경변수 또는 기본값)
# ==========================================
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
TOPIC_NAME = os.getenv('KAFKA_TOPIC', 'vehicle-telemetry')
CONSUMER_GROUP_ID = os.getenv('CONSUMER_GROUP_ID', 'coldchain-consumer-group')
ALERT_COOLDOWN_SECONDS = 60
last_alert_sent = {}

# TimescaleDB 연결 정보
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'coldchain_db')
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'password123!')

# discord Incoming Webhook URL (테스트 시 본인 웹훅 주소로 교체)
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "https://discord.com/api/webhooks/1542044129704411137/ZJu0iV7rVjvTDuol83n0_NKPiOu5g_gW4KxoE_Bg2NRnUrisKaGB_QQl5UmICMvYzJb4/slack")

# 비즈니스 임계치
TEMP_THRESHOLD = 0.0  # 냉동 안전 한계 온도 (초과 시 이상 감지)

# ==========================================
# 2. 헬퍼 함수: DB 및 알림 연동
# ==========================================
# 대규모 트래픽 환경에서는 PgBouncer나 커넥션풀을 통해 관리
def get_db_connection():
    """TimescaleDB 커넥션 생성"""
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

def find_nearest_center(cur, lat, lng):
    """
    PostGIS 공간 쿼리를 실행해 현재 차량 좌표(lat, lng)에서
    가장 가까운 물류센터 1곳과 거리를 조회
    """
    query = """
    SELECT 
        center_id,
        name,
        contact,
        address,
        ROUND(ST_Distance(location, ST_SetSRID(ST_MakePoint(%s, %s), 4326))::numeric, 1) AS distance_meters
    FROM logistics_centers
    ORDER BY location <-> ST_SetSRID(ST_MakePoint(%s, %s), 4326)
    LIMIT 1;
    """
    # PostGIS는 X(경도/lng), Y(위도/lat) 순서로 좌표를 받음
    cur.execute(query, (lng, lat, lng, lat))
    return cur.fetchone()

def save_telemetry_to_db(cur, data):
    """실시간 차량 텔레메트리 데이터를 TimescaleDB에 적재"""
    insert_query = """
    INSERT INTO vehicle_telemetry (
        time, vehicle_id, vehicle_name, temperature, 
        humidity, speed_kmh, latitude, longitude, location
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s,
        ST_SetSRID(ST_MakePoint(%s, %s), 4326)
    );
    """
    cur.execute(insert_query, (
        data['timestamp'],
        data['vehicle_id'],
        data['vehicle_name'],
        data['temperature'],
        data['humidity'],
        data['speed_kmh'],
        data['latitude'],
        data['longitude'],
        data['longitude'], # Point X
        data['latitude']   # Point Y
    ))

def send_discord_alert(data, center_info):
    """이상 온도 감지 시 discord 채널로 리치 포맷 메시지 전송"""
    if "YOUR/DISCORD/WEBHOOK_URL" in DISCORD_WEBHOOK_URL:
        # 실제 웹훅이 없을 때는 콘솔에만 출력하고 패스
        print(f"   [discord 알림 스킵] 웹훅 URL 미설정 -> {data['vehicle_id']} 온도: {data['temperature']}°C")
        return

    # 거리를 km 단위로 변환
    dist_km = round(float(center_info['distance_meters']) / 1000.0, 2)
    
    payload = {
        "text": f"🚨 *[긴급] 콜드체인 온도 이탈 경보 - {data['vehicle_id']}*",
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"🚨 콜드체인 이상 온도 감지 ({data['vehicle_id']})"
                }
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*차량명:* {data['vehicle_name']}"},
                    {"type": "mrkdwn", "text": f"*현재 온도:* ` {data['temperature']}°C ` (임계치: -18°C)"},
                    {"type": "mrkdwn", "text": f"*차량 속도:* {data['speed_kmh']} km/h"},
                    {"type": "mrkdwn", "text": f"*현재 좌표:* {data['latitude']}, {data['longitude']}"}
                ]
            },
            {
                "type": "divider"
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        f"🏬 *긴급 회차 권장 물류센터:* *{center_info['name']}*\n"
                        f"• *직선거리:* 약 `{dist_km} km`\n"
                        f"• *센터 주소:* {center_info['address']}\n"
                        f"• *비상 연락처:* `{center_info['contact']}`"
                    )
                }
            }
        ]
    }
    
    try:
        response = requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=3)
        if response.status_code == 200:
            print(f"   📢 [Discord 전송 성공] {data['vehicle_id']} 경보 발송 완료 (최근접: {center_info['name']})")
        else:
            print(f"   ⚠️ [Discord 전송 실패] HTTP {response.status_code}: {response.text}")
    except Exception as e:
        print(f"   ❌ [Discord 전송 에러]: {e}")

# ==========================================
# 3. Consumer 메인 루프 (카프카 수신 및 파이프라인 처리)
# ==========================================
def main():
    print("[*] DB 연결 초기화 중...")
    db_conn = get_db_connection()
    db_cur = db_conn.cursor(cursor_factory=RealDictCursor)
    print("[*] TimescaleDB 연결 성공.")

    print(f"[*] Kafka Consumer 생성 중 (Topic: {TOPIC_NAME}, Group: {CONSUMER_GROUP_ID})...")
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP_ID,
        api_version=(2, 0, 2),
        
        # [역직렬화]: 카프카에서 받은 UTF-8 바이트 스트림 -> JSON 딕셔너리로 변환
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        
        # [신뢰성 보장]: 자동 커밋을 끄고(False), DB 적재가 완전히 끝났을 때만 수동 커밋
        enable_auto_commit=False,
        
        # 처음 붙을 때 가장 오래된 메시지부터 읽어옴 (earliest)
        auto_offset_reset='earliest'
    )

    print("[*] 컨슈머 대기 상태 돌입. 메시지 수신 대기 중... (종료: Ctrl+C)")

    # 안전한 종료(Graceful Shutdown)를 위한 플래그
    running = True
    def handle_shutdown(signum, frame):
        nonlocal running
        print("\n[!] 안전 종료 신호 수신. 마무리 작업 진행 중...")
        running = False

    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    try:
        while running:
            # 1초 타임아웃으로 메시지 폴링 (배치 단위 수신)
            message_batch = consumer.poll(timeout_ms=1000)
            
            if not message_batch:
                continue

            for topic_partition, messages in message_batch.items():
                for msg in messages:
                    data = msg.value
                    temp = data.get('temperature')
                    v_id = data.get('vehicle_id')
                    
                    # ----------------------------------------------------
                    # Step 1: 실시간 이상 온도 검사 (In-Memory 판별)
                    # ----------------------------------------------------
                    if temp > TEMP_THRESHOLD:
                        current_time = time.time()
                        last_sent = last_alert_sent.get(v_id, 0)
                        if current_time - last_sent >= ALERT_COOLDOWN_SECONDS:
                            print(f"🚨 [이상 감지] {v_id} 온도 이탈! ({temp}°C) -> 📢 Discord 알림 발송")
                        
                            # Step 2: PostGIS로 가장 가까운 거점 물류센터 쿼리
                            nearest_center = find_nearest_center(db_cur, data['latitude'], data['longitude'])
                            
                            # Step 3: Discord 비상 알림 발송
                            if nearest_center:
                                send_discord_alert(data, nearest_center)
                            last_alert_sent[v_id] = current_time
                        else:
                            # 쿨다운 중일 때는 로그만 찍고 디스코드 전송은 스킵
                            remaining = int(ALERT_COOLDOWN_SECONDS - (current_time - last_sent))
                            print(f"⏳ [알림 쿨다운] {v_id} 이상 지속 중 ({temp}°C) - 다음 알림까지 {remaining}초 대기")
                    else:
                        print(f"✅ [정상 수신] {v_id} ({temp}°C) - 파티션:{msg.partition}, 오프셋:{msg.offset}")

                    # ----------------------------------------------------
                    # Step 4: TimescaleDB 시계열 테이블에 적재
                    # ----------------------------------------------------
                    save_telemetry_to_db(db_cur, data)

                # 배치 단위로 DB 커밋 및 카프카 오프셋 커밋 실행
                db_conn.commit()
                consumer.commit()

    except Exception as e:
        print(f"❌ [치명적 에러 발생]: {e}")
        db_conn.rollback()
    finally:
        print("[*] 리소스 정리 중 (DB 닫기, 카프카 컨슈머 연결 해제)...")
        db_cur.close()
        db_conn.close()
        consumer.close()
        print("[*] Consumer 안전 종료 완료.")

if __name__ == '__main__':
    main()