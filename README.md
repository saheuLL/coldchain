# ❄️️ Real-Time Cold Chain Telemetry Pipeline

> **GKE, Kafka, KEDA 기반의 이벤트 기반(EDA) 실시간 콜드체인 관제 및 선제적 오토스케일링 인프라**

대규모 물류 차량 IoT 센서로부터 초당 발생하는 GPS 좌표 및 온·습도 텔레메트리 데이터를 수집하고, 임계치(-18°C) 이탈 시 인접 거점을 즉시 계산하여 알림을 전송하는 클라우드 네이티브 관제 파이프라인입니다.

---

## 🏛️ 시스템 아키텍쳐

![Architecture](https://app.notion.com/p/34ffdc154f30807bb4b7f0910ff35359?source=copy_link#3e9fdc154f308070b86ce45a1273b4ca)

- **Message Streaming**: Apache Kafka (KRaft Mode, Multi-Partition)
- **Container Orchestration**: Google Kubernetes Engine (GKE)
- **Autoscaling**: KEDA (Kafka Consumer Lag Trigger)
- **Database**: TimescaleDB (Hypertables), PostGIS (GIST Index)
- **Monitoring & Alert**: Prometheus, Grafana, Discord Webhook

---

## 📂 프로젝트 구조

```text
├── k8s/                  # 쿠버네티스 매니패스트 (Deployments, Services)
│   └── [00-05]-*.yaml             # Producer & Consumer Deployment manifests
├── consumer.py           # 컨슈머 (TimescaleDB/PostGIS 적재 & 알림 전송)
├── producer.py             # IoT 센서 데이터 시뮬레이터 (Mock Telemetry Producer)
├── Dockerfile            # 컨슈머와 프로듀서 이미지 만들기 위한 도커파일
└── README.md
