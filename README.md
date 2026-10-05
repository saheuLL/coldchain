# ❄️️ Real-Time Cold Chain Telemetry Pipeline

> **GKE, Kafka, KEDA 기반의 이벤트 기반(EDA) 실시간 콜드체인 관제 및 선제적 오토스케일링 인프라**

대규모 물류 차량 IoT 센서로부터 초당 발생하는 GPS 좌표 및 온·습도 텔레메트리 데이터를 수집하고, 임계치(-18°C) 이탈 시 인접 거점을 즉시 계산하여 알림을 전송하는 클라우드 네이티브 관제 파이프라인입니다.

---

## 🏛️ System Architecture

![Architecture](포폴에_올린_아키텍처_이미지_경로_또는_상대경로)

- **Message Streaming**: Apache Kafka (KRaft Mode, Multi-Partition)
- **Container Orchestration**: Google Kubernetes Engine (GKE)
- **Autoscaling**: KEDA (Kafka Consumer Lag Trigger)
- **Database**: TimescaleDB (Hypertables), PostGIS (GIST Index)
- **Monitoring & Alert**: Prometheus, Grafana, Discord Webhook

---

## 📂 Project Structure

```text
├── k8s/                  # Kubernetes Manifests (Deployments, Services, ScaledObject)
│   ├── kafka/            # Kafka Broker & Topic manifests
│   ├── keda/             # KEDA ScaledObject & TriggerAuthentication
│   └── apps/             # Producer & Consumer Deployment manifests
├── consumer/             # Python Stream Consumer (TimescaleDB/PostGIS 적재 & Anomaly 감지)
├── producer/             # IoT Sensor Data Simulator (Mock Telemetry Producer)
└── README.md
