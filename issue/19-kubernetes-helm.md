# Issue 19: Kubernetes Deployment — Helm Chart

## Tujuan
Memigrasikan deployment pipeline dari Docker Compose ke Kubernetes (Minikube untuk lokal, GKE untuk production) menggunakan Helm chart.

---

## Spesifikasi

### A. Helm Chart Structure

```
k8s/
├── Chart.yaml
├── values.yaml                 # Default values
├── values-dev.yaml             # Dev override
├── values-prod.yaml            # Production override
├── templates/
│   ├── _helpers.tpl            # Template helpers (naming, labels)
│   ├── namespace.yaml
│   ├── configmap.yaml          # Non-sensitive config
│   ├── secret.yaml             # GCP service account
│   ├── kafka/
│   │   ├── statefulset.yaml    # Kafka StatefulSet (KRaft, 1 replica)
│   │   ├── service.yaml        # Internal + external service
│   │   └── pvc.yaml            # Persistent volume claim
│   ├── spark/
│   │   ├── master-deployment.yaml
│   │   ├── master-service.yaml
│   │   ├── worker-deployment.yaml
│   │   └── worker-service.yaml
│   ├── producer/
│   │   └── deployment.yaml     # Producer Deployment (1 replica)
│   ├── processor/
│   │   └── deployment.yaml     # Spark processor Deployment
│   ├── monitoring/             # Prometheus + Grafana (via subchart dependency)
│   └── ingress.yaml            # Ingress untuk Spark UI, Grafana
└── Chart.lock
```

### B. `values.yaml` — Konfigurasi Utama

```yaml
namespace: idx-stream

kafka:
  image: confluentinc/cp-kafka:7.5.5
  replicas: 1
  storage: 10Gi
  resources:
    requests: { memory: "1Gi", cpu: "500m" }
    limits:   { memory: "2Gi", cpu: "1000m" }

spark:
  master:
    image: apache/spark:3.4.4
    replicas: 1
    resources:
      requests: { memory: "512Mi", cpu: "250m" }
      limits:   { memory: "1Gi", cpu: "500m" }
  worker:
    replicas: 2
    resources:
      requests: { memory: "1Gi", cpu: "500m" }
      limits:   { memory: "2Gi", cpu: "1000m" }

producer:
  replicas: 1
  schedule: "*/5 * * * *"   # CronJob schedule
  env:
    FETCH_INTERVAL_SECONDS: "300"

processor:
  replicas: 1
  checkpointStorage: 5Gi

monitoring:
  prometheus:
    enabled: true
    retention: 15d
  grafana:
    enabled: true
    adminPassword: admin

gcp:
  projectId: idx-analytics-platform
  serviceAccountKey: ""  # From sealed secret
```

### C. Producer as CronJob (bukan Deployment)

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: idx-producer
spec:
  schedule: "*/5 * * * *"
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: producer
            image: ghcr.io/adityapratama575/idx-producer:latest
            envFrom:
            - configMapRef: { name: idx-config }
            - secretRef: { name: idx-secrets }
            resources:
              requests: { memory: "256Mi", cpu: "250m" }
              limits:   { memory: "512Mi", cpu: "500m" }
          restartPolicy: OnFailure
```

### D. Kafka StatefulSet (KRaft)

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: kafka
spec:
  serviceName: kafka-headless
  replicas: 1        # Single node for dev
  podManagementPolicy: OrderedReady
  template:
    spec:
      containers:
      - name: kafka
        image: confluentinc/cp-kafka:7.5.5
        ports:
        - containerPort: 9092
          name: external
        - containerPort: 29092
          name: internal
        - containerPort: 9093
          name: controller
        env:
        - name: KAFKA_NODE_ID
          value: "1"
        - name: KAFKA_LISTENERS
          value: "PLAINTEXT://0.0.0.0:29092,PLAINTEXT_HOST://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093"
        - name: KAFKA_ADVERTISED_LISTENERS
          value: "PLAINTEXT://kafka-0.kafka-headless:29092,PLAINTEXT_HOST://localhost:9092"
        volumeMounts:
        - name: kafka-data
          mountPath: /var/lib/kafka/data
  volumeClaimTemplates:
  - metadata: { name: kafka-data }
    spec:
      accessModes: ["ReadWriteOnce"]
      resources:
        requests: { storage: 10Gi }
```

### E. Secrets Management (via Sealed Secrets)

Lihat Issue 20 untuk detail secret management.

### F. Ingress

```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: idx-ingress
  annotations:
    nginx.ingress.kubernetes.io/rewrite-target: /
spec:
  rules:
  - host: idx-spark.local
    http:
      paths:
      - path: /
        pathType: Prefix
        backend:
          service:
            name: spark-master
            port: { number: 8080 }
  - host: idx-grafana.local
    http:
      paths:
      - path: /
        pathType: Prefix
        backend:
          service:
            name: grafana
            port: { number: 3000 }
```

### G. Deployment Commands

```bash
# Minikube (local dev)
minikube start --cpus 4 --memory 8192
helm upgrade --install idx-stream ./k8s -f k8s/values-dev.yaml -n idx-stream --create-namespace

# GKE (production)
gcloud container clusters create idx-stream-prod --zone asia-southeast2-a
helm upgrade --install idx-stream ./k8s -f k8s/values-prod.yaml -n idx-stream --create-namespace
```

### H. Minikube addons yang dibutuhkan

```bash
minikube addons enable ingress
minikube addons enable metrics-server
```

---

## Acceptance Criteria

- [ ] `helm install` berhasil deploy semua komponen ke Minikube
- [ ] `kubectl get pods` menunjukkan semua pod running (kafka, spark-master, spark-worker, grafana, prometheus)
- [ ] Producer CronJob berjalan sesuai schedule
- [ ] Ingress bisa diakses: Spark UI di `http://idx-spark.local`, Grafana di `http://idx-grafana.local`
- [ ] Kafka data + Spark checkpoint bertahan setelah pod restart (PVC)
- [ ] Resource limits/requests diset untuk semua container
- [ ] Health check (liveness + readiness probe) di semua Deployment
