# Customer 360 Lakehouse — Ngân hàng Bán lẻ

Hệ thống **Data Lakehouse** phục vụ phân tích khách hàng 360° cho ngân hàng bán lẻ. Được xây dựng trên Apache Iceberg + Spark + Trino + Airflow, chạy hoàn toàn trên Docker.

---

## Demo Story — Tại sao cần Lakehouse?

**Trước Lakehouse**: Team Marketing cần danh sách khách hàng để chạy chiến dịch cross-sell thẻ tín dụng → gửi yêu cầu cho DBA → chờ **2–3 ngày**.

**Sau Lakehouse**: Marketing tự query Trino với SQL đơn giản → kết quả trong **< 10 giây**.

```sql
-- Marketing tự chạy, không cần DBA
SELECT customer_id, full_name_masked, aum_total, rfm_segment
FROM lakehouse.sandbox.mart_customer_360_masked
WHERE has_credit_card = 0
  AND aum_total > 100000000          -- Tổng tiền gửi > 100 triệu
  AND rfm_segment IN ('Champions', 'Loyal Customers')
  AND days_since_last_txn <= 30
  AND primary_branch_code LIKE 'HCM%'
ORDER BY aum_total DESC
LIMIT 1000;
```

---

## Kiến trúc tổng quan

```
┌─────────────────────────────────────────────────────────────────┐
│                        SOURCE SYSTEMS                           │
│  ┌──────────────────┐        ┌──────────────────────────────┐   │
│  │  Oracle XE 21c   │        │      PostgreSQL 15            │   │
│  │  Core Banking    │        │   Card System + CRM          │   │
│  │  (7 tables)      │        │   (3 tables)                 │   │
│  └────────┬─────────┘        └─────────────┬────────────────┘   │
└───────────┼──────────────────────────────── ┼───────────────────┘
            │           JDBC Ingest           │
            ▼                                 ▼
┌─────────────────────────────────────────────────────────────────┐
│                    LAKEHOUSE (MinIO + Iceberg)                   │
│                                                                  │
│  ┌─────────────┐   ┌─────────────┐   ┌─────────────────────┐   │
│  │   BRONZE    │──▶│   SILVER    │──▶│        GOLD         │   │
│  │  Raw copy   │   │ Clean+SCD2  │   │  Mart Customer 360  │   │
│  │ 10 tables   │   │ 10 tables   │   │  + Segments + Branch│   │
│  └─────────────┘   └─────────────┘   │  10 tables          │   │
│                                       └─────────────────────┘   │
│                                                                  │
│  Compute: Apache Spark 3.5    Catalog: Iceberg REST Catalog     │
└─────────────────────────────────────────────────────────────────┘
            │                                 │
            ▼                                 ▼
┌───────────────────┐               ┌─────────────────────────────┐
│ Apache Airflow    │               │  Trino (Ad-hoc Query)       │
│ DAG Orchestration │               │  DBeaver / SQL Client       │
└───────────────────┘               └─────────────────────────────┘
```

---

## Tech Stack

| Lớp | Công nghệ | Version |
|-----|-----------|---------|
| Source DB | Oracle XE | 21c |
| Source DB | PostgreSQL | 15 |
| Object Storage | MinIO | RELEASE.2025-09-07 |
| Table Format | Apache Iceberg | 1.4.3 |
| Compute | Apache Spark | 3.5.0 |
| Query Engine | Trino | 481 |
| Orchestration | Apache Airflow | 2.10.0 |
| Notebook | JupyterLab | 4.3.8 |
| Container | Docker Compose | — |

---

## Quick Start

### Yêu cầu
- Docker Desktop ≥ 4.x, RAM ≥ 12GB (allocate ≥ 10GB cho Docker)
- Python 3.10+ (cho data generator chạy local)
- DBeaver (để query Trino)

### Các bước

```bash
# 1. Chuẩn bị config (không commit các file chứa secret)
cd retail-banking-customer360-lakehouse
cp docker/.env.example docker/.env
cp data_generator/config.example.yaml data_generator/config.yaml

# 2. Build custom images (~5 phút lần đầu)
docker compose -f docker/docker-compose.yml build

# 3. Khởi động stack
docker compose -f docker/docker-compose.yml up -d

# 4. Chờ Oracle khởi động (~60-90 giây)
docker compose -f docker/docker-compose.yml logs -f oracle
# Chờ: "DATABASE IS READY TO USE!"

# 5. Sinh dữ liệu giả lập
cd data_generator && pip install -r requirements.txt && python run_sql_gen.py

# 6. Khởi tạo Iceberg tables (qua Airflow UI hoặc CLI)
docker compose -f docker/docker-compose.yml exec airflow-scheduler \
    airflow dags trigger implement_iceberg_table_dag --conf '{"layer": "all"}'

# 7. Chạy pipeline Bronze → Silver → Gold (qua Airflow UI hoặc CLI)
# Thứ tự: bronze_initial → bronze_core_banking + bronze_card_crm → silver_all → gold_mart360 → gold_segmentation
```

### Truy cập các service

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow UI | http://localhost:8080 | Theo `docker/.env` |
| Spark Master | http://localhost:9090 | — |
| MinIO Console | http://localhost:9001 | Theo `docker/.env` |
| Trino | localhost:8085 | Connect qua DBeaver |
| JupyterLab | http://localhost:8888 | — (không cần password) |

> **Jupyter**: Khởi động cùng stack và dùng thư mục `notebooks/` làm workspace bền vững. Do Spark cluster hiện có một worker nhỏ, không chạy notebook Spark đồng thời với Airflow Spark jobs.

---

## Cấu trúc thư mục

```
retail-banking-customer360-lakehouse/
├── docker/                     # Docker Compose + Dockerfiles + init scripts
│   ├── docker-compose.yml      # Toàn bộ service Lakehouse
│   ├── .env.example            # Template credentials
│   ├── init_oracle/            # DDL Oracle (7 bảng Core Banking)
│   ├── init_postgres/          # DDL PostgreSQL (3 bảng Card + CRM)
│   ├── spark/                  # Custom Spark image + config
│   ├── airflow/                # Custom Airflow image
│   ├── jupyter/                # Custom JupyterLab image
│   └── trino/                  # Trino config + catalog
│
├── data_generator/             # Python script sinh dữ liệu giả lập
│   ├── run_sql_gen.py          # Entry point
│   ├── config.example.yaml     # Template; config.yaml thật được git-ignore
│   ├── sql_generators/         # SQL sinh dữ liệu Oracle/PostgreSQL
│   └── loaders/                # Oracle + PostgreSQL loaders
│
├── code_etl/                   # Spark ETL jobs
│   ├── bronze/                 # Generic JDBC ingest + 10 YAML configs
│   ├── silver/                 # SCD1/SCD2/Fact jobs + 10 YAML configs
│   ├── gold/                   # Gold mart jobs + 9 YAML configs
│   └── shared/                 # SparkSession, utils, ops (PII masking, maintenance)
│
├── airflow/                    # DAGs + plugins
│   ├── dags/
│   │   ├── bronze/             # 3 DAGs (initial + core_banking + card_crm)
│   │   ├── silver/             # 1 consolidated DAG (silver_all)
│   │   ├── gold/               # 3 DAGs (mart360 + segmentation + time_analytics)
│   │   ├── ops/                # 3 DAGs (implement_iceberg + pii_masking + maintenance)
│   │   └── util/               # 1 DAG (util_spark_sql — debug/ad-hoc)
│   └── plugins/                # ETL flag, JDBC utils
│
├── notebooks/                  # PySpark baseline và acceptance runbooks
├── tests/                      # Static contract + isolated Iceberg integration tests
└── sql_templates/trino/        # 6 business queries
```

---

## Dữ liệu giả lập (Thực tế trong config)

| Entity | Số lượng | Ghi chú |
|--------|---------|---------|
| Chi nhánh | 50 | Toàn quốc, phân bố theo vùng |
| Sản phẩm | 16 | CASA, tiết kiệm, vay, thẻ |
| Khách hàng | 10,000 | 70% Retail / 25% Priority / 5% VIP |
| Tài khoản | ~13,500 | Tài khoản CASA và tiền gửi có kỳ hạn |
| Sổ tiết kiệm | ~5,000 | — |
| Khoản vay | ~3,000 | — |
| Giao dịch TK | ~1,050,000 | 12 tháng (Jan–Dec 2025) |
| Thẻ | ~10,000 | Debit + Credit |
| Giao dịch thẻ | ~212,400 | 12 tháng |
| CRM Interactions | ~10,000 | 12 tháng |

> Lịch sử giao dịch 1 năm đầy đủ cho phép tính RFM 90 ngày, churn detection 365 ngày, và monthly analytics có ý nghĩa thống kê.

---

## Tài liệu

Thư mục `docs/` được giữ local và chủ động loại khỏi Git repository theo phạm vi chia sẻ hiện tại.
