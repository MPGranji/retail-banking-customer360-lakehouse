# Hướng dẫn chạy project từ đầu sau khi clone

Tài liệu này dành cho người clone repository về máy local và muốn dựng lại toàn bộ
Customer 360 Lakehouse bằng Docker. Guide tập trung vào cách chạy hệ thống từ một
môi trường sạch đến lúc có dữ liệu Gold, Sandbox, Data Quality và dashboard mẫu.

Chạy các lệnh từ thư mục gốc của project, tức thư mục có `docker/`, `airflow/`,
`code_etl/`, `data_generator/` và `sql_templates/`.

## 1. Yêu cầu hệ thống

| Thành phần | Khuyến nghị |
|---|---|
| Docker Desktop / Docker Engine | Docker Compose v2 |
| RAM host | Tối thiểu 16GB, cấp cho Docker khoảng 10GB trở lên |
| Disk trống | Tối thiểu 20GB |
| Python | 3.10+ để chạy data generator trên host |
| OS | Windows 10/11, macOS hoặc Linux |

Các service local sau sẽ được mở sau khi stack chạy:

| Service | URL |
|---|---|
| Airflow | <http://localhost:8080> |
| JupyterLab | <http://localhost:8888> |
| Spark Master UI | <http://localhost:9090> |
| MinIO Console | <http://localhost:9001> |
| Trino HTTPS | <https://localhost:8085> |
| Superset, tùy chọn | <http://localhost:8088> |

## 2. Chuẩn bị cấu hình local

Khi nộp hoặc clone source code, repository chỉ nên có file mẫu
`docker/.env.example` và `data_generator/config.example.yaml`. Hai file runtime
`docker/.env` và `data_generator/config.yaml` bị Git ignore vì chứa password,
secret và cấu hình local.

Xử lý theo tình huống:

- Nếu đã có `docker/.env` hoặc `data_generator/config.yaml` trên máy local, giữ
  nguyên file đó và chỉ kiểm tra lại giá trị bên trong.
- Nếu chưa có, tạo file runtime từ file mẫu bằng các lệnh bên dưới.
- Không commit `docker/.env` và `data_generator/config.yaml` lên Git.

Windows PowerShell:

```powershell
if (-not (Test-Path docker\.env)) {
  Copy-Item docker\.env.example docker\.env
}

if (-not (Test-Path data_generator\config.yaml)) {
  Copy-Item data_generator\config.example.yaml data_generator\config.yaml
}
```

macOS/Linux:

```bash
cp -n docker/.env.example docker/.env
cp -n data_generator/config.example.yaml data_generator/config.yaml
```

Mở `docker/.env` và thay toàn bộ `CHANGE_ME` bằng giá trị local đủ mạnh. Các biến
bắt buộc cần chú ý:

- `ORACLE_PASSWORD`
- `POSTGRES_PASSWORD`
- `MINIO_ROOT_PASSWORD`
- `TRINO_KEYSTORE_PASSWORD`
- `TRINO_SHARED_SECRET`
- `TRINO_MARKETING_PASSWORD`
- `TRINO_ENGINEERING_PASSWORD`
- `SUPERSET_SECRET_KEY`
- `SUPERSET_ADMIN_PASSWORD`
- `AIRFLOW_FERNET_KEY`
- `AIRFLOW_SECRET_KEY`
- `AIRFLOW_ADMIN_PASSWORD`
- `PII_HASH_SALT`

`AIRFLOW_FERNET_KEY` phải đúng định dạng Fernet key. Có thể tạo bằng:

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Nếu máy host chưa có `cryptography`, có thể tạo bằng Docker:

```bash
docker run --rm apache/airflow:2.10.0 python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Mở `data_generator/config.yaml` và cập nhật password cho khớp với `docker/.env`:

```yaml
cob_dt: "2025-12-31"

oracle:
  host: "localhost"
  port: 1521
  service: "XEPDB1"
  user: "core_banking"
  password: "<ORACLE_PASSWORD trong docker/.env>"

postgres:
  host: "localhost"
  port: 5432
  dbname: "card_crm"
  user: "card_crm"
  password: "<POSTGRES_PASSWORD trong docker/.env>"
```

Giữ `cob_dt: "2025-12-31"` cho dữ liệu baseline demo.

## 3. Build và khởi động Docker stack

```powershell
docker compose -f docker/docker-compose.yml build
docker compose -f docker/docker-compose.yml config -q
docker compose -f docker/docker-compose.yml up -d
docker compose -f docker/docker-compose.yml ps
```

Chờ Oracle sẵn sàng:

```powershell
docker compose -f docker/docker-compose.yml logs -f oracle
```

Khi log có `DATABASE IS READY TO USE!`, mở terminal khác và kiểm tra Airflow:

```powershell
docker compose -f docker/docker-compose.yml logs --tail 100 airflow-webserver
docker compose -f docker/docker-compose.yml logs --tail 100 airflow-scheduler
```

## 4. Tạo Airflow connections

Chạy một lần sau khi `airflow-webserver` và `airflow-scheduler` đã lên:

```powershell
docker exec airflow-scheduler bash -lc "sed -i 's/\r$//' /opt/project/airflow/setup_connections.sh && /opt/project/airflow/setup_connections.sh"
```

Kiểm tra DAG import:

```powershell
docker exec airflow-scheduler airflow dags list-import-errors --output json
```

Kỳ vọng output là `[]`.

## 5. Sinh dữ liệu nguồn

Data generator chạy trên máy host và ghi dữ liệu vào Oracle/PostgreSQL đang chạy
trong Docker.

Windows PowerShell:

```powershell
cd data_generator
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python run_sql_gen.py
cd ..
```

macOS/Linux:

```bash
cd data_generator
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python run_sql_gen.py
cd ..
```

Output mong đợi có dạng:

```text
cob_dt=2025-12-31  customers=10,000  seed=42

[1/2] Oracle - core_banking (oracle_gen.sql)...
      Done.

[2/2] PostgreSQL - card_crm (postgres_gen.sql)...
      Done.
```

## 6. Khởi tạo Iceberg tables

DAG này drop/create lại toàn bộ bảng Iceberg. Chỉ chạy khi fresh start hoặc khi
muốn reset Lakehouse local.

Windows PowerShell:

```powershell
$stamp = Get-Date -Format "yyyyMMddTHHmmss"
docker exec airflow-scheduler airflow dags unpause implement_iceberg_table_dag *> $null
docker exec airflow-scheduler airflow dags trigger `
  -r "manual__init_tables_$stamp" `
  -c '{\"layer\":\"all\"}' `
  implement_iceberg_table_dag
```

Theo dõi trên Airflow UI hoặc CLI:

```powershell
docker exec airflow-scheduler airflow dags list-runs `
  -d implement_iceberg_table_dag `
  --output table
```

Chỉ sang bước tiếp theo khi run `implement_iceberg_table_dag` có state `success`.

## 7. Bootstrap baseline ngày 2025-12-31

Baseline tạo snapshot đầu tiên cho Bronze/Silver/Gold, đồng thời load lịch sử
fact một năm để tính RFM, churn và Customer 360.

Tạo `pipeline_run_id` dùng chung cho toàn bộ baseline:

```powershell
$stamp = Get-Date -Format "yyyyMMddTHHmmss"
$baselinePipelineRunId = "bootstrap_20251231_$stamp"
$baselineConf = '{\"cob_dt\":\"2025-12-31\",\"pipeline_run_id\":\"' + $baselinePipelineRunId + '\"}'
Write-Host "baseline pipeline_run_id=$baselinePipelineRunId"
```

Unpause các DAG cần chạy thủ công:

```powershell
$bootstrapDags = @(
  "bronze_core_banking_dag",
  "bronze_card_crm_dag",
  "bronze_initial_dag",
  "silver_all_dag",
  "silver_initial_dag",
  "gold_mart360_dag",
  "gold_time_analytics_dag",
  "gold_segmentation_dag",
  "ops_pii_masking_daily_dag",
  "ops_dq_daily_dag"
)

foreach ($dag in $bootstrapDags) {
  docker exec airflow-scheduler airflow dags unpause $dag *> $null
}
```

Chạy Bronze daily cho source snapshot:

```powershell
docker exec airflow-scheduler airflow dags trigger `
  -r "manual__${baselinePipelineRunId}_bronze_core" `
  -c $baselineConf `
  bronze_core_banking_dag

docker exec airflow-scheduler airflow dags trigger `
  -r "manual__${baselinePipelineRunId}_bronze_card" `
  -c $baselineConf `
  bronze_card_crm_dag
```

Chờ hai DAG Bronze success, rồi chạy Bronze initial cho lịch sử fact:

```powershell
docker exec airflow-scheduler airflow dags trigger `
  -r "manual__${baselinePipelineRunId}_bronze_initial" `
  -c $baselineConf `
  bronze_initial_dag
```

Chờ `bronze_initial_dag` success, rồi chạy Silver daily và Silver initial facts:

```powershell
docker exec airflow-scheduler airflow dags trigger `
  -r "manual__${baselinePipelineRunId}_silver_all" `
  -c $baselineConf `
  silver_all_dag
```

Chờ `silver_all_dag` success:

```powershell
docker exec airflow-scheduler airflow dags trigger `
  -r "manual__${baselinePipelineRunId}_silver_initial" `
  -c $baselineConf `
  silver_initial_dag
```

Chờ `silver_initial_dag` success, rồi publish Gold:

```powershell
docker exec airflow-scheduler airflow dags trigger `
  -r "manual__${baselinePipelineRunId}_gold_mart360" `
  -c $baselineConf `
  gold_mart360_dag

docker exec airflow-scheduler airflow dags trigger `
  -r "manual__${baselinePipelineRunId}_gold_time" `
  -c $baselineConf `
  gold_time_analytics_dag
```

Chờ `gold_mart360_dag` và `gold_time_analytics_dag` success:

```powershell
docker exec airflow-scheduler airflow dags trigger `
  -r "manual__${baselinePipelineRunId}_gold_segmentation" `
  -c $baselineConf `
  gold_segmentation_dag
```

Chờ `gold_segmentation_dag` success, rồi chạy masking/dashboard serving và DQ:

```powershell
docker exec airflow-scheduler airflow dags trigger `
  -r "manual__${baselinePipelineRunId}_masking" `
  -c $baselineConf `
  ops_pii_masking_daily_dag

docker exec airflow-scheduler airflow dags trigger `
  -r "manual__${baselinePipelineRunId}_dq" `
  -c $baselineConf `
  ops_dq_daily_dag
```

Chờ `ops_pii_masking_daily_dag` và `ops_dq_daily_dag` success.

## 8. Chạy demo day-2 ngày 2026-01-01

Bước này được khuyến nghị nếu muốn thấy SCD2 history có version cũ/mới và Gold
history có hai ngày dữ liệu.

Áp dụng thay đổi có kiểm soát vào Oracle:

```powershell
Get-Content -Raw data_generator\sql_generators\scd2_demo_changes_oracle.sql |
  docker exec -i oracle sqlplus -s / as sysdba
```

Tạo `pipeline_run_id` cho day-2:

```powershell
$stamp = Get-Date -Format "yyyyMMddTHHmmss"
$pipelineRunId = "daily_20260101_$stamp"
$dailyConf = '{\"cob_dt\":\"2026-01-01\",\"pipeline_run_id\":\"' + $pipelineRunId + '\"}'
Write-Host "pipeline_run_id=$pipelineRunId"
```

Chạy các child DAG theo đúng thứ tự. Giữ cùng `$dailyConf` cho mọi DAG trong run.

```powershell
docker exec airflow-scheduler airflow dags trigger `
  -r "manual__${pipelineRunId}_bronze_core" `
  -c $dailyConf `
  bronze_core_banking_dag

docker exec airflow-scheduler airflow dags trigger `
  -r "manual__${pipelineRunId}_bronze_card" `
  -c $dailyConf `
  bronze_card_crm_dag
```

Chờ hai Bronze DAG success:

```powershell
docker exec airflow-scheduler airflow dags trigger `
  -r "manual__${pipelineRunId}_silver_all" `
  -c $dailyConf `
  silver_all_dag
```

Chờ `silver_all_dag` success:

```powershell
docker exec airflow-scheduler airflow dags trigger `
  -r "manual__${pipelineRunId}_gold_mart360" `
  -c $dailyConf `
  gold_mart360_dag

docker exec airflow-scheduler airflow dags trigger `
  -r "manual__${pipelineRunId}_gold_time" `
  -c $dailyConf `
  gold_time_analytics_dag
```

Chờ hai Gold DAG success:

```powershell
docker exec airflow-scheduler airflow dags trigger `
  -r "manual__${pipelineRunId}_gold_segmentation" `
  -c $dailyConf `
  gold_segmentation_dag
```

Chờ `gold_segmentation_dag` success:

```powershell
docker exec airflow-scheduler airflow dags trigger `
  -r "manual__${pipelineRunId}_masking" `
  -c $dailyConf `
  ops_pii_masking_daily_dag
```

Chờ `ops_pii_masking_daily_dag` success:

```powershell
docker exec airflow-scheduler airflow dags trigger `
  -r "manual__${pipelineRunId}_dq" `
  -c $dailyConf `
  ops_dq_daily_dag
```

Luồng xử lý tương đương daily master DAG:

```text
Bronze Core + Bronze Card/CRM
  -> Silver
  -> Gold Mart360 + Gold Time Analytics
  -> Gold Segmentation/NBO
  -> PII Masking/Dashboard serving
  -> Data Quality
```

Khi chạy production/scheduled thật, có thể dùng `lakehouse_daily_pipeline_dag` để
điều phối các child DAG. Với local demo từ dữ liệu seed, cách trigger child DAG
thủ công giúp tránh Airflow tự tạo scheduled run theo ngày hiện tại.

## 9. Kiểm tra kết quả

### 9.1. Kiểm tra Airflow runs

```powershell
$dailyDags = @(
  "bronze_core_banking_dag",
  "bronze_card_crm_dag",
  "silver_all_dag",
  "gold_mart360_dag",
  "gold_time_analytics_dag",
  "gold_segmentation_dag",
  "ops_pii_masking_daily_dag",
  "ops_dq_daily_dag"
)

foreach ($dag in $dailyDags) {
  docker exec airflow-scheduler airflow dags list-runs -d $dag --output table
}
```

Kỳ vọng: các DAG thuộc `$pipelineRunId` đều `success`. Nếu bạn chỉ chạy baseline,
dùng `$baselinePipelineRunId` khi kiểm tra các query bên dưới.

### 9.2. Kiểm tra ETL flags

```powershell
$runToCheck = $pipelineRunId
$cobDtToCheck = "2026-01-01"

$flagSql = @"
WITH expected(job_name) AS (
  VALUES
    ('bronze_core_banking_dag'),
    ('bronze_card_crm_dag'),
    ('silver_all_dag'),
    ('gold_mart360_dag'),
    ('gold_time_analytics_dag'),
    ('gold_segmentation_dag'),
    ('ops_pii_masking_daily_dag'),
    ('ops_dq_daily_dag')
),
latest AS (
  SELECT DISTINCT ON (f.job_name)
         f.job_name, f.status, f.cob_dt, f.pipeline_run_id, f.error_detail
  FROM opslakehouse.flag_job_etl f
  JOIN expected e ON e.job_name = f.job_name
  WHERE f.pipeline_run_id = '$runToCheck'
    AND f.cob_dt = DATE '$cobDtToCheck'
  ORDER BY f.job_name, f.id DESC
)
SELECT e.job_name, l.status, l.cob_dt, l.pipeline_run_id, l.error_detail
FROM expected e
LEFT JOIN latest l ON l.job_name = e.job_name
ORDER BY e.job_name;
"@

$flagSql | docker exec -i postgres /bin/sh -c `
  'PGPASSWORD="$POSTGRES_PASSWORD" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"'
```

Kỳ vọng: 8 dòng, tất cả `status = S`, `error_detail` rỗng.

### 9.3. Kiểm tra Data Quality

```powershell
$dqSql = @"
WITH latest_run AS (
  SELECT dq_run_id
  FROM opslakehouse.dq_check_result
  WHERE pipeline_run_id = '$runToCheck'
    AND cob_dt = DATE '$cobDtToCheck'
  ORDER BY executed_at DESC
  LIMIT 1
)
SELECT COUNT(*) AS total_checks,
       COUNT(*) FILTER (WHERE passed) AS passed_checks,
       COUNT(*) FILTER (WHERE NOT passed) AS failed_checks,
       COUNT(*) FILTER (WHERE severity = 'critical') AS critical_checks,
       COUNT(*) FILTER (WHERE severity = 'warning') AS warning_checks
FROM opslakehouse.dq_check_result
WHERE dq_run_id = (SELECT dq_run_id FROM latest_run);
"@

$dqSql | docker exec -i postgres /bin/sh -c `
  'PGPASSWORD="$POSTGRES_PASSWORD" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"'
```

Kỳ vọng ở release hiện tại: `19` checks, `19` pass, `0` fail.

### 9.4. Kiểm tra Customer 360 qua Trino

```powershell
@'
SELECT 'current' AS dataset,
       COUNT(*) AS rows,
       COUNT(DISTINCT customer_id) AS customers,
       MIN(cob_dt) AS min_cob_dt,
       MAX(cob_dt) AS max_cob_dt
FROM lakehouse.gold.mart_customer_360
UNION ALL
SELECT 'history',
       COUNT(*),
       COUNT(DISTINCT customer_id),
       MIN(cob_dt),
       MAX(cob_dt)
FROM lakehouse.gold.mart_customer_360_history;
'@ | docker exec -i trino /bin/sh -c `
  'TRINO_PASSWORD="$TRINO_ENGINEERING_PASSWORD" trino --server https://localhost:8443 --insecure --user data_engineer --password --output-format TSV'
```

Nếu đã chạy đủ baseline + day-2, kỳ vọng:

- `mart_customer_360`: 10,000 rows, 10,000 customers, ngày `2026-01-01`.
- `mart_customer_360_history`: 20,000 rows, 10,000 customers, từ `2025-12-31` đến
  `2026-01-01`.

Nếu chỉ chạy baseline, `current` và `history` đều ở ngày `2025-12-31`.

## 10. Chạy acceptance SQL và use cases

Chạy acceptance SQL:

```powershell
$acceptanceFiles = @(
  "sql_templates/trino/07_scd2_acceptance_checks.sql",
  "sql_templates/trino/08_gold_customer360_acceptance_checks.sql",
  "sql_templates/trino/09_daily_pipeline_dq_acceptance.sql",
  "sql_templates/trino/10_nbo_security_acceptance.sql",
  "sql_templates/trino/11_dashboard_serving_acceptance.sql"
)

foreach ($file in $acceptanceFiles) {
  Write-Host "Running $file"
  Get-Content -Raw $file | docker exec -i trino /bin/sh -c `
    'TRINO_PASSWORD="$TRINO_ENGINEERING_PASSWORD" trino --server https://localhost:8443 --insecure --user data_engineer --password'
  if ($LASTEXITCODE -ne 0) {
    throw "Trino acceptance failed: $file"
  }
}
```

Chạy sáu SQL use case nghiệp vụ:

```powershell
$businessFiles = Get-ChildItem sql_templates/trino -File |
  Where-Object Name -Match '^0[1-6]_' |
  Sort-Object Name

foreach ($file in $businessFiles) {
  Write-Host "Running $($file.Name)"
  Get-Content -Raw $file.FullName | docker exec -i trino /bin/sh -c `
    'TRINO_PASSWORD="$TRINO_ENGINEERING_PASSWORD" trino --server https://localhost:8443 --insecure --user data_engineer --password'
  if ($LASTEXITCODE -ne 0) {
    throw "Trino business query failed: $($file.Name)"
  }
}
```

## 11. JupyterLab và notebook acceptance

JupyterLab chạy cùng stack mặc định tại <http://localhost:8888>. Không chạy notebook
Spark đồng thời với Airflow Spark jobs vì local stack chỉ có một Spark worker nhỏ.

Chạy notebook acceptance tự động:

```powershell
$notebooks = @(
  "01_scd2_baseline_preflight.ipynb",
  "03_customer360_gold_acceptance.ipynb",
  "05_nbo_security_acceptance.ipynb",
  "06_dashboard_data_acceptance.ipynb"
)

foreach ($nb in $notebooks) {
  docker exec jupyter jupyter nbconvert `
    --to notebook `
    --execute `
    --inplace `
    --ExecutePreprocessor.timeout=1800 `
    "/opt/notebooks/$nb"
}
```

Notebook `02_scd2_day2_acceptance.ipynb` có phần idempotency rerun nên nên chạy thủ
công trong Jupyter sau khi đã hoàn tất day-2.

## 12. Superset dashboard tùy chọn

Superset nằm trong profile `bi`, không bắt buộc để chạy ETL.

```powershell
docker compose -f docker/docker-compose.yml --profile bi up -d --build superset
```

Mở <http://localhost:8088>. Tài khoản lấy từ `SUPERSET_ADMIN_USER` và
`SUPERSET_ADMIN_PASSWORD` trong `docker/.env`.

Sau khi pipeline đã publish `lakehouse.sandbox.mart_customer_360_dashboard`, tạo
dashboard mẫu:

```powershell
docker exec superset bash -lc ". /app/.venv/bin/activate && python /app/docker/create_customer360_dashboard.py"
```

Script này tạo/cập nhật dashboard `Customer 360 Lakehouse` không có native filter. Dashboard dùng trực tiếp
serving mart `lakehouse.sandbox.mart_customer_360_dashboard` do pipeline trong repo publish cho snapshot demo
`2026-01-01`; các CSV đã export gọn để đối chiếu nằm tại `exports/trino_csv/`.

Các chart chính gồm KPI cards, RFM Segment Distribution, AUM by Branch, Recommended Product Mix,
Campaign Priority, Churn Risk, Cross-sell Score Distribution, Product Penetration và Customer Drill-down.
KPI cards dùng CSS theo chart id thực tế nên icon vẫn hiện sau khi script upsert lại chart.

Smoke test dataset Superset:

```powershell
docker exec superset bash -lc ". /app/.venv/bin/activate && python /app/docker/check_customer360_queries.py"
```

Xem thêm hướng dẫn dashboard tại `docker/superset/README.md`.

## 13. Reset môi trường local

Reset toàn bộ containers và Docker volumes:

```powershell
docker compose -f docker/docker-compose.yml down -v
```

Lệnh này xóa dữ liệu local của Oracle, PostgreSQL, MinIO/Iceberg và Airflow
metadata. Sau đó chạy lại từ bước build/up, sinh dữ liệu, khởi tạo Iceberg và
bootstrap baseline.

Nếu chỉ muốn tắt stack nhưng giữ dữ liệu:

```powershell
docker compose -f docker/docker-compose.yml down
```

## 14. Ghi chú cho macOS/Linux

Các lệnh Docker giống nhau. Khác biệt chính là cú pháp biến và JSON:

```bash
stamp=$(date +%Y%m%dT%H%M%S)
pipeline_run_id="daily_20260101_$stamp"
daily_conf="{\"cob_dt\":\"2026-01-01\",\"pipeline_run_id\":\"$pipeline_run_id\"}"

docker exec airflow-scheduler airflow dags trigger \
  -r "manual__${pipeline_run_id}_silver_all" \
  --conf "$daily_conf" \
  silver_all_dag
```

Với Bash, JSON literal đơn giản cũng dùng được:

```bash
docker exec airflow-scheduler airflow dags trigger \
  -r "manual__init_tables_$(date +%Y%m%dT%H%M%S)" \
  --conf '{"layer":"all"}' \
  implement_iceberg_table_dag
```

## 15. Troubleshooting nhanh

- `docker compose ... config -q` fail: kiểm tra lại `docker/.env`, thường là thiếu
  biến hoặc có ký tự quote không mong muốn.
- Trino container exit sớm: đảm bảo các biến `TRINO_*` không còn là `CHANGE_ME`.
- Airflow không import DAG: chạy `airflow dags list-import-errors --output json` và
  đọc log `airflow-scheduler`.
- DAG Silver chờ mãi ở sensor: kiểm tra Bronze cùng `pipeline_run_id` và `cob_dt`
  đã `success` chưa.
- DQ fail: xem bảng `opslakehouse.dq_check_result` để biết check nào fail.
- DBeaver/SQL client không kết nối Trino: dùng HTTPS, port `8085`, bật SSL và
  trust/ignore certificate validation vì cert local là self-signed.
