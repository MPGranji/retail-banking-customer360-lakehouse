# Lakehouse notebooks

Các notebook trong thư mục này là runbook thực hành cho môi trường Docker Compose local.
JupyterLab mount thư mục này vào `/opt/notebooks`, vì vậy file được lưu bền vững trên máy host.

Mở [http://localhost:8888](http://localhost:8888) và chọn kernel **PySpark (Lakehouse)**.

## Thứ tự chạy SCD2 history lab

1. `01_scd2_baseline_preflight.ipynb`
   - Kiểm tra Spark/Iceberg/MinIO.
   - Chụp counts và khoảng ngày của Bronze.
   - Kiểm tra schema bốn dimension.
   - Nghiệm thu baseline sau khi load `2025-12-31`.
2. Trigger hai Bronze DAG với Param `cob_dt=2026-01-01` sau khi áp dụng controlled changes.
3. Trigger `silver_all_dag` với cùng `cob_dt=2026-01-01`.
4. `02_scd2_day2_acceptance.ipynb`
   - Viết theo hướng SQL-first: logic nghiệm thu nằm trong Spark SQL; Python chỉ gọi query và dừng khi có `FAIL`.
   - Kiểm tra duplicate current row/SK, date range và overlap.
   - Kiểm tra version cũ/mới của bốn test keys.
   - Kiểm tra thay đổi riêng `last_updated` không tạo version.
   - Chụp fingerprint trước và sau rerun để kiểm tra idempotency.

## Nghiệm thu Customer 360 Gold

Sau khi chạy Gold cho hai ngày theo thứ tự `gold_mart360_dag` →
`gold_segmentation_dag` → `gold_time_analytics_dag` →
`ops_pii_masking_daily_dag`, mở `03_customer360_gold_acceptance.ipynb`.

Notebook dùng Spark SQL để kiểm tra:

- current mart chỉ có snapshot mới nhất và duy nhất theo `customer_id`;
- history giữ đủ hai ngày và duy nhất theo `(customer_id, cob_dt)`;
- AUM/card KPI khớp aggregate độc lập từ Silver facts;
- RFM, churn, cross-sell và campaign có population hợp lệ;
- masked mart không lộ các cột PII gốc;
- lịch sử Customer 360 của test customer phản ánh thay đổi SCD2.

Kết quả thực thi được lưu trực tiếp trong notebook. File SQL tương đương cho
Trino là `sql_templates/trino/08_gold_customer360_acceptance_checks.sql`.

## Nghiệm thu daily pipeline và Data Quality

Sau khi trigger `lakehouse_daily_pipeline_dag` với cùng `cob_dt` và
`pipeline_run_id` cho toàn pipeline, mở `04_daily_pipeline_dq_acceptance.ipynb`.

Notebook kiểm tra:

- trạng thái mới nhất của master và tám child DAG theo đúng `pipeline_run_id` đều là `S`;
- đủ 14 kết quả DQ đã được lưu vào PostgreSQL và không có critical failure;
- ba bảng fact không mất lịch sử, không trùng business key và không có orphan SK;
- current mart, masked mart và bốn bảng marketing có cùng population theo ngày.

Kết quả thực thi được lưu trực tiếp trong notebook. Bộ query độc lập cho Trino
là `sql_templates/trino/09_daily_pipeline_dq_acceptance.sql`.

## Nghiệm thu Next Best Offer và Trino Security

Sau khi migration NBO và `gold_segmentation_dag`/`ops_dq_daily_dag` thành công cho cùng ngày,
chạy `05_nbo_security_acceptance.ipynb`. Notebook dùng Spark SQL để kiểm tra grain, score,
reason, product, priority và suppression; sau đó gọi Trino HTTPS để xác nhận anonymous bị 401,
Marketing chỉ đọc sandbox và Data Engineering đọc được raw layer. Password chỉ đọc từ environment,
không được ghi vào cell hoặc output.

Bộ query Trino tương ứng là `sql_templates/trino/10_nbo_security_acceptance.sql`.

## Nghiệm thu dashboard serving layer

Sau khi `gold_segmentation_dag` và `ops_pii_masking_daily_dag` thành công cho
`cob_dt=2026-01-01`, chạy `06_dashboard_data_acceptance.ipynb`. Notebook kiểm tra
grain `(customer_id, cob_dt)`, population, NBO contract, schema không có raw PII
và quyền của user Marketing trên bảng
`sandbox.mart_customer_360_dashboard`. Output được lưu trực tiếp trong notebook.

Bộ query Trino tương ứng là
`sql_templates/trino/11_dashboard_serving_acceptance.sql`.

## Controlled Oracle changes

Từ PowerShell tại project root:

```powershell
Get-Content -Raw data_generator\sql_generators\scd2_demo_changes_oracle.sql |
  docker exec -i oracle sqlplus -s / as sysdba
```

Sau khi nghiệm thu, có thể khôi phục source demo:

```powershell
Get-Content -Raw data_generator\sql_generators\scd2_demo_rollback_oracle.sql |
  docker exec -i oracle sqlplus -s / as sysdba
```

Không đặt mật khẩu hoặc secret trong notebook. Không chạy reset Silver/Gold từ notebook; migration destructive phải đi qua Airflow DAG và checkpoint riêng.
