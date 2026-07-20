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
   - Chứng minh thay đổi riêng `last_updated` không tạo version.
   - Chụp fingerprint trước và sau rerun để chứng minh idempotency.

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
