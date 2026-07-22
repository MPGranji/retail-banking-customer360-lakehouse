# Superset Dashboard Guide

Superset is an optional BI profile for the Customer 360 Lakehouse stack. It connects to Trino with the existing `marketing` user, so charts can read only `lakehouse.sandbox`.

## Start Superset

```bash
cd lakehouse_etl/docker
docker compose --profile bi up -d --build superset
```

Open Superset at <http://localhost:8088>.

Default local login, unless overridden in `docker/.env`:

```text
username: admin
password: admin
```

The bootstrap imports one database connection and one dataset:

```text
Database: Customer 360 Lakehouse - Trino Sandbox
Dataset : sandbox.mart_customer_360_dashboard
```

## Auto-create the dashboard

After the daily pipeline has published `lakehouse.sandbox.mart_customer_360_dashboard`, run:

```bash
docker exec superset bash -lc ". /app/.venv/bin/activate && python /app/docker/create_customer360_dashboard.py"
```

The script upserts the dashboard `Customer 360 Lakehouse` end-to-end:

- creates dynamic Superset virtual datasets over `mart_customer_360_dashboard`;
- creates/updates all charts, metrics, layout, colors, CSS, KPI cards, and product penetration logic;
- applies KPI card icon CSS with dashboard-specific chart ids;
- keeps the dashboard filter-free and sets the default time range to `2026-01-01 : 2026-01-02` without hard-coding that date inside chart SQL.

The dashboard reads the serving mart published by this repo's pipeline:

```text
lakehouse.sandbox.mart_customer_360_dashboard
```

Compact Trino export files used for reconciliation and screenshots are kept in:

```text
exports/trino_csv/
```

Those CSV files are reference outputs, not a replacement for the 10k-row dashboard serving mart.

Dashboard layout:

```text
Row 1: Total Customers | Total AUM | Active 30d | Campaign Eligible
Row 2: RFM Segment Distribution | AUM by Branch
Row 3: Recommended Product Mix | Campaign Priority | Churn Risk
Row 4: Cross-sell Score Distribution | Product Penetration
Row 5: Customer Drill-down
```

Smoke-test the Trino/Sandbox data used by the charts:

```bash
docker exec superset bash -lc ". /app/.venv/bin/activate && python /app/docker/check_customer360_queries.py"
```

If the import warning appears in logs, create the connection manually:

```text
SQLAlchemy URI:
trino://marketing:<TRINO_MARKETING_PASSWORD>@trino:8443/lakehouse/sandbox

Advanced > Other > ENGINE PARAMETERS:
{"connect_args":{"http_scheme":"https","verify":false}}
```

`verify=false` is only for the local self-signed Trino certificate.

## Dashboard contents

The dashboard has no native filters. It is a demo snapshot for `2026-01-01`, based on the sandbox serving mart above.

### Row 1 - KPI Cards

Four `Big Number` charts:

| Chart | Metric |
|---|---|
| Total Customers | `customer_count` |
| Total AUM | `total_aum` |
| Active 30d | `active_30d_customers` |
| Campaign Eligible | `eligible_customers` |

### Row 2 - Segmentation

| Chart | Visualization | Setup |
|---|---|---|
| RFM Segment Distribution | Bar Chart | Dimension `rfm_segment`, metric `customer_count`, sort descending |
| AUM by Branch | Horizontal Bar Chart | Dimension `primary_branch_code`, metric `total_aum`, row limit 10 |

### Row 3 - Campaign

| Chart | Visualization | Setup |
|---|---|---|
| Recommended Product Mix | Bar Chart | Dimension `recommended_product_name`, metric `customer_count` |
| Campaign Priority | Donut Chart | Dimension `campaign_priority`, metric `customer_count` |
| Churn Risk | Bar Chart | Dimension `churn_risk_bucket`, metric `customer_count` |

### Row 4 - Cross-Sell

| Chart | Visualization | Setup |
|---|---|---|
| Cross-sell Score Distribution | Bar Chart | Dimension `score_bucket`, metric `customer_count` |
| Product Penetration | Grouped Bar Chart | Dynamic unpivot dataset, metrics `Have Product` and `Eligible but Don't Have` |

### Row 5 - Customer drill-down

The drill-down table includes:

```text
customer_id
full_name_masked
customer_segment
aum_bucket
rfm_segment
recommended_product
campaign_priority
cross_sell_score
```

Do not add raw PII fields. The serving dataset should not expose `full_name`, `phone`, `email`, `cccd`, or `address`.

## Acceptance checks

Run in Superset SQL Lab:

```sql
SELECT COUNT(*) AS rows,
       COUNT(DISTINCT customer_id) AS customers
FROM lakehouse.sandbox.mart_customer_360_dashboard
WHERE cob_dt = DATE '2026-01-01';
```

Expected:

```text
rows = 10000
customers = 10000
```

RBAC smoke test:

```sql
SELECT COUNT(*) FROM lakehouse.gold.campaign_target;
```

The query should fail for the Superset/marketing connection.
