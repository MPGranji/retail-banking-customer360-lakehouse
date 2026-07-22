import json
import uuid
from datetime import datetime

from superset.app import create_app


DASHBOARD_TITLE = "Customer 360 Lakehouse"
GRID_ID = "GRID_ID"
DASHBOARD_TABS_ID = "TABS-CUSTOMER-360"
DASHBOARD_TAB_ID = "TAB-CUSTOMER-360-OVERVIEW"
DATASET_SCHEMA = "sandbox"
DATASET_TABLE = "mart_customer_360_dashboard"
SNAPSHOT_TABLE = "v_customer_360_dashboard_snapshot"
PRODUCT_PENETRATION_TABLE = "v_customer_360_product_penetration"
SNAPSHOT_DATE = "2026-01-01"
SNAPSHOT_END_DATE = "2026-01-02"
DEFAULT_TIME_RANGE = f"{SNAPSHOT_DATE} : {SNAPSHOT_END_DATE}"
RECOMMENDED_PRODUCT_NAME_SQL = (
    "CASE recommended_product "
    "WHEN 'CREDIT_CARD' THEN 'Credit Card' "
    "WHEN 'DIGITAL_SAVINGS' THEN 'Digital Savings' "
    "WHEN 'PERSONAL_LOAN' THEN 'Personal Loan' "
    "WHEN 'TERM_DEPOSIT' THEN 'Term Deposit' "
    "WHEN 'WEALTH_MANAGEMENT' THEN 'Wealth Management' "
    "ELSE REPLACE(recommended_product, '_', ' ') END"
)
CHURN_RISK_BUCKET_SQL = (
    "CASE WHEN days_since_last_txn <= 7 THEN 'Very Low' "
    "WHEN days_since_last_txn <= 30 THEN 'Low' "
    "WHEN days_since_last_txn <= 60 THEN 'Medium' "
    "WHEN days_since_last_txn <= 90 THEN 'High' "
    "ELSE 'Very High' END"
)
CROSS_SELL_SCORE_BUCKET_SQL = (
    "CASE WHEN cross_sell_score >= 90 THEN '90-100' "
    "ELSE CAST(FLOOR(cross_sell_score / 10) * 10 AS VARCHAR) || '-' || "
    "CAST(FLOOR(cross_sell_score / 10) * 10 + 9 AS VARCHAR) END"
)
METRIC_COLOR_LABELS = {
    "customer_count": ["customer_count", "Customer count"],
    "total_aum": ["total_aum", "Total AUM"],
    "rfm_customer_count": ["rfm_customer_count", "RFM Customers"],
    "branch_total_aum": ["branch_total_aum", "Branch AUM"],
    "product_mix_customers": ["product_mix_customers", "Product Mix Customers"],
    "churn_risk_customers": ["churn_risk_customers", "Churn Risk Customers"],
    "score_distribution_customers": [
        "score_distribution_customers",
        "Score Distribution Customers",
    ],
    "has_product_customers": ["has_product_customers", "Have Product"],
    "eligible_without_product_customers": [
        "eligible_without_product_customers",
        "Eligible but Don't Have",
    ],
}
CHART_COLOR_METRICS = [
    ("rfm_customer_count", "COUNT(DISTINCT customer_id)", "RFM Customers"),
    ("branch_total_aum", "SUM(aum_total)", "Branch AUM"),
    ("product_mix_customers", "COUNT(DISTINCT customer_id)", "Product Mix Customers"),
    ("churn_risk_customers", "COUNT(DISTINCT customer_id)", "Churn Risk Customers"),
    ("score_distribution_customers", "COUNT(DISTINCT customer_id)", "Score Distribution Customers"),
]
SNAPSHOT_SQL = f"""
SELECT
    *,
    {RECOMMENDED_PRODUCT_NAME_SQL} AS recommended_product_name,
    {CHURN_RISK_BUCKET_SQL} AS churn_risk_bucket,
    {CROSS_SELL_SCORE_BUCKET_SQL} AS score_bucket
FROM {DATASET_SCHEMA}.{DATASET_TABLE}
"""
PRODUCT_PENETRATION_SQL = f"""
SELECT
    cob_dt,
    primary_branch_code,
    customer_segment,
    rfm_segment,
    campaign_priority,
    recommended_product,
    {RECOMMENDED_PRODUCT_NAME_SQL} AS recommended_product_name,
    'Credit Card' AS product,
    has_credit_card AS has_product_customers,
    no_credit_card AS eligible_without_product_customers
FROM {DATASET_SCHEMA}.{DATASET_TABLE}
UNION ALL
SELECT
    cob_dt,
    primary_branch_code,
    customer_segment,
    rfm_segment,
    campaign_priority,
    recommended_product,
    {RECOMMENDED_PRODUCT_NAME_SQL} AS recommended_product_name,
    'Savings' AS product,
    has_savings AS has_product_customers,
    no_deposit AS eligible_without_product_customers
FROM {DATASET_SCHEMA}.{DATASET_TABLE}
UNION ALL
SELECT
    cob_dt,
    primary_branch_code,
    customer_segment,
    rfm_segment,
    campaign_priority,
    recommended_product,
    {RECOMMENDED_PRODUCT_NAME_SQL} AS recommended_product_name,
    'Loan' AS product,
    has_loan AS has_product_customers,
    no_loan AS eligible_without_product_customers
FROM {DATASET_SCHEMA}.{DATASET_TABLE}
"""


def dumps(value: dict) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def base_query(datasource_id: int, *, filters: list[dict] | None = None) -> dict:
    return {
        "datasource": {"id": datasource_id, "type": "table"},
        "force": False,
        "queries": [
            {
                "filters": filters or [],
                "extras": {"having": "", "where": ""},
                "applied_time_extras": {},
                "columns": [],
                "metrics": [],
                "annotation_layers": [],
                "series_limit": 0,
                "group_others_when_limit_reached": False,
                "order_desc": True,
                "url_params": {},
                "custom_params": {},
                "custom_form_data": {},
            }
        ],
        "result_format": "json",
        "result_type": "full",
    }


def big_number_form(datasource_id: int, dashboard_id: int, name: str, metric: str, subtitle: str) -> dict:
    return {
        "datasource": f"{datasource_id}__table",
        "viz_type": "big_number_total",
        "metric": metric,
        "adhoc_filters": [],
        "granularity_sqla": "cob_dt",
        "time_range": DEFAULT_TIME_RANGE,
        "header_font_size": 0.44,
        "subtitle": subtitle,
        "subtitle_font_size": 0.13,
        "y_axis_format": "SMART_NUMBER",
        "time_format": "smart_date",
        "conditional_formatting": [],
        "extra_form_data": {},
        "emit_filter": False,
        "dashboards": [dashboard_id],
    }


def big_number_query(datasource_id: int, form_data: dict) -> dict:
    context = base_query(datasource_id)
    context["queries"][0]["metrics"] = [form_data["metric"]]
    context["form_data"] = {**form_data, "force": False, "result_format": "json", "result_type": "full"}
    return context


def axis_column(label: str, sql: str | None = None) -> dict:
    return {
        "timeGrain": "P1D",
        "columnType": "BASE_AXIS",
        "sqlExpression": sql or label,
        "label": label,
        "expressionType": "SQL",
        "isColumnReference": sql is None,
    }


def apply_series_colors(form_data: dict, metric_colors: dict[str, str]) -> None:
    label_colors = dict(form_data.get("label_colors") or {})
    for metric, color in metric_colors.items():
        for label in METRIC_COLOR_LABELS.get(metric, [metric]):
            label_colors[label] = color
    form_data["label_colors"] = label_colors
    form_data["color_scheme_domain"] = list(metric_colors.values())


def bar_form(
    datasource_id: int,
    dashboard_id: int,
    *,
    x_axis,
    metrics: list[str],
    sort_metric: str,
    row_limit: int,
    orientation: str,
    x_axis_title: str,
    y_axis_title: str,
    show_legend: bool = False,
    x_axis_label_rotation: int = 0,
    color_scheme: str = "supersetColors",
) -> dict:
    return {
        "datasource": f"{datasource_id}__table",
        "viz_type": "echarts_timeseries_bar",
        "x_axis": x_axis,
        "granularity_sqla": "cob_dt",
        "time_range": DEFAULT_TIME_RANGE,
        "time_grain_sqla": "P1D",
        "x_axis_sort_asc": True,
        "metrics": metrics,
        "groupby": [],
        "adhoc_filters": [],
        "timeseries_limit_metric": sort_metric,
        "order_desc": True,
        "row_limit": row_limit,
        "truncate_metric": True,
        "show_empty_columns": False,
        "comparison_type": "values",
        "annotation_layers": [],
        "forecastPeriods": 10,
        "forecastInterval": 0.8,
        "orientation": orientation,
        "x_axis_title": x_axis_title,
        "x_axis_title_margin": 15,
        "y_axis_title": y_axis_title,
        "y_axis_title_margin": 30,
        "y_axis_title_position": "Left",
        "sort_series_type": "sum",
        "color_scheme": color_scheme,
        "time_shift_color": True,
        "show_value": True,
        "only_total": True,
        "show_legend": show_legend,
        "legendType": "scroll",
        "legendOrientation": "top",
        "x_axis_time_format": "smart_date",
        "xAxisLabelInterval": "auto",
        "y_axis_format": "SMART_NUMBER",
        "y_axis_bounds": [None, None],
        "truncateXAxis": True,
        "x_axis_label_rotation": x_axis_label_rotation,
        "rotateXAxisLabel": x_axis_label_rotation,
        "rich_tooltip": True,
        "showTooltipTotal": True,
        "tooltipTimeFormat": "smart_date",
        "extra_form_data": {},
        "emit_filter": False,
        "dashboards": [dashboard_id],
    }


def bar_query(
    datasource_id: int,
    form_data: dict,
    x_label: str,
    x_sql: str | None = None,
) -> dict:
    context = base_query(datasource_id)
    x_column = axis_column(x_label, x_sql)
    post_processing = [
        {
            "operation": "pivot",
            "options": {
                "index": [x_label],
                "columns": [],
                "aggregates": {metric: {"operator": "mean"} for metric in form_data["metrics"]},
                "drop_missing_columns": False,
            },
        },
    ]
    post_processing.append({"operation": "flatten"})
    context["queries"][0].update(
        {
            "extras": {"time_grain_sqla": "P1D", "having": "", "where": ""},
            "columns": [x_column],
            "metrics": form_data["metrics"],
            "orderby": [[form_data["timeseries_limit_metric"], False]],
            "row_limit": form_data["row_limit"],
            "series_columns": [],
            "series_limit": 0,
            "series_limit_metric": form_data["timeseries_limit_metric"],
            "time_offsets": [],
            "post_processing": post_processing,
        }
    )
    context["form_data"] = {**form_data, "force": False, "result_format": "json", "result_type": "full"}
    return context


def pie_form(datasource_id: int, dashboard_id: int) -> dict:
    return {
        "datasource": f"{datasource_id}__table",
        "viz_type": "pie",
        "groupby": ["campaign_priority"],
        "metric": "customer_count",
        "adhoc_filters": [],
        "granularity_sqla": "cob_dt",
        "time_range": DEFAULT_TIME_RANGE,
        "row_limit": 10,
        "sort_by_metric": True,
        "color_scheme": "supersetColors",
        "donut": True,
        "innerRadius": 38,
        "outerRadius": 64,
        "show_labels": True,
        "labels_outside": True,
        "label_line": True,
        "label_type": "key_value_percent",
        "number_format": "SMART_NUMBER",
        "show_legend": True,
        "legendType": "scroll",
        "legendOrientation": "bottom",
        "extra_form_data": {},
        "emit_filter": False,
        "dashboards": [dashboard_id],
    }


def pie_query(datasource_id: int, form_data: dict) -> dict:
    context = base_query(datasource_id)
    context["queries"][0].update(
        {
            "columns": form_data["groupby"],
            "metrics": [form_data["metric"]],
            "orderby": [[form_data["metric"], False]],
            "row_limit": form_data["row_limit"],
        }
    )
    context["form_data"] = {**form_data, "force": False, "result_format": "json", "result_type": "full"}
    return context


def table_form(datasource_id: int, dashboard_id: int) -> dict:
    return {
        "datasource": f"{datasource_id}__table",
        "viz_type": "table",
        "query_mode": "raw",
        "groupby": [],
        "granularity_sqla": "cob_dt",
        "time_range": DEFAULT_TIME_RANGE,
        "temporal_columns_lookup": {
            "register_date": True,
            "last_txn_date": True,
            "last_interaction_date": True,
            "cob_dt": True,
        },
        "all_columns": [
            "customer_id",
            "full_name_masked",
            "customer_segment",
            "aum_bucket",
            "rfm_segment",
            "recommended_product",
            "campaign_priority",
            "cross_sell_score",
        ],
        "percent_metrics": [],
        "adhoc_filters": [],
        "order_by_cols": [],
        "order_desc": True,
        "server_page_length": 10,
        "page_length": 10,
        "row_limit": 10001,
        "percent_metric_calculation": "row_limit",
        "table_timestamp_format": "smart_date",
        "allow_render_html": True,
        "show_cell_bars": True,
        "color_pn": True,
        "comparison_color_scheme": "Green",
        "conditional_formatting": [],
        "comparison_type": "values",
        "extra_form_data": {},
        "emit_filter": False,
        "dashboards": [dashboard_id],
    }


def table_query(datasource_id: int, form_data: dict) -> dict:
    context = base_query(datasource_id)
    context["queries"][0].update(
        {
            "columns": form_data["all_columns"],
            "orderby": [],
            "row_limit": form_data["row_limit"],
            "post_processing": [],
            "time_offsets": [],
        }
    )
    context["form_data"] = {
        **form_data,
        "force": False,
        "result_format": "json",
        "result_type": "full",
        "include_time": False,
    }
    return context


def ensure_metric(db, SqlMetric, table, metric_name: str, expression: str, verbose_name: str) -> None:
    metric = (
        db.session.query(SqlMetric)
        .filter(SqlMetric.table_id == table.id, SqlMetric.metric_name == metric_name)
        .one_or_none()
    )
    if metric is None:
        metric = SqlMetric(metric_name=metric_name, table_id=table.id)
        db.session.add(metric)
    metric.expression = expression
    metric.verbose_name = verbose_name
    metric.metric_type = "count"
    metric.d3format = "SMART_NUMBER"


def ensure_column(db, TableColumn, table, column_name: str, column_type: str, *, is_dttm: bool = False) -> None:
    column = (
        db.session.query(TableColumn)
        .filter(TableColumn.table_id == table.id, TableColumn.column_name == column_name)
        .one_or_none()
    )
    if column is None:
        column = TableColumn(column_name=column_name, table_id=table.id)
        db.session.add(column)
    column.type = column_type
    column.is_dttm = is_dttm
    column.groupby = True
    column.filterable = True
    column.is_active = True


def ensure_product_penetration_table(db, SqlaTable, TableColumn, SqlMetric, source_table, owner_id: int | None):
    table = (
        db.session.query(SqlaTable)
        .filter(SqlaTable.schema == DATASET_SCHEMA, SqlaTable.table_name == PRODUCT_PENETRATION_TABLE)
        .one_or_none()
    )
    if table is None:
        table = SqlaTable(
            table_name=PRODUCT_PENETRATION_TABLE,
            schema=DATASET_SCHEMA,
            database_id=source_table.database_id,
            uuid=uuid.uuid4(),
        )
        if owner_id:
            table.created_by_fk = owner_id
        db.session.add(table)
        db.session.flush()
    table.database_id = source_table.database_id
    table.main_dttm_col = "cob_dt"
    table.sql = PRODUCT_PENETRATION_SQL
    table.is_sqllab_view = True
    table.changed_by_fk = owner_id
    for column_name, column_type, is_dttm in [
        ("cob_dt", "DATE", True),
        ("primary_branch_code", "STRING", False),
        ("customer_segment", "STRING", False),
        ("rfm_segment", "STRING", False),
        ("campaign_priority", "STRING", False),
        ("recommended_product", "STRING", False),
        ("recommended_product_name", "STRING", False),
        ("product", "STRING", False),
        ("has_product_customers", "BIGINT", False),
        ("eligible_without_product_customers", "BIGINT", False),
    ]:
        ensure_column(db, TableColumn, table, column_name, column_type, is_dttm=is_dttm)
    for metric_name, expression, verbose_name in [
        ("has_product_customers", "SUM(has_product_customers)", "Have Product"),
        (
            "eligible_without_product_customers",
            "SUM(eligible_without_product_customers)",
            "Eligible but Don't Have",
        ),
    ]:
        ensure_metric(db, SqlMetric, table, metric_name, expression, verbose_name)
    return table


def ensure_snapshot_table(db, SqlaTable, TableColumn, SqlMetric, source_table, owner_id: int | None):
    table = (
        db.session.query(SqlaTable)
        .filter(SqlaTable.schema == DATASET_SCHEMA, SqlaTable.table_name == SNAPSHOT_TABLE)
        .one_or_none()
    )
    if table is None:
        table = SqlaTable(
            table_name=SNAPSHOT_TABLE,
            schema=DATASET_SCHEMA,
            database_id=source_table.database_id,
            uuid=uuid.uuid4(),
        )
        if owner_id:
            table.created_by_fk = owner_id
        db.session.add(table)
        db.session.flush()
    table.database_id = source_table.database_id
    table.main_dttm_col = "cob_dt"
    table.sql = SNAPSHOT_SQL
    table.is_sqllab_view = True
    table.changed_by_fk = owner_id

    for source_column in source_table.columns:
        ensure_column(
            db,
            TableColumn,
            table,
            source_column.column_name,
            source_column.type or "STRING",
            is_dttm=bool(source_column.is_dttm),
        )
    for source_metric in source_table.metrics:
        ensure_metric(
            db,
            SqlMetric,
            table,
            source_metric.metric_name,
            source_metric.expression,
            source_metric.verbose_name or source_metric.metric_name,
        )
    for column_name in [
        "recommended_product_name",
        "churn_risk_bucket",
        "score_bucket",
    ]:
        ensure_column(db, TableColumn, table, column_name, "STRING")
    return table


def upsert_slice(db, Slice, dashboard, table, owner_id: int | None, *, name: str, viz_type: str, form_data: dict, query_context: dict):
    slc = db.session.query(Slice).filter(Slice.slice_name == name).one_or_none()
    if slc is None:
        slc = Slice(
            slice_name=name,
            viz_type=viz_type,
            datasource_id=table.id,
            datasource_type="table",
            datasource_name=table.table_name,
            uuid=uuid.uuid4(),
        )
        if owner_id:
            slc.created_by_fk = owner_id
        db.session.add(slc)
        db.session.flush()
    slc.viz_type = viz_type
    slc.datasource_id = table.id
    slc.datasource_type = "table"
    slc.datasource_name = table.table_name
    slc.changed_by_fk = owner_id
    slc.last_saved_by_fk = owner_id
    slc.last_saved_at = datetime.utcnow()
    form_data = {**form_data, "slice_id": slc.id}
    query_context = {**query_context, "form_data": {**query_context["form_data"], "slice_id": slc.id}}
    slc.params = dumps(form_data)
    slc.query_context = dumps(query_context)
    if slc not in dashboard.slices:
        dashboard.slices.append(slc)
    return slc


def build_position_json(charts_by_name: dict[str, object]) -> str:
    rows = [
        (
            "ROW-KPI",
            [("Total Customers", 3), ("Total AUM", 3), ("Active 30d", 3), ("Campaign Eligible", 3)],
            15,
        ),
        ("ROW-SEGMENT", [("RFM Segment Distribution", 6), ("AUM by Branch", 6)], 35),
        (
            "ROW-CAMPAIGN",
            [("Recommended Product Mix", 4), ("Campaign Priority", 4), ("Churn Risk", 4)],
            38,
        ),
        ("ROW-SCORE", [("Cross-sell Score Distribution", 6), ("Product Penetration", 6)], 34),
        ("ROW-DETAIL", [("Customer Drill-down", 12)], 30),
    ]
    position = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"children": [DASHBOARD_TABS_ID], "id": "ROOT_ID", "type": "ROOT"},
        DASHBOARD_TABS_ID: {
            "children": [DASHBOARD_TAB_ID],
            "id": DASHBOARD_TABS_ID,
            "meta": {},
            "parents": ["ROOT_ID"],
            "type": "TABS",
        },
        DASHBOARD_TAB_ID: {
            "children": [row_id for row_id, *_ in rows],
            "id": DASHBOARD_TAB_ID,
            "meta": {"text": "Overview"},
            "parents": ["ROOT_ID", DASHBOARD_TABS_ID],
            "type": "TAB",
        },
        GRID_ID: {
            "children": [],
            "id": GRID_ID,
            "parents": ["ROOT_ID"],
            "type": "GRID",
        },
        "HEADER_ID": {"id": "HEADER_ID", "meta": {"text": DASHBOARD_TITLE}, "type": "HEADER"},
    }
    for row_id, items, height in rows:
        chart_node_ids = []
        for name, width in items:
            chart = charts_by_name[name]
            chart_node_id = f"CHART-{chart.id}"
            chart_node_ids.append(chart_node_id)
            position[chart_node_id] = {
                "children": [],
                "id": chart_node_id,
                "meta": {
                    "chartId": chart.id,
                    "height": height,
                    "sliceName": chart.slice_name,
                    "uuid": str(chart.uuid),
                    "width": width,
                },
                "parents": ["ROOT_ID", DASHBOARD_TABS_ID, DASHBOARD_TAB_ID, row_id],
                "type": "CHART",
            }
        position[row_id] = {
            "children": chart_node_ids,
            "id": row_id,
            "meta": {"background": "BACKGROUND_WHITE"},
            "parents": ["ROOT_ID", DASHBOARD_TABS_ID, DASHBOARD_TAB_ID],
            "type": "ROW",
        }
    return dumps(position)


def chart_selector(charts_by_name: dict[str, object], *names: str) -> str:
    return ",\n".join(f".dashboard-chart-id-{charts_by_name[name].id}" for name in names)


def chart_child_selector(charts_by_name: dict[str, object], child_selector: str, *names: str) -> str:
    return ",\n".join(f".dashboard-chart-id-{charts_by_name[name].id} {child_selector}" for name in names)


def chart_pseudo_selector(charts_by_name: dict[str, object], pseudo_selector: str, *names: str) -> str:
    return ",\n".join(f".dashboard-chart-id-{charts_by_name[name].id}{pseudo_selector}" for name in names)


def dashboard_css(charts_by_name: dict[str, object]) -> str:
    kpis = ("Total Customers", "Total AUM", "Active 30d", "Campaign Eligible")
    kpi_selector = chart_selector(charts_by_name, *kpis)
    title_selector = chart_child_selector(charts_by_name, ".header-title", *charts_by_name.keys())
    kpi_before = chart_pseudo_selector(charts_by_name, "::before", *kpis)
    kpi_after = chart_pseudo_selector(charts_by_name, "::after", *kpis)
    kpi_slice = chart_child_selector(charts_by_name, ".chart-slice", *kpis)
    kpi_header = chart_child_selector(charts_by_name, ".chart-header", *kpis)
    kpi_header_title = chart_child_selector(charts_by_name, ".header-title", *kpis)
    kpi_text = chart_child_selector(charts_by_name, ".big_number_total .text-container", *kpis)
    kpi_number = chart_child_selector(charts_by_name, ".big_number_total .header-line", *kpis)
    kpi_subtitle = chart_child_selector(charts_by_name, ".big_number_total .subtitle-line", *kpis)
    table_id = charts_by_name["Customer Drill-down"].id
    total_customers_id = charts_by_name["Total Customers"].id
    total_aum_id = charts_by_name["Total AUM"].id
    active_id = charts_by_name["Active 30d"].id
    eligible_id = charts_by_name["Campaign Eligible"].id

    return f"""
html,
body,
#app,
.ant-layout,
.ant-layout-content,
.dashboard,
.dashboard-content {{
  background: #F6F8FB !important;
  color: #172033 !important;
}}

#main-menu,
header#main-menu,
.header-with-actions {{
  background: #FFFFFF !important;
  border-bottom: 1px solid #E5EAF0 !important;
  box-shadow: 0 2px 8px rgba(15, 23, 42, 0.04) !important;
  color: #172033 !important;
}}

#main-menu .main-nav,
#main-menu .ant-menu,
#main-menu .ant-menu-root,
#main-menu .ant-menu-item,
#main-menu .ant-menu-submenu {{
  background: #FFFFFF !important;
}}

#main-menu a,
#main-menu button,
#main-menu span,
#main-menu div,
#main-menu .ant-menu-title-content,
.header-with-actions div,
.header-with-actions span,
.header-with-actions input {{
  color: #172033 !important;
}}

.header-with-actions .dynamic-title-input,
.header-with-actions .ant-input {{
  background: #FFFFFF !important;
  color: #172033 !important;
}}

.ant-layout-content > div,
.dashboard-grid,
.grid-content,
.grid-row,
.grid-row.background--white,
.dashboard-component,
.resizable-container {{
  background: #F6F8FB !important;
}}

.dashboard-content {{
  padding: 14px 18px 28px !important;
}}

.dashboard .ant-tabs-nav {{
  display: none !important;
}}

.dashboard .ant-tabs,
.dashboard .ant-tabs-content-holder,
.dashboard .ant-tabs-content,
.dashboard .ant-tabs-tabpane {{
  background: transparent !important;
}}

.dashboard-component-chart-holder,
.dashboard .dashboard-component-chart-holder,
.dashboard-component-chart-holder[class*="superset-"] {{
  background: #FFFFFF !important;
  border: 1px solid #DFE7EF !important;
  border-radius: 10px !important;
  box-shadow: 0 10px 26px rgba(15, 23, 42, 0.09) !important;
  overflow: hidden !important;
  transition: box-shadow 160ms ease, transform 160ms ease !important;
}}

.dashboard-component-chart-holder:hover {{
  box-shadow: 0 14px 34px rgba(15, 23, 42, 0.13) !important;
  transform: translateY(-1px) !important;
}}

.dashboard-component-chart-holder .chart-header {{
  padding: 12px 16px 0 !important;
}}

{title_selector} {{
  color: #172033 !important;
  font-size: 15px !important;
  font-weight: 750 !important;
}}

.dashboard-component-chart-holder .header-controls {{
  display: none !important;
  opacity: 0 !important;
}}

.dashboard-component-chart-holder .slice_container {{
  padding: 0 8px 8px;
}}

.dashboard-component-chart-holder canvas,
.dashboard-component-chart-holder svg {{
  font-family: Inter, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif !important;
}}

.dashboard-component-chart-holder svg text {{
  fill: #334155 !important;
}}

{kpi_selector} {{
  position: relative;
  background:
    radial-gradient(circle at 18% 22%, rgba(15, 143, 163, 0.13), transparent 35%),
    linear-gradient(180deg, rgba(255, 255, 255, 0.98), #FFFFFF);
  min-height: 118px;
}}

{kpi_before} {{
  border-radius: 999px;
  box-shadow: inset 0 -9px 18px rgba(0, 0, 0, 0.15), 0 12px 20px rgba(15, 143, 163, 0.22);
  content: "";
  display: block;
  height: 58px;
  left: 20px;
  position: absolute;
  top: 34px;
  width: 58px;
  z-index: 2;
}}

{kpi_after} {{
  content: "";
  display: block;
  height: 28px;
  left: 35px;
  position: absolute;
  top: 49px;
  width: 28px;
  z-index: 3;
}}

.dashboard-chart-id-{total_customers_id}::before {{
  background: #0F8FA3;
}}
.dashboard-chart-id-{total_customers_id}::after {{
  background: transparent url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='28' height='28' viewBox='0 0 24 24' fill='none' stroke='white' stroke-width='2.2' stroke-linecap='round' stroke-linejoin='round'%3E%3Cpath d='M16 21v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2'/%3E%3Ccircle cx='9' cy='7' r='4'/%3E%3Cpath d='M22 21v-2a4 4 0 0 0-3-3.9'/%3E%3Cpath d='M16 3.1a4 4 0 0 1 0 7.8'/%3E%3C/svg%3E") center / contain no-repeat;
}}

.dashboard-chart-id-{total_aum_id}::before {{
  background: #2B6CB0;
}}
.dashboard-chart-id-{total_aum_id}::after {{
  background: transparent url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='28' height='28' viewBox='0 0 24 24' fill='none' stroke='white' stroke-width='2.2' stroke-linecap='round' stroke-linejoin='round'%3E%3Cpath d='M3 21h18'/%3E%3Cpath d='M5 21V10'/%3E%3Cpath d='M19 21V10'/%3E%3Cpath d='M7 10h10'/%3E%3Cpath d='M12 3 3 8h18z'/%3E%3Cpath d='M9 21V10'/%3E%3Cpath d='M15 21V10'/%3E%3C/svg%3E") center / contain no-repeat;
}}

.dashboard-chart-id-{active_id}::before {{
  background: #16A34A;
}}
.dashboard-chart-id-{active_id}::after {{
  background: transparent url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='28' height='28' viewBox='0 0 24 24' fill='none' stroke='white' stroke-width='2.2' stroke-linecap='round' stroke-linejoin='round'%3E%3Ccircle cx='12' cy='8' r='4'/%3E%3Cpath d='M6 21v-2a6 6 0 0 1 12 0v2'/%3E%3Cpath d='m17 5 2 2 4-4'/%3E%3C/svg%3E") center / contain no-repeat;
}}

.dashboard-chart-id-{eligible_id}::before {{
  background: #7C3AED;
}}
.dashboard-chart-id-{eligible_id}::after {{
  background: transparent url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='28' height='28' viewBox='0 0 24 24' fill='none' stroke='white' stroke-width='2.2' stroke-linecap='round' stroke-linejoin='round'%3E%3Cpath d='m3 11 18-5v12L3 13z'/%3E%3Cpath d='M7 14a4 4 0 0 0 5 6'/%3E%3C/svg%3E") center / contain no-repeat;
}}

{kpi_slice} {{
  box-sizing: border-box;
  overflow: hidden;
  padding-left: 94px;
}}

{kpi_header} {{
  padding-left: 104px !important;
  padding-top: 12px !important;
}}

{kpi_header_title} {{
  color: #172033 !important;
  font-size: 15px !important;
  font-weight: 750 !important;
}}

{kpi_text} {{
  width: auto !important;
}}

{kpi_number} {{
  color: #08798C !important;
  font-size: 42px !important;
  font-weight: 800 !important;
  letter-spacing: 0 !important;
  line-height: 46px !important;
}}

{kpi_subtitle} {{
  color: #64748B !important;
  font-size: 10px !important;
  font-weight: 600 !important;
  line-height: 14px !important;
  margin-top: 4px;
}}

.dashboard-chart-id-{table_id} table {{
  border-color: #E5EAF0 !important;
  font-size: 12px !important;
  line-height: 18px !important;
}}

.dashboard-chart-id-{table_id} th {{
  background: #F8FAFC !important;
  color: #334155 !important;
  font-weight: 750 !important;
}}

.dashboard-chart-id-{table_id} td {{
  color: #334155 !important;
}}

.dashboard-chart-id-{table_id} tbody tr:nth-child(even) td {{
  background: #F8FAFC !important;
}}

.dashboard-chart-id-{table_id} tbody tr:hover td {{
  background: #EAF7FA !important;
}}

.dashboard-chart-id-{table_id} .pagination button,
.dashboard-chart-id-{table_id} .dt-pagination button {{
  border-radius: 6px !important;
}}
"""


def main() -> None:
    app = create_app()
    with app.app_context():
        from superset.connectors.sqla.models import SqlaTable, SqlMetric, TableColumn
        from superset.extensions import db, security_manager
        from superset.models.dashboard import Dashboard
        from superset.models.slice import Slice

        admin = security_manager.find_user(username="admin")
        owner_id = admin.id if admin else None

        source_table = (
            db.session.query(SqlaTable)
            .filter(SqlaTable.schema == DATASET_SCHEMA, SqlaTable.table_name == DATASET_TABLE)
            .one()
        )
        dashboard = db.session.query(Dashboard).filter(Dashboard.dashboard_title == DASHBOARD_TITLE).one_or_none()
        if dashboard is None:
            dashboard = Dashboard(dashboard_title=DASHBOARD_TITLE, published=True)
            if owner_id:
                dashboard.created_by_fk = owner_id
            db.session.add(dashboard)
            db.session.flush()
        dashboard.published = True
        dashboard.changed_by_fk = owner_id

        for metric_name, expression, verbose_name in [
            ("credit_card_customers", "SUM(has_credit_card)", "Credit Card Customers"),
            ("savings_customers", "SUM(has_savings)", "Savings Customers"),
            ("loan_customers", "SUM(has_loan)", "Loan Customers"),
            *CHART_COLOR_METRICS,
        ]:
            ensure_metric(db, SqlMetric, source_table, metric_name, expression, verbose_name)
        db.session.flush()
        table = ensure_snapshot_table(db, SqlaTable, TableColumn, SqlMetric, source_table, owner_id)
        for metric_name, expression, verbose_name in [
            ("credit_card_customers", "SUM(has_credit_card)", "Credit Card Customers"),
            ("savings_customers", "SUM(has_savings)", "Savings Customers"),
            ("loan_customers", "SUM(has_loan)", "Loan Customers"),
            *CHART_COLOR_METRICS,
        ]:
            ensure_metric(db, SqlMetric, table, metric_name, expression, verbose_name)
        product_penetration_table = ensure_product_penetration_table(
            db, SqlaTable, TableColumn, SqlMetric, source_table, owner_id
        )
        db.session.flush()

        chart_specs = []
        for name, metric, subtitle in [
            ("Total Customers", "customer_count", "Customers in snapshot"),
            ("Total AUM", "total_aum", "Total assets under management"),
            ("Active 30d", "active_30d_customers", "Customers active in last 30 days"),
            ("Campaign Eligible", "eligible_customers", "Eligible for campaign contact"),
        ]:
            form = big_number_form(table.id, dashboard.id, name, metric, subtitle)
            chart_specs.append((name, table, "big_number_total", form, big_number_query(table.id, form)))

        bar_chart_colors = {
            "RFM Segment Distribution": "#2F80ED",
            "AUM by Branch": "#14A6B8",
            "Recommended Product Mix": "#7C3AED",
            "Churn Risk": "#E4572E",
            "Cross-sell Score Distribution": "#5BC08A",
        }
        for name, source, x_axis, metrics, sort_metric, row_limit, orientation, x_title, y_title, x_label, x_sql, show_legend, label_rotation in [
            ("RFM Segment Distribution", table, "rfm_segment", ["rfm_customer_count"], "rfm_customer_count", 10, "vertical", "", "Customers", "rfm_segment", None, False, 0),
            ("AUM by Branch", table, "primary_branch_code", ["branch_total_aum"], "branch_total_aum", 10, "horizontal", "", "", "primary_branch_code", None, False, 0),
            (
                "Recommended Product Mix",
                table,
                "recommended_product_name",
                ["product_mix_customers"],
                "product_mix_customers",
                10,
                "vertical",
                "",
                "Customers",
                "recommended_product_name",
                None,
                False,
                0,
            ),
            ("Churn Risk", table, "churn_risk_bucket", ["churn_risk_customers"], "churn_risk_customers", 10, "vertical", "", "Customers", "churn_risk_bucket", None, False, 0),
            ("Cross-sell Score Distribution", table, "score_bucket", ["score_distribution_customers"], "score_distribution_customers", 20, "vertical", "", "Customers", "score_bucket", None, False, 0),
        ]:
            form = bar_form(
                source.id,
                dashboard.id,
                x_axis=x_axis,
                metrics=metrics,
                sort_metric=sort_metric,
                row_limit=row_limit,
                orientation=orientation,
                x_axis_title=x_title,
                y_axis_title=y_title,
                show_legend=show_legend,
                x_axis_label_rotation=label_rotation,
            )
            if color := bar_chart_colors.get(name):
                apply_series_colors(form, {metric: color for metric in metrics})
            if name == "Recommended Product Mix":
                form["xAxisLabelInterval"] = 0
                form["truncateXAxis"] = False
            chart_specs.append((name, source, "echarts_timeseries_bar", form, bar_query(source.id, form, x_label, x_sql)))

        form = bar_form(
            product_penetration_table.id,
            dashboard.id,
            x_axis="product",
            metrics=["has_product_customers", "eligible_without_product_customers"],
            sort_metric="eligible_without_product_customers",
            row_limit=10,
            orientation="vertical",
            x_axis_title="",
            y_axis_title="Customers",
            show_legend=True,
        )
        apply_series_colors(
            form,
            {
                "eligible_without_product_customers": "#14A6B8",
                "has_product_customers": "#465681",
            },
        )
        chart_specs.append(
            (
                "Product Penetration",
                product_penetration_table,
                "echarts_timeseries_bar",
                form,
                bar_query(product_penetration_table.id, form, "product"),
            )
        )

        form = pie_form(table.id, dashboard.id)
        chart_specs.append(("Campaign Priority", table, "pie", form, pie_query(table.id, form)))

        form = table_form(table.id, dashboard.id)
        chart_specs.append(("Customer Drill-down", table, "table", form, table_query(table.id, form)))

        charts = {}
        for name, source_table, viz_type, form, query in chart_specs:
            charts[name] = upsert_slice(
                db,
                Slice,
                dashboard,
                source_table,
                owner_id,
                name=name,
                viz_type=viz_type,
                form_data=form,
                query_context=query,
            )

        dashboard.position_json = build_position_json(charts)
        chart_ids = [charts[name].id for name in charts]
        dashboard.json_metadata = dumps(
            {
                "color_scheme_domain": [
                    "#0F8FA3",
                    "#2B6CB0",
                    "#5BC08A",
                    "#F2B824",
                    "#4C5A8A",
                    "#CBD5E1",
                ],
                "shared_label_colors": [],
                "map_label_colors": {},
                "label_colors": {
                    "At Risk": "#F2B824",
                    "Branch AUM": "#14A6B8",
                    "Champions": "#0F8FA3",
                    "Churn Risk Customers": "#E4572E",
                    "Credit Card": "#0F8FA3",
                    "Digital Savings": "#5BC08A",
                    "Eligible but Don't Have": "#2B6CB0",
                    "Have Product": "#0F8FA3",
                    "High": "#EF4444",
                    "HIGH": "#4C5A8A",
                    "Hibernating": "#94A3B8",
                    "Loan": "#4C5A8A",
                    "Lost": "#B91C1C",
                    "Loyal Customers": "#2B6CB0",
                    "Low": "#5BC08A",
                    "LOW": "#5BC08A",
                    "MEDIUM": "#2B6CB0",
                    "Medium": "#2B6CB0",
                    "New Customers": "#38BDF8",
                    "Personal Loan": "#4C5A8A",
                    "Potential Loyalists": "#A78BFA",
                    "Product Mix Customers": "#7C3AED",
                    "RFM Customers": "#2F80ED",
                    "Savings": "#5BC08A",
                    "Score Distribution Customers": "#5BC08A",
                    "Term Deposit": "#2B6CB0",
                    "Very High": "#B91C1C",
                    "Very Low": "#0F8FA3",
                    "Wealth Management": "#7C3AED",
                    "branch_total_aum": "#14A6B8",
                    "churn_risk_customers": "#E4572E",
                    "product_mix_customers": "#7C3AED",
                    "rfm_customer_count": "#2F80ED",
                    "score_distribution_customers": "#5BC08A",
                },
                "chart_configuration": {
                    str(chart_id): {
                        "id": chart_id,
                        "crossFilters": {"scope": "global", "chartsInScope": []},
                        "emitCrossFilters": False,
                    }
                    for chart_id in chart_ids
                },
                "global_chart_configuration": {
                    "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
                    "chartsInScope": chart_ids,
                },
                "color_scheme": "supersetColors",
                "refresh_frequency": 0,
                "expanded_slices": {},
                "timed_refresh_immune_slices": [],
                "cross_filters_enabled": False,
                "default_filters": "{}",
                "filter_scopes": {},
                "native_filter_configuration": [],
            }
        )
        dashboard.css = dashboard_css(charts)

        db.session.commit()
        print("Updated dashboard:", dashboard.id, dashboard.dashboard_title)
        for name in [
            "Total Customers",
            "Total AUM",
            "Active 30d",
            "Campaign Eligible",
            "RFM Segment Distribution",
            "AUM by Branch",
            "Recommended Product Mix",
            "Campaign Priority",
            "Churn Risk",
            "Cross-sell Score Distribution",
            "Product Penetration",
            "Customer Drill-down",
        ]:
            chart = charts[name]
            print(f"{chart.id}: {chart.slice_name} [{chart.viz_type}]")


if __name__ == "__main__":
    main()
