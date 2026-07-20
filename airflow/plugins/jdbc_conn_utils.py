"""
Shared JDBC connection utilities cho Bronze/Silver/Gold DAGs.

Hai chế độ sử dụng:
  1. jdbc_jinja_args(conn_id)   — trả về Jinja template strings, gọi ở DAG parse time.
                                  Airflow resolve giá trị thực lúc task execute.
                                  → Dùng cho SparkSubmitOperator.application_args
  2. resolve_jdbc_conn(conn_id) — trả về giá trị thực, gọi trong hook/runtime code.
                                  → Dùng trong PythonOperator hoặc custom hook

Quy ước JDBC connection trong Airflow UI:
  - conn_type : jdbc
  - Host      : full JDBC URL, vd jdbc:postgresql://postgres:5432/card_crm
  - Login     : username
  - Password  : password
"""

from __future__ import annotations

from airflow.hooks.base import BaseHook

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _build_jdbc_url(conn) -> str:
    """
    Xây JDBC URL từ Airflow connection object.

    conn_type='jdbc'     → host đã là full JDBC URL, dùng trực tiếp.
    conn_type='postgres' → build từ host/port/schema.
    conn_type='oracle'   → build thin URL từ host/port/schema.
    """
    conn_type = (conn.conn_type or "").lower()

    builders = {
        "jdbc":     lambda c: c.host,
        "postgres": lambda c: f"jdbc:postgresql://{c.host}:{c.port or 5432}/{c.schema}",
        "oracle":   lambda c: f"jdbc:oracle:thin:@//{c.host}:{c.port or 1521}/{c.schema}",
        "mysql":    lambda c: f"jdbc:mysql://{c.host}:{c.port or 3306}/{c.schema}",
        "mssql":    lambda c: (
            f"jdbc:sqlserver://{c.host}:{c.port or 1433};databaseName={c.schema}"
        ),
    }

    if conn_type not in builders:
        raise ValueError(
            f"conn_id '{conn.conn_id}' có conn_type='{conn_type}' không được hỗ trợ. "
            f"Supported: {list(builders)}"
        )

    return builders[conn_type](conn)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def jdbc_jinja_args(conn_id: str) -> dict[str, str]:
    """
    Trả về dict chứa Jinja template strings cho SparkSubmitOperator.application_args.

    Không gọi DB, không cần connection tồn tại lúc DAG parse.
    Airflow resolve template lúc task execute.

    Ví dụ:
        conn_tmpl = jdbc_jinja_args("postgres-card-crm")
        SparkSubmitOperator(
            application_args=[
                "--jdbc_url", conn_tmpl["jdbc_url"],
                "--db_user",  conn_tmpl["db_user"],
                ...
            ]
        )
    """
    return {
        "jdbc_url":    f"{{{{ conn['{conn_id}'].host }}}}",
        "db_user":     f"{{{{ conn['{conn_id}'].login }}}}",
        "db_password": f"{{{{ conn['{conn_id}'].password }}}}",
    }


def resolve_jdbc_conn(conn_id: str) -> dict[str, str]:
    """
    Trả về JDBC connection params thực (gọi DB ngay lập tức).

    Chỉ dùng trong runtime context (PythonOperator, custom Hook),
    KHÔNG gọi ở module level của DAG file.

    Returns:
        dict với keys: jdbc_url, db_user, db_password
    """
    conn = BaseHook.get_connection(conn_id)
    return {
        "jdbc_url":    _build_jdbc_url(conn),
        "db_user":     conn.login,
        "db_password": conn.password,
    }
