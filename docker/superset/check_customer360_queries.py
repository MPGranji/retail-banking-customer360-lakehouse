import os

import trino
import urllib3


def main() -> None:
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    user = os.environ.get("SUPERSET_TRINO_USER", "marketing")
    conn = trino.dbapi.connect(
        host="trino",
        port=8443,
        user=user,
        http_scheme="https",
        auth=trino.auth.BasicAuthentication(user, os.environ["SUPERSET_TRINO_PASSWORD"]),
        catalog="lakehouse",
        schema="sandbox",
        verify=False,
    )
    cur = conn.cursor()
    queries = {
        "rfm": """
            SELECT rfm_segment, COUNT(*)
            FROM mart_customer_360_dashboard
            WHERE cob_dt = DATE '2026-01-01'
            GROUP BY 1
            ORDER BY 2 DESC
            LIMIT 3
        """,
        "aum_branch": """
            SELECT primary_branch_code, SUM(aum_total)
            FROM mart_customer_360_dashboard
            WHERE cob_dt = DATE '2026-01-01'
            GROUP BY 1
            ORDER BY 2 DESC
            LIMIT 3
        """,
        "product": """
            SELECT recommended_product, COUNT(*)
            FROM mart_customer_360_dashboard
            WHERE cob_dt = DATE '2026-01-01'
            GROUP BY 1
            ORDER BY 2 DESC
            LIMIT 3
        """,
        "priority": """
            SELECT campaign_priority, COUNT(*)
            FROM mart_customer_360_dashboard
            WHERE cob_dt = DATE '2026-01-01'
            GROUP BY 1
            ORDER BY 2 DESC
        """,
    }
    for name, sql in queries.items():
        cur.execute(sql)
        print(name, cur.fetchall())


if __name__ == "__main__":
    main()
