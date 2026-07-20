"""End-to-end SCD2 correctness test against an isolated Iceberg table.

Run from the Jupyter container so driver and worker both use Python 3.8:
    docker exec jupyter /opt/spark/bin/spark-submit \
      --master spark://spark-master:7077 \
      /opt/project/tests/integration/test_scd2_integration.py
"""

import hashlib
import sys
import unittest
from datetime import datetime
from pathlib import Path

from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType


PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT / "code_etl" / "silver" / "base_job"))
sys.path.insert(0, str(PROJECT_ROOT / "code_etl" / "shared"))

from scd_type2 import run_scd_type2, validate_config
from spark.spark_session import get_spark_session
from utils.logger import get_logger


class Scd2IcebergIntegrationTest(unittest.TestCase):
    namespace = "lakehouse.scd2_test"
    target = f"{namespace}.dim_entity"
    source_view = "scd2_test_source"

    config = {
        "job": {"type": "scd_type2"},
        "source": {"tables": ["scd2_test_source"], "snapshot_mode": "full"},
        "target": {"catalog": "lakehouse", "schema": "scd2_test", "table": "dim_entity"},
        "business_key": ["entity_id"],
        "scd": {
            "type": 2,
            "sk_column": "entity_sk",
            "effective_from_column": "effective_from",
            "effective_to_column": "effective_to",
            "current_flag_column": "is_current",
            "detect_deletes": True,
        },
        "tracked_columns": ["entity_name"],
        "sql": "SELECT entity_id, entity_name, last_updated FROM scd2_test_source WHERE '{{ cob_dt }}' IS NOT NULL",
    }

    source_schema = StructType(
        [
            StructField("entity_id", LongType(), False),
            StructField("entity_name", StringType(), True),
            StructField("last_updated", TimestampType(), True),
        ]
    )

    @classmethod
    def setUpClass(cls):
        validate_config(cls.config)
        cls.logger = get_logger("scd2-integration-test")
        cls.spark = get_spark_session("scd2-integration-test")
        cls.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {cls.namespace}")
        cls.spark.sql(f"DROP TABLE IF EXISTS {cls.target} PURGE")
        cls.spark.sql(
            f"""
            CREATE TABLE {cls.target} (
                entity_id BIGINT,
                entity_name STRING,
                last_updated TIMESTAMP,
                effective_from DATE,
                effective_to DATE,
                is_current INT,
                entity_sk STRING
            )
            USING iceberg
            PARTITIONED BY (is_current)
            TBLPROPERTIES ('format-version' = '2')
            """
        )

    @classmethod
    def tearDownClass(cls):
        try:
            cls.spark.catalog.dropTempView(cls.source_view)
            cls.spark.sql(f"DROP TABLE IF EXISTS {cls.target} PURGE")
            cls.spark.sql(f"DROP NAMESPACE IF EXISTS {cls.namespace}")
        finally:
            cls.spark.stop()

    def _set_source(self, rows):
        self.spark.createDataFrame(rows, self.source_schema).createOrReplaceTempView(self.source_view)

    def _run(self, cob_dt):
        run_scd_type2(self.spark, self.config, cob_dt, self.logger)

    def _scalar(self, sql):
        return self.spark.sql(sql).first()[0]

    def test_full_lifecycle_and_recovery(self):
        t0 = datetime(2025, 12, 31, 8, 0, 0)
        self._set_source([(1, "Alice", t0), (2, "Bob", t0)])
        self._run("2025-12-31")
        self.assertEqual(self._scalar(f"SELECT count(*) FROM {self.target}"), 2)
        self.assertEqual(self._scalar(f"SELECT count(*) FROM {self.target} WHERE is_current=1"), 2)

        first_sk_set = {
            row.entity_sk for row in self.spark.sql(f"SELECT entity_sk FROM {self.target}").collect()
        }
        self._run("2025-12-31")
        self.assertEqual(self._scalar(f"SELECT count(*) FROM {self.target}"), 2)
        self.assertEqual(
            {row.entity_sk for row in self.spark.sql(f"SELECT entity_sk FROM {self.target}").collect()},
            first_sk_set,
        )

        self._set_source(
            [
                (1, "Alice Prime", datetime(2026, 1, 1, 8, 0, 0)),
                (2, "Bob", datetime(2026, 1, 1, 8, 0, 0)),
            ]
        )
        self._run("2026-01-01")
        self.assertEqual(self._scalar(f"SELECT count(*) FROM {self.target}"), 3)
        self.assertEqual(
            self._scalar(f"SELECT count(*) FROM {self.target} WHERE entity_id=1"), 2
        )
        self.assertEqual(
            self._scalar(f"SELECT count(*) FROM {self.target} WHERE entity_id=2"), 1
        )

        # Simulate an Iceberg commit that appended the new version, followed by
        # process failure before the predecessor was expired.
        partial_sk = hashlib.sha256("1|2026-01-02".encode("utf-8")).hexdigest()
        self.spark.sql(
            f"""
            INSERT INTO {self.target} VALUES (
                1, 'Alice Interrupted', TIMESTAMP '2026-01-02 08:00:00',
                DATE '2026-01-02', DATE '9999-12-31', 1, '{partial_sk}'
            )
            """
        )
        self.assertEqual(
            self._scalar(f"SELECT count(*) FROM {self.target} WHERE entity_id=1 AND is_current=1"),
            2,
        )

        self._set_source(
            [
                (1, "Alice Recovered", datetime(2026, 1, 2, 8, 0, 0)),
                (2, "Bob", datetime(2026, 1, 2, 8, 0, 0)),
            ]
        )
        self._run("2026-01-02")
        self.assertEqual(
            self._scalar(f"SELECT count(*) FROM {self.target} WHERE entity_id=1 AND is_current=1"),
            1,
        )
        self.assertEqual(
            self._scalar(
                f"SELECT count(*) FROM {self.target} "
                "WHERE entity_id=1 AND entity_name='Alice Recovered' AND is_current=1"
            ),
            1,
        )

        self._set_source([(1, "Alice Recovered", datetime(2026, 1, 3, 8, 0, 0))])
        self._run("2026-01-03")
        self.assertEqual(
            self._scalar(f"SELECT count(*) FROM {self.target} WHERE entity_id=2 AND is_current=1"),
            0,
        )

        row_count_before_failure = self._scalar(f"SELECT count(*) FROM {self.target}")
        self._set_source(
            [
                (1, "Duplicate A", datetime(2026, 1, 4, 8, 0, 0)),
                (1, "Duplicate B", datetime(2026, 1, 4, 8, 0, 0)),
            ]
        )
        with self.assertRaisesRegex(ValueError, "duplicate business key"):
            self._run("2026-01-04")
        self.assertEqual(self._scalar(f"SELECT count(*) FROM {self.target}"), row_count_before_failure)


if __name__ == "__main__":
    unittest.main(verbosity=2)
