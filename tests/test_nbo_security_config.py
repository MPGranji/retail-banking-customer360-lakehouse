"""Static contracts for explainable NBO output and secured Trino access."""

import json
import unittest
from pathlib import Path

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class NboContractTest(unittest.TestCase):
    def test_nbo_columns_exist_in_ddl_reset_and_migration(self):
        paths = (
            PROJECT_ROOT / "ddl" / "Lakehouse" / "Gold" / "01_create_gold_tables.sql",
            PROJECT_ROOT / "code_etl" / "shared" / "ops" / "reset_iceberg_tables.py",
            PROJECT_ROOT / "docker" / "migrations" / "002_campaign_nbo_columns.sql",
        )
        for path in paths:
            text = path.read_text(encoding="utf-8")
            with self.subTest(path=path.name):
                for column in (
                    "cross_sell_score",
                    "recommended_product",
                    "recommendation_reason",
                    "campaign_priority",
                    "contact_eligible_flag",
                    "suppression_reason",
                ):
                    self.assertIn(column, text)


class TrinoSecurityContractTest(unittest.TestCase):
    def test_tls_and_password_auth_are_enabled(self):
        config = (PROJECT_ROOT / "docker" / "trino" / "etc" / "config.properties").read_text(
            encoding="utf-8"
        )
        for expected in (
            "http-server.https.enabled=true",
            "http-server.authentication.type=PASSWORD",
            "internal-communication.shared-secret=${ENV:TRINO_SHARED_SECRET}",
        ):
            self.assertIn(expected, config)
        self.assertNotIn("authentication.allow-insecure-over-http=true", config)

        authenticator = (
            PROJECT_ROOT / "docker" / "trino" / "etc" / "password-authenticator.properties"
        ).read_text(encoding="utf-8")
        self.assertIn("password-authenticator.name=file", authenticator)
        self.assertIn("/var/trino/security/password.db", authenticator)

    def test_rbac_limits_marketing_to_sandbox(self):
        rules = json.loads(
            (PROJECT_ROOT / "docker" / "trino" / "etc" / "access-control-rules.json").read_text(
                encoding="utf-8"
            )
        )
        marketing_allow = [
            rule
            for rule in rules["tables"]
            if rule.get("user") == "marketing" and rule.get("privileges") == ["SELECT"]
        ]
        self.assertEqual(marketing_allow[0]["schema"], "sandbox")
        self.assertTrue(
            any(rule.get("user") == "marketing" and rule.get("privileges") == [] for rule in rules["tables"])
        )
        self.assertTrue(
            any(rule.get("user") == "data_engineer" and "OWNERSHIP" in rule["privileges"] for rule in rules["tables"])
        )

    def test_compose_keeps_runtime_credentials_out_of_git(self):
        compose = (PROJECT_ROOT / "docker" / "docker-compose.yml").read_text(encoding="utf-8")
        self.assertIn('"8085:8443"', compose)
        self.assertIn("./volumes/trino-security:/var/trino/security", compose)
        self.assertIn("TRINO_MARKETING_PASSWORD: ${TRINO_MARKETING_PASSWORD}", compose)

        env_example = (PROJECT_ROOT / "docker" / ".env.example").read_text(encoding="utf-8")
        parsed = yaml.safe_load(
            "\n".join(
                f"{key}: {value}"
                for key, value in (
                    line.split("=", 1)
                    for line in env_example.splitlines()
                    if line.startswith("TRINO_") and "=" in line
                )
            )
        )
        for key in (
            "TRINO_KEYSTORE_PASSWORD",
            "TRINO_SHARED_SECRET",
            "TRINO_MARKETING_PASSWORD",
            "TRINO_ENGINEERING_PASSWORD",
        ):
            self.assertEqual(parsed[key], "CHANGE_ME")


if __name__ == "__main__":
    unittest.main(verbosity=2)
