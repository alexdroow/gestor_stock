import os
import sqlite3
import unittest
from unittest.mock import patch

import app


class FlowEnvironmentTests(unittest.TestCase):
    def test_production_uses_official_api_and_production_credentials(self):
        env = {
            "FLOW_ENV": "production",
            "FLOW_API_URL": "https://www.flow.cl/api",
            "FLOW_PRODUCTION_API_KEY": "prod-api-key",
            "FLOW_PRODUCTION_SECRET_KEY": "prod-secret",
        }
        with patch.dict(os.environ, env, clear=True):
            cfg = app._flow_cfg()
        self.assertTrue(cfg["enabled"])
        self.assertEqual(cfg["environment"], "production")
        self.assertEqual(cfg["api_url"], "https://www.flow.cl/api")

    def test_sandbox_remains_available_with_sandbox_credentials(self):
        env = {
            "FLOW_ENV": "sandbox",
            "FLOW_API_URL": "https://sandbox.flow.cl/api",
            "FLOW_API_KEY": "sandbox-api-key",
            "FLOW_SECRET_KEY": "sandbox-secret",
        }
        with patch.dict(os.environ, env, clear=True):
            cfg = app._flow_cfg()
        self.assertTrue(cfg["enabled"])
        self.assertEqual(cfg["environment"], "sandbox")

    def test_environment_and_api_url_must_match(self):
        env = {
            "FLOW_ENV": "production",
            "FLOW_API_URL": "https://sandbox.flow.cl/api",
            "FLOW_API_KEY": "api-key",
            "FLOW_SECRET_KEY": "secret",
        }
        with patch.dict(os.environ, env, clear=True):
            cfg = app._flow_cfg()
        self.assertFalse(cfg["enabled"])
        self.assertTrue(cfg["configuration_error"])

    def test_sandbox_keys_cannot_enable_production(self):
        env = {
            "FLOW_ENV": "production",
            "FLOW_API_URL": "https://www.flow.cl/api",
            "FLOW_API_KEY": "sandbox-api-key",
            "FLOW_SECRET_KEY": "sandbox-secret",
        }
        with patch.dict(os.environ, env, clear=True):
            cfg = app._flow_cfg()
        self.assertFalse(cfg["enabled"])

    def test_redirect_allowlist_matches_environment(self):
        self.assertIn("api.flow.cl?token=", app._flow_payment_redirect_url("https://api.flow.cl", "t", "production"))
        self.assertIn("www.flow.cl/app/pay?token=", app._flow_payment_redirect_url("https://www.flow.cl/app/pay", "t", "production"))
        self.assertIn("sandbox.flow.cl/app/web?token=", app._flow_payment_redirect_url("https://sandbox.flow.cl/app/web", "t", "sandbox"))
        with self.assertRaises(ValueError):
            app._flow_payment_redirect_url("https://sandbox.flow.cl/app/web", "t", "production")
        with self.assertRaises(ValueError):
            app._flow_payment_redirect_url("https://api.flow.cl.evil.example/pay", "t", "production")

    def test_legacy_payment_rows_keep_their_environment(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.execute("CREATE TABLE tienda_flow_pagos (id INTEGER, flow_token TEXT, flow_redirect_url TEXT)")
        conn.executemany(
            "INSERT INTO tienda_flow_pagos (id, flow_token, flow_redirect_url) VALUES (?, ?, ?)",
            [
                (1, "sandbox-token", "https://sandbox.flow.cl/app/web?token=sandbox-token"),
                (2, "production-token", "https://api.flow.cl?token=production-token"),
                (3, "SIM-3", "https://pasteleriasucree.cl/tienda?flow=paid"),
            ],
        )
        app._ensure_flow_pago_table(conn.cursor())
        rows = dict(conn.execute("SELECT id, flow_environment FROM tienda_flow_pagos").fetchall())
        self.assertEqual(rows, {1: "sandbox", 2: "production", 3: "simulation"})
        conn.close()


if __name__ == "__main__":
    unittest.main()
