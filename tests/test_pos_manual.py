import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import app as app_module


class ManualPosTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "manual-pos.sqlite"
        self._create_database()
        self.get_db_patch = patch.object(app_module, "get_db", self._get_db)
        self.backup_patch = patch.object(app_module, "crear_backup", lambda: None)
        self.flow_log_patch = patch.object(app_module, "_flow_log", lambda *args, **kwargs: None)
        self.get_db_patch.start()
        self.backup_patch.start()
        self.flow_log_patch.start()
        self.client = app_module.app.test_client()
        with self.client.session_transaction() as session:
            session[app_module._ADMIN_SESSION_KEY] = True
            session[app_module._ADMIN_USER_NAME_SESSION_KEY] = "Tester"

    def tearDown(self):
        self.flow_log_patch.stop()
        self.backup_patch.stop()
        self.get_db_patch.stop()
        self.temp_dir.cleanup()

    def _get_db(self):
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def _create_database(self):
        conn = sqlite3.connect(self.db_path)
        conn.executescript(
            """
            CREATE TABLE ventas (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fecha_hora TEXT DEFAULT CURRENT_TIMESTAMP,
                codigo_pedido TEXT,
                canal_venta TEXT DEFAULT 'presencial',
                codigo_operacion TEXT,
                total_items INTEGER DEFAULT 0,
                total_monto REAL DEFAULT 0,
                estado TEXT DEFAULT 'completada'
            );
            CREATE TABLE productos (id INTEGER PRIMARY KEY, nombre TEXT, stock REAL);
            INSERT INTO productos (id, nombre, stock) VALUES (1, 'Producto de catalogo', 5);
            """
        )
        conn.commit()
        conn.close()

    def _sale_payload(self, request_id="manual-req-1", payment_method="transfer"):
        return {
            "request_id": request_id,
            "title": "Encargo especial",
            "lines": [
                {"name": "Torta personalizada", "amount": 20000},
                {"name": "Decoracion adicional", "amount": 5000},
            ],
            "discount": {"type": "percent", "value": 10, "reason": "Promocion"},
            "payment_method": payment_method,
            "customer": {"name": "Cliente Prueba", "phone": "91234567"},
        }

    def test_manual_summary_applies_discount_and_debit_fee(self):
        with patch.object(app_module, "_pos_admin_card_fee_pct", return_value=3.5):
            summary = app_module._pos_admin_manual_summary(self._sale_payload(payment_method="card"))
        self.assertEqual(summary["subtotal"], 25000)
        self.assertEqual(summary["discount"], 2500)
        self.assertEqual(summary["card_fee"], 788)
        self.assertEqual(summary["total"], 23288)
        self.assertTrue(summary["manual_sale"])

    def test_manual_sale_is_idempotent_and_does_not_change_catalog_stock(self):
        payload = self._sale_payload()
        first = self.client.post("/api/admin/pos/manual/guardar", json=payload)
        second = self.client.post("/api/admin/pos/manual/guardar", json=payload)
        self.assertEqual(first.status_code, 200, first.get_json())
        self.assertEqual(second.status_code, 200, second.get_json())
        self.assertFalse(first.get_json()["idempotent"] if "idempotent" in first.get_json() else False)
        self.assertTrue(second.get_json()["idempotent"])
        self.assertEqual(first.get_json()["venta_id"], second.get_json()["venta_id"])

        conn = self._get_db()
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM ventas").fetchone()[0], 1)
        self.assertEqual(conn.execute("SELECT stock FROM productos WHERE id=1").fetchone()[0], 5)
        self.assertEqual(conn.execute("SELECT canal_venta FROM ventas").fetchone()[0], "pos_admin_manual")
        conn.close()

    def test_history_and_whatsapp_include_manual_summary_and_transfer_details(self):
        created = self.client.post("/api/admin/pos/manual/guardar", json=self._sale_payload())
        self.assertEqual(created.status_code, 200, created.get_json())
        sale_id = created.get_json()["venta_id"]

        history = self.client.get("/api/admin/pos/manual/historial")
        self.assertEqual(history.status_code, 200)
        self.assertEqual(len(history.get_json()["ventas"]), 1)
        self.assertTrue(history.get_json()["ventas"][0]["snapshot"]["manual_sale"])

        whatsapp = self.client.post(f"/api/admin/pos/{sale_id}/whatsapp", json={})
        self.assertEqual(whatsapp.status_code, 200, whatsapp.get_json())
        message = whatsapp.get_json()["message"]
        self.assertIn("Datos para transferencia", message)
        self.assertIn("19284422", message)
        self.assertIn("venta manual", message.lower())

    def test_manual_payment_method_can_change_before_flow_link_is_created(self):
        created = self.client.post("/api/admin/pos/manual/guardar", json=self._sale_payload())
        sale_id = created.get_json()["venta_id"]
        with patch.object(app_module, "_flow_cfg", return_value={"enabled": True}), patch.object(
            app_module, "_flow_checkout_public_enabled", return_value=True
        ):
            changed = self.client.post(
                f"/api/admin/pos/manual/{sale_id}/metodo",
                json={"payment_method": "card", "email": "cliente@example.com"},
            )
        self.assertEqual(changed.status_code, 200, changed.get_json())
        self.assertEqual(changed.get_json()["snapshot"]["checkout"]["payment_method"], "card")

        conn = self._get_db()
        sale = conn.execute("SELECT metodo_pago, cliente_email FROM ventas WHERE id=?", (sale_id,)).fetchone()
        self.assertEqual(sale["metodo_pago"], "flow_pendiente")
        self.assertEqual(sale["cliente_email"], "cliente@example.com")
        conn.close()


if __name__ == "__main__":
    unittest.main()
