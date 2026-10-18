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

    def test_manual_page_exposes_history_edit_and_delete_controls(self):
        response = self.client.get("/admin/pos/manual")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers.get("X-POS-Manual-Version"), "manual-pos-20261018-2")
        self.assertIn(b"Editar venta", response.data)
        self.assertIn(b"Borrar del historial", response.data)

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
        self.assertIn("Torta personalizada", message)
        self.assertIn("$20.000", message)
        self.assertIn("Decoracion adicional", message)
        self.assertIn("Datos para transferencia", message)
        self.assertIn("19284422", message)
        self.assertIn("venta manual", message.lower())
        self.assertLess(message.index("Torta personalizada"), message.index("Datos para transferencia"))

    def test_flow_manual_summary_contains_descriptions_and_amounts_before_link(self):
        snapshot = {
            "venta_id": 19,
            "manual_sale": True,
            "summary": {
                "lines": [
                    {"name": "Torta personalizada", "quantity": 1, "line_total": 20000},
                    {"name": "Decoracion adicional", "quantity": 1, "line_total": 5000},
                ],
                "subtotal": 25000,
                "discount": 2500,
                "card_fee": 788,
                "total": 23288,
            },
        }
        message = app_module._pos_admin_whatsapp_manual_summary_message(
            {"id": 19, "cliente_nombre": "Cliente Prueba"}, snapshot
        )
        message = f"{message}\n\nPaga con debito o credito de forma segura en Flow:\nhttps://flow.test/pagar"
        self.assertIn("Torta personalizada: $20.000", message)
        self.assertIn("Decoracion adicional: $5.000", message)
        self.assertIn("Descuento: -$2.500", message)
        self.assertIn("TOTAL: $23.288", message)
        self.assertLess(message.index("Torta personalizada"), message.index("https://flow.test/pagar"))

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

    def test_history_sale_can_edit_customer_title_descriptions_and_recalculate_total(self):
        created = self.client.post("/api/admin/pos/manual/guardar", json=self._sale_payload())
        sale_id = created.get_json()["venta_id"]
        edited = self.client.post(
            f"/api/admin/pos/manual/{sale_id}/editar",
            json={
                "title": "Pedido actualizado",
                "customer": {"name": "Nombre Nuevo", "phone": "+56987654321"},
                "lines": [
                    {"name": "Torta especial", "amount": 22000},
                    {"name": "Decoracion nueva", "amount": 8000},
                ],
            },
        )
        self.assertEqual(edited.status_code, 200, edited.get_json())
        self.assertTrue(edited.get_json()["financial_change"])
        self.assertEqual(edited.get_json()["snapshot"]["title"], "Pedido actualizado")
        self.assertEqual(edited.get_json()["snapshot"]["summary"]["subtotal"], 30000)
        self.assertEqual(edited.get_json()["snapshot"]["summary"]["discount"], 3000)
        self.assertEqual(edited.get_json()["snapshot"]["summary"]["total"], 27000)

        conn = self._get_db()
        sale = conn.execute("SELECT cliente_nombre, cliente_telefono, total_monto FROM ventas WHERE id=?", (sale_id,)).fetchone()
        self.assertEqual(sale["cliente_nombre"], "Nombre Nuevo")
        self.assertEqual(sale["cliente_telefono"], "+56987654321")
        self.assertEqual(sale["total_monto"], 27000)
        conn.close()

    def test_delete_removes_manual_sale_from_history_but_keeps_audit_record(self):
        created = self.client.post("/api/admin/pos/manual/guardar", json=self._sale_payload())
        sale_id = created.get_json()["venta_id"]
        deleted = self.client.post(f"/api/admin/pos/manual/{sale_id}/eliminar")
        self.assertEqual(deleted.status_code, 200, deleted.get_json())
        history = self.client.get("/api/admin/pos/manual/historial")
        self.assertEqual(history.get_json()["ventas"], [])

        conn = self._get_db()
        sale = conn.execute("SELECT estado FROM ventas WHERE id=?", (sale_id,)).fetchone()
        snapshot = conn.execute("SELECT deleted_at FROM pos_admin_snapshots WHERE venta_id=?", (sale_id,)).fetchone()
        self.assertEqual(sale["estado"], "cancelado")
        self.assertTrue(snapshot["deleted_at"])
        conn.close()

    def test_delete_rejects_manual_sale_with_flow_pending(self):
        created = self.client.post("/api/admin/pos/manual/guardar", json=self._sale_payload())
        sale_id = created.get_json()["venta_id"]
        conn = self._get_db()
        conn.execute("UPDATE ventas SET metodo_pago='flow_pendiente' WHERE id=?", (sale_id,))
        conn.commit()
        conn.close()

        deleted = self.client.post(f"/api/admin/pos/manual/{sale_id}/eliminar")
        self.assertEqual(deleted.status_code, 400)
        self.assertIn("Flow", deleted.get_json()["error"])

    def test_flow_pending_sale_allows_description_edits_but_not_amount_changes(self):
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

        conn = self._get_db()
        app_module._ensure_flow_pago_table(conn.cursor())
        conn.execute(
            "INSERT INTO tienda_flow_pagos (venta_id, commerce_order, flow_token, estado, amount) VALUES (?, ?, ?, 'pendiente', ?)",
            (sale_id, f"TEST-{sale_id}", f"TOKEN-{sale_id}", 23288),
        )
        conn.commit()
        conn.close()

        payload = {
            "title": "Titulo corregido",
            "customer": {"name": "Cliente corregido", "phone": "+56912345678"},
            "lines": [
                {"name": "Descripcion corregida", "amount": 20000},
                {"name": "Decoracion adicional", "amount": 5000},
            ],
        }
        description_edit = self.client.post(f"/api/admin/pos/manual/{sale_id}/editar", json=payload)
        self.assertEqual(description_edit.status_code, 200, description_edit.get_json())
        self.assertFalse(description_edit.get_json()["financial_change"])

        payload["lines"][0]["amount"] = 21000
        amount_edit = self.client.post(f"/api/admin/pos/manual/{sale_id}/editar", json=payload)
        self.assertEqual(amount_edit.status_code, 400)
        self.assertIn("montos", amount_edit.get_json()["error"])


if __name__ == "__main__":
    unittest.main()
