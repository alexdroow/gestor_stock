from io import BytesIO
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import insumos_boletas
import app as app_module
import database
from PIL import Image


class InsumosBoletasTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp.name) / "boletas.sqlite"

        def connect_db():
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            return conn

        self.connect_db = connect_db
        conn = connect_db()
        conn.executescript("""
            CREATE TABLE insumos (
                id INTEGER PRIMARY KEY, nombre TEXT, stock REAL, stock_minimo REAL, unidad TEXT,
                precio_unitario REAL, cantidad_comprada REAL, unidad_compra TEXT,
                precio_incluye_iva INTEGER, precio_unitario_anterior REAL,
                cantidad_comprada_anterior REAL, unidad_compra_anterior TEXT,
                precio_incluye_iva_anterior INTEGER
            );
            CREATE TABLE insumo_precio_historial (
                id INTEGER PRIMARY KEY AUTOINCREMENT, insumo_id INTEGER, precio_unitario REAL,
                cantidad_comprada REAL, unidad_compra TEXT, precio_incluye_iva INTEGER,
                precio_unitario_base REAL, variacion_monto_base REAL, variacion_pct_base REAL,
                origen_modulo TEXT, codigo_operacion TEXT, creado TEXT DEFAULT CURRENT_TIMESTAMP
            );
            INSERT INTO insumos VALUES (1, 'Harina 000', 12, 2, 'kg', 720, 1, 'kg', 1, NULL, NULL, NULL, NULL);
        """)
        conn.commit()
        conn.close()
        self.db_patch = patch("insumos_boletas.get_db", side_effect=connect_db)
        self.db_patch.start()
        insumos_boletas.ensure_schema()
        insumos_boletas.create_receipt("receipt-1", "abc123", "boleta.jpg", "/private/boleta.jpg", "HARINA 000 $800")

    def tearDown(self):
        self.db_patch.stop()
        self.temp.cleanup()

    def line(self, **overrides):
        row = {
            "confirmar": True, "insumo_id": 1, "precio_actual": 720,
            "cantidad_actual": 1, "unidad_actual": "kg", "iva_actual": True,
            "precio_unitario": 800, "cantidad_comprada": 1, "unidad_compra": "kg",
            "precio_incluye_iva": True, "texto_original": "HARINA 000 $800",
        }
        row.update(overrides)
        return row

    def test_confirm_updates_only_price_and_preserves_stock_with_history(self):
        result = insumos_boletas.confirm_receipt("receipt-1", [self.line()], "Administrador")
        self.assertEqual(result, ["Harina 000"])
        conn = self.connect_db()
        try:
            item = conn.execute("SELECT * FROM insumos WHERE id=1").fetchone()
            history = conn.execute("SELECT * FROM insumo_precio_historial").fetchone()
            receipt = conn.execute("SELECT estado, usuario_nombre, lineas_json FROM insumos_boletas WHERE id='receipt-1'").fetchone()
        finally:
            conn.close()
        self.assertEqual(item["stock"], 12)
        self.assertEqual(item["precio_unitario"], 800)
        self.assertEqual(item["cantidad_comprada"], 1)
        self.assertEqual(history["origen_modulo"], "boleta_insumos")
        self.assertEqual(receipt["estado"], "confirmada")
        self.assertEqual(receipt["usuario_nombre"], "Administrador")
        self.assertEqual(len(__import__("json").loads(receipt["lineas_json"])), 1)

    def test_price_conflict_rolls_back_every_line_and_keeps_receipt_pending(self):
        bad = self.line(precio_actual=700)
        with self.assertRaisesRegex(ValueError, "cambió"):
            insumos_boletas.confirm_receipt("receipt-1", [self.line(), bad])
        conn = self.connect_db()
        try:
            self.assertEqual(conn.execute("SELECT precio_unitario FROM insumos WHERE id=1").fetchone()[0], 720)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM insumo_precio_historial").fetchone()[0], 0)
            self.assertEqual(conn.execute("SELECT estado FROM insumos_boletas WHERE id='receipt-1'").fetchone()[0], "revision")
        finally:
            conn.close()

    def test_duplicate_confirm_is_rejected(self):
        insumos_boletas.confirm_receipt("receipt-1", [self.line()], "Admin")
        with self.assertRaisesRegex(ValueError, "ya fue confirmada"):
            insumos_boletas.confirm_receipt("receipt-1", [self.line(precio_actual=800)], "Admin")

    def test_reversal_restores_purchase_price_without_changing_stock(self):
        insumos_boletas.confirm_receipt("receipt-1", [self.line()], "Admin")
        result = insumos_boletas.reverse_receipt("receipt-1", "Admin")
        self.assertEqual(result, ["Harina 000"])
        conn = self.connect_db()
        try:
            item = conn.execute("SELECT precio_unitario, cantidad_comprada, unidad_compra, stock FROM insumos WHERE id=1").fetchone()
            history_count = conn.execute("SELECT COUNT(*) FROM insumo_precio_historial").fetchone()[0]
            state = conn.execute("SELECT estado FROM insumos_boletas WHERE id='receipt-1'").fetchone()[0]
        finally:
            conn.close()
        self.assertEqual(item["precio_unitario"], 720)
        self.assertEqual(item["cantidad_comprada"], 1)
        self.assertEqual(item["unidad_compra"], "kg")
        self.assertEqual(item["stock"], 12)
        self.assertEqual(history_count, 2)
        self.assertEqual(state, "revertida")

    def test_reversal_does_not_overwrite_a_later_price_change(self):
        insumos_boletas.confirm_receipt("receipt-1", [self.line()], "Admin")
        conn = self.connect_db()
        conn.execute("UPDATE insumos SET precio_unitario=900 WHERE id=1")
        conn.commit()
        conn.close()
        with self.assertRaisesRegex(ValueError, "modificado despu"):
            insumos_boletas.reverse_receipt("receipt-1", "Admin")
        conn = self.connect_db()
        try:
            self.assertEqual(conn.execute("SELECT precio_unitario FROM insumos WHERE id=1").fetchone()[0], 900)
            self.assertEqual(conn.execute("SELECT estado FROM insumos_boletas WHERE id='receipt-1'").fetchone()[0], "confirmada")
        finally:
            conn.close()

    def test_matching_is_conservative_when_two_names_are_similar(self):
        items = [{"id": 1, "nombre": "Azucar Flor"}, {"id": 2, "nombre": "Azucar Flor Justo"}]
        self.assertEqual(insumos_boletas.candidate_matches("azucar flor justo 1kg", items), [])
        exact = insumos_boletas.candidate_matches("Azucar Flor Justo", items)
        self.assertEqual(exact[0]["id"], 2)


class InsumosBoletasUploadRouteTests(unittest.TestCase):
    def test_same_pending_pdf_can_be_reopened_to_run_browser_ocr(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "upload.sqlite"
            with patch.object(database, "DB_PATH", str(db_path)), patch.object(app_module, "DATA_DIR", temp_dir):
                client = app_module.app.test_client()
                with client.session_transaction() as session:
                    session[app_module._ADMIN_SESSION_KEY] = True
                    session[app_module._ADMIN_USER_NAME_SESSION_KEY] = "Admin"
                image = Image.new("RGB", (8, 8), color="white")
                uploaded_image = BytesIO()
                image.save(uploaded_image, format="JPEG")
                image_bytes = uploaded_image.getvalue()
                first = client.post(
                    "/api/insumos/boletas/cargar",
                    data={"archivo": (BytesIO(image_bytes), "factura-escaneada.jpg")},
                    content_type="multipart/form-data",
                )
                self.assertEqual(first.status_code, 200)
                first_data = first.get_json()
                self.assertTrue(first_data["success"])

                second = client.post(
                    "/api/insumos/boletas/cargar",
                    data={"archivo": (BytesIO(image_bytes), "factura-escaneada.jpg")},
                    content_type="multipart/form-data",
                )
                self.assertEqual(second.status_code, 200)
                second_data = second.get_json()
                self.assertTrue(second_data["duplicate_pending"])
                self.assertEqual(second_data["boleta_id"], first_data["boleta_id"])

                conn = sqlite3.connect(db_path)
                try:
                    self.assertEqual(conn.execute("SELECT COUNT(*) FROM insumos_boletas").fetchone()[0], 1)
                finally:
                    conn.close()


if __name__ == "__main__":
    unittest.main()
