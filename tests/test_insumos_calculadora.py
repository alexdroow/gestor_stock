import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import app as app_module
import database


class CalculadoraInsumosTests(unittest.TestCase):
    def test_recipe_unit_conversions_and_incompatible_units(self):
        self.assertAlmostEqual(app_module._convertir_unidad_calculadora(3960, "g", "kg"), 3.96)
        self.assertAlmostEqual(app_module._convertir_unidad_calculadora(1200, "cc", "L"), 1.2)
        self.assertAlmostEqual(app_module._convertir_unidad_calculadora(2, "docena", "unidad"), 24)
        self.assertIsNone(app_module._convertir_unidad_calculadora(2, "caja", "unidad"))
        self.assertIsNone(app_module._convertir_unidad_calculadora(0, "kg", "g"))

    def test_iva_totals_respect_whether_source_price_includes_tax(self):
        sample_lines = [(20, 720), (5, 1244), (2, 2647)]
        gross_total = sum(qty * price for qty, price in sample_lines)
        self.assertEqual(gross_total, 25914)

        net_included, gross_included = database._totales_compra(gross_total, 1, True)
        net_excluded, gross_excluded = database._totales_compra(gross_total, 1, False)
        self.assertAlmostEqual(net_included, gross_total / 1.19)
        self.assertEqual(gross_included, gross_total)
        self.assertEqual(net_excluded, gross_total)
        self.assertAlmostEqual(gross_excluded, gross_total * 1.19)

    def test_price_editor_comparison_for_both_iva_treatments(self):
        package_price = 15992

        net_when_included, payable_when_included = database._totales_compra(
            1, package_price, True
        )
        net_when_excluded, payable_when_excluded = database._totales_compra(
            1, package_price, False
        )

        self.assertAlmostEqual(net_when_included, 15992 / 1.19)
        self.assertEqual(payable_when_included, 15992)
        self.assertEqual(net_when_excluded, 15992)
        self.assertAlmostEqual(payable_when_excluded, 19030.48)

    def test_price_editor_preserves_decimal_purchase_values(self):
        price = 1243.6975
        net_when_included, payable_when_included = database._totales_compra(
            4, price, True
        )
        net_when_excluded, payable_when_excluded = database._totales_compra(
            4, price, False
        )

        self.assertAlmostEqual(payable_when_included, 4974.79)
        self.assertAlmostEqual(net_when_included, 4974.79 / 1.19)
        self.assertAlmostEqual(net_when_excluded, 4974.79)
        self.assertAlmostEqual(payable_when_excluded, 4974.79 * 1.19)

    def test_pending_rows_merge_only_when_unit_price_and_tax_snapshot_match(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "pending.sqlite"

            def connect_db():
                conn = sqlite3.connect(db_path)
                conn.row_factory = sqlite3.Row
                return conn

            conn = connect_db()
            try:
                conn.execute(
                    """CREATE TABLE compras_pendientes (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        insumo_id INTEGER, nombre TEXT, cantidad REAL, unidad TEXT,
                        precio_unitario REAL, precio_incluye_iva INTEGER,
                        estado TEXT, nota TEXT, actualizado TEXT
                    )"""
                )
                conn.commit()
            finally:
                conn.close()

            batch = [
                {"insumo_id": 8, "nombre": "Harina", "cantidad": 20, "unidad": "kg", "precio_unitario": 720, "precio_incluye_iva": False},
                {"insumo_id": 8, "nombre": "Harina", "cantidad": 5, "unidad": "kg", "precio_unitario": 720, "precio_incluye_iva": False},
                {"insumo_id": 8, "nombre": "Harina", "cantidad": 1, "unidad": "kg", "precio_unitario": 800, "precio_incluye_iva": False},
                {"insumo_id": 8, "nombre": "Harina", "cantidad": 1, "unidad": "kg", "precio_unitario": 720, "precio_incluye_iva": True},
            ]
            with patch.object(database, "get_db", connect_db):
                result = database.agregar_lote_compras_pendientes(batch, combinar=True)

            self.assertTrue(result["success"])
            conn = connect_db()
            try:
                rows = conn.execute(
                    "SELECT cantidad, unidad, precio_unitario, precio_incluye_iva FROM compras_pendientes ORDER BY id"
                ).fetchall()
            finally:
                conn.close()
            self.assertEqual(len(rows), 3)
            self.assertEqual(rows[0]["cantidad"], 25)
            self.assertEqual(rows[0]["precio_unitario"], 720)
            self.assertEqual(rows[0]["precio_incluye_iva"], 0)
            self.assertEqual(rows[1]["precio_unitario"], 800)
            self.assertEqual(rows[2]["precio_incluye_iva"], 1)


if __name__ == "__main__":
    unittest.main()
