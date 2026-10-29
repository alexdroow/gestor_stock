import os
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

import app as app_module
import database


class ProductUnitStockTests(unittest.TestCase):
    def test_stock_values_must_be_whole_units(self):
        self.assertEqual(database._entero_producto("100", "stock"), 100)
        with self.assertRaises(ValueError):
            database._entero_producto("2.5", "stock")
        with self.assertRaises(ValueError):
            database._entero_producto("-1", "stock")

    def test_store_admin_quick_edit_persists_exact_integer_stock_as_units(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = os.path.join(temp_dir, "quick-edit.sqlite")
            conn = sqlite3.connect(path)
            conn.row_factory = sqlite3.Row
            conn.executescript(
                """
                CREATE TABLE productos (
                    id INTEGER PRIMARY KEY, nombre TEXT, stock REAL, fecha_vencimiento TEXT,
                    precio REAL, unidad TEXT, porcion_cantidad REAL, porcion_unidad TEXT,
                    activo_tienda INTEGER, eliminado INTEGER DEFAULT 0,
                    actualizado_en TEXT DEFAULT CURRENT_TIMESTAMP
                );
                CREATE TABLE producto_lotes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, producto_id INTEGER,
                    cantidad REAL, fecha_vencimiento TEXT
                );
                INSERT INTO productos(id,nombre,stock,unidad,porcion_cantidad,porcion_unidad,activo_tienda)
                VALUES(1,'Minitorta Cuatro Leches',0,'kg',160,'kg',1);
                """
            )
            conn.commit()
            conn.close()

            def open_db():
                db = sqlite3.connect(path)
                db.row_factory = sqlite3.Row
                return db

            client = app_module.app.test_client()
            with client.session_transaction() as session:
                session[app_module._ADMIN_SESSION_KEY] = True
            with (
                patch.object(app_module, "get_db", side_effect=open_db),
                patch.object(database, "get_db", side_effect=open_db),
                patch.object(database, "registrar_movimiento_stock"),
                patch.object(app_module, "crear_backup"),
            ):
                response = client.post(
                    "/api/tienda/admin/producto/1/quick-update",
                    json={"precio": 2800, "stock": 25, "oculto": False},
                )

            self.assertEqual(response.status_code, 200, response.get_json())
            self.assertEqual(response.get_json()["producto"]["stock"], 25)
            conn = open_db()
            saved = conn.execute(
                "SELECT stock,unidad,porcion_cantidad,porcion_unidad FROM productos WHERE id=1"
            ).fetchone()
            lot_stock = conn.execute("SELECT SUM(cantidad) FROM producto_lotes WHERE producto_id=1").fetchone()[0]
            conn.close()
            self.assertEqual(tuple(saved), (25, "unidad", 1, "unidad"))
            self.assertEqual(lot_stock, 25)

    def test_migration_normalizes_stock_and_keeps_audit_of_previous_values(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = os.path.join(temp_dir, "stock.sqlite")
            conn = sqlite3.connect(path)
            conn.row_factory = sqlite3.Row
            conn.executescript(
                """
                CREATE TABLE productos (
                    id INTEGER PRIMARY KEY, nombre TEXT, stock REAL,
                    unidad TEXT, stock_minimo REAL,
                    porcion_cantidad REAL, porcion_unidad TEXT
                );
                CREATE TABLE producto_lotes (
                    id INTEGER PRIMARY KEY, producto_id INTEGER,
                    cantidad INTEGER, fecha_vencimiento TEXT
                );
                INSERT INTO productos VALUES
                    (1, 'Minitorta Cuatro Leches', 100, 'kg', 4, 160, 'kg'),
                    (2, 'Producto fraccionario', 3.7, 'gr', 0.5, 50, 'gr');
                INSERT INTO producto_lotes VALUES
                    (1, 1, 40, '2026-11-01'), (2, 1, 60, '2026-12-01'),
                    (3, 2, 4, '2026-11-01');
                """
            )
            conn.commit()
            conn.close()

            def open_db():
                db = sqlite3.connect(path)
                db.row_factory = sqlite3.Row
                return db

            with patch.object(database, "get_db", side_effect=open_db):
                result = database.migrar_productos_a_unidades()
                second_run = database.migrar_productos_a_unidades()

            conn = open_db()
            rows = conn.execute(
                "SELECT id, stock, unidad, stock_minimo, porcion_cantidad, porcion_unidad FROM productos ORDER BY id"
            ).fetchall()
            audit = conn.execute(
                "SELECT stock_anterior, unidad_anterior, stock_nuevo, unidades_fraccionarias_descartadas FROM productos_unidad_migracion ORDER BY producto_id"
            ).fetchall()
            lots = conn.execute(
                "SELECT producto_id, SUM(cantidad) cantidad FROM producto_lotes GROUP BY producto_id ORDER BY producto_id"
            ).fetchall()
            conn.close()

            self.assertEqual(result["productos_migrados"], 2)
            self.assertEqual(second_run["productos_migrados"], 0)
            self.assertEqual((rows[0]["stock"], rows[0]["unidad"], rows[0]["porcion_cantidad"], rows[0]["porcion_unidad"]), (100, "unidad", 1, "unidad"))
            self.assertEqual(rows[1]["stock"], 3)
            self.assertEqual(rows[1]["stock_minimo"], 1)
            self.assertEqual([row["cantidad"] for row in lots], [100, 3])
            self.assertEqual(audit[0]["stock_anterior"], 100)
            self.assertEqual(audit[0]["unidad_anterior"], "kg")
            self.assertAlmostEqual(audit[1]["unidades_fraccionarias_descartadas"], 0.7)


if __name__ == "__main__":
    unittest.main()
