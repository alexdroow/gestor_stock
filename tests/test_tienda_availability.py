import sqlite3
import unittest
from unittest.mock import patch

import app as app_module


class TiendaAvailabilityTests(unittest.TestCase):
    def test_product_availability_uses_its_own_unit_stock_not_ingredient_quantity(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.executescript(
            """
            CREATE TABLE productos (
                id INTEGER PRIMARY KEY, nombre TEXT, stock REAL, stock_minimo REAL,
                unidad TEXT, porcion_cantidad REAL, porcion_unidad TEXT,
                stock_dependencia_tipo TEXT, stock_dependencia_id INTEGER,
                stock_dependencia_cantidad REAL, eliminado INTEGER, activo_tienda INTEGER
            );
            CREATE TABLE insumos (
                id INTEGER PRIMARY KEY, nombre TEXT, stock REAL, stock_minimo REAL, unidad TEXT
            );
            CREATE TABLE producto_insumos_venta (
                id INTEGER PRIMARY KEY, producto_id INTEGER, insumo_id INTEGER,
                cantidad REAL, unidad TEXT
            );
            CREATE TABLE producto_productos_venta (
                id INTEGER PRIMARY KEY, producto_id INTEGER,
                producto_asociado_id INTEGER, cantidad REAL
            );
            CREATE TABLE producto_desactivaciones_manuales (
                producto_id INTEGER PRIMARY KEY, confirmado_en TEXT
            );
            INSERT INTO insumos VALUES (7, 'Stock de minitortas', 6, 0, 'unidad');
            INSERT INTO productos VALUES
                (1, 'Minitorta cuatro leches', 0, 0, 'unidad', 1, 'unidad', 'insumo', 7, 1, 0, 1),
                (2, 'Producto realmente agotado', 0, 0, 'unidad', 1, 'unidad', NULL, NULL, 1, 0, 1);
            """
        )
        try:
            with patch.object(app_module, "get_db", return_value=conn):
                productos = app_module._obtener_productos_para_venta()

            self.assertEqual(productos, [])
        finally:
            conn.close()

    def test_admin_catalog_reports_the_same_effective_availability_as_store(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.execute(
            """CREATE TABLE productos (
                id INTEGER PRIMARY KEY, nombre TEXT, eliminado INTEGER,
                categoria_tienda TEXT, orden_tienda INTEGER
            )"""
        )
        conn.execute(
            "INSERT INTO productos VALUES (1, 'Minitorta tres leches', 0, 'Tortas', 1)"
        )
        efectivo = {
            "id": 1,
            "nombre": "Minitorta tres leches",
            "precio": 12500,
            "stock": 100,
            "unidad": "unidad",
            "porcion_cantidad": 1,
            "porcion_unidad": "unidad",
            "porciones_disponibles": 100,
            "porciones_disponibles_base": 100,
            "activo_tienda": 1,
        }
        try:
            with (
                patch.object(app_module, "get_db", return_value=conn),
                patch.object(app_module, "_obtener_productos_para_venta", return_value=[efectivo]),
                patch.object(app_module, "_cargar_categorias_tienda", return_value=[]),
            ):
                response = app_module.app.test_client().get("/api/tienda/admin/productos")

            self.assertEqual(response.status_code, 200)
            producto = response.get_json()["productos"][0]
            self.assertEqual(producto["max_compra"], 100)
            self.assertEqual(producto["stock_porciones_disponibles"], 100)
        finally:
            conn.close()

    def test_legacy_stock_100_kg_is_available_as_100_finished_items(self):
        info = app_module._calcular_info_porciones_producto({
            "stock": 100,
            "stock_minimo": 5,
            "unidad": "kg",
            "porcion_cantidad": 160,
            "porcion_unidad": "kg",
        })
        self.assertEqual(info["porciones_disponibles"], 100)
        self.assertEqual(info["unidad_stock"], "unidad")
        self.assertEqual(info["porcion_cantidad"], 1)
        self.assertFalse(info["sin_porcion_disponible"])


if __name__ == "__main__":
    unittest.main()
