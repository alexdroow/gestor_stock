import sqlite3
import unittest
from unittest.mock import patch

import app as app_module


class TiendaAvailabilityTests(unittest.TestCase):
    def test_products_with_zero_local_stock_use_their_effective_dependency_stock(self):
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

            self.assertEqual([p["nombre"] for p in productos], ["Minitorta cuatro leches"])
            self.assertEqual(productos[0]["stock"], 0)
            self.assertEqual(productos[0]["stock_visual"], 6)
            self.assertEqual(productos[0]["porciones_disponibles"], 6)
            serializado = app_module._serializar_producto_tienda(productos[0])
            self.assertEqual(serializado["max_compra"], 6)
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
