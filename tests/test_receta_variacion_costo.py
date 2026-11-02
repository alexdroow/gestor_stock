import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import database


class RecetaVariacionCostoTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "recipes.sqlite"
        conn = self._connect()
        conn.executescript(
            """
            CREATE TABLE recetas (id INTEGER PRIMARY KEY, rendimiento REAL);
            CREATE TABLE productos (id INTEGER PRIMARY KEY, nombre TEXT, precio REAL);
            CREATE TABLE insumos (
                id INTEGER PRIMARY KEY, nombre TEXT, unidad TEXT, precio_unitario REAL,
                cantidad_comprada REAL, unidad_compra TEXT, precio_incluye_iva INTEGER,
                precio_unitario_anterior REAL, cantidad_comprada_anterior REAL,
                unidad_compra_anterior TEXT, precio_incluye_iva_anterior INTEGER
            );
            CREATE TABLE receta_items (
                id INTEGER PRIMARY KEY, receta_id INTEGER, tipo TEXT, insumo_id INTEGER,
                producto_id INTEGER, cantidad REAL, unidad TEXT
            );
            INSERT INTO recetas VALUES (1, 1);
            INSERT INTO insumos VALUES (1, 'Chocolate', 'kg', 120, 1, 'kg', 1, 100, 1, 'kg', 1);
            INSERT INTO receta_items VALUES (1, 1, 'insumo', 1, NULL, 1, 'kg');
            """
        )
        conn.commit()
        conn.close()
        self.db_patch = patch.object(database, "get_db", side_effect=self._connect)
        self.db_patch.start()

    def tearDown(self):
        self.db_patch.stop()
        self.temp_dir.cleanup()

    def _connect(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _set_prices(self, current, previous):
        conn = self._connect()
        conn.execute(
            "UPDATE insumos SET precio_unitario = ?, precio_unitario_anterior = ? WHERE id = 1",
            (current, previous),
        )
        conn.commit()
        conn.close()

    def test_recipe_total_marks_increase_decrease_and_neutral(self):
        result = database.calcular_costo_receta(1)
        self.assertEqual(result["costo_total"], 120)
        self.assertEqual(result["costo_total_anterior"], 100)
        self.assertEqual(result["variacion_tipo"], "alza")
        self.assertEqual(result["variacion_monto"], 20)

        self._set_prices(80, 100)
        result = database.calcular_costo_receta(1)
        self.assertEqual(result["variacion_tipo"], "baja")
        self.assertEqual(result["variacion_monto"], -20)

        self._set_prices(100, 100)
        result = database.calcular_costo_receta(1)
        self.assertEqual(result["variacion_tipo"], "sin_cambio")
        self.assertEqual(result["variacion_monto"], 0)


if __name__ == "__main__":
    unittest.main()
