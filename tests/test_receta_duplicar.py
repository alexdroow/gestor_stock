import unittest
from unittest.mock import patch

import app


class DuplicateRecipeTests(unittest.TestCase):
    def setUp(self):
        self.client = app.app.test_client()
        with self.client.session_transaction() as session:
            session[app._ADMIN_SESSION_KEY] = True

    def test_duplicate_clones_recipe_metadata_and_mixed_components(self):
        original = {
            "id": 8,
            "nombre": "Bizcocho vainilla",
            "producto_id": 14,
            "rendimiento": 3.5,
            "items": [
                {"tipo": "insumo", "recurso_id": 21, "cantidad": 450, "unidad": "gr"},
                {"tipo": "producto", "recurso_id": 32, "cantidad": 2, "unidad": "unidad"},
            ],
        }
        with (
            patch.object(app, "obtener_receta_detalle", return_value=original),
            patch.object(app, "guardar_receta", return_value=55) as save_recipe,
            patch.object(app, "crear_backup") as backup,
        ):
            response = self.client.post("/api/receta/8/duplicar")

        self.assertEqual(response.status_code, 201)
        self.assertEqual(response.get_json()["receta_id"], 55)
        save_recipe.assert_called_once_with(
            "Bizcocho vainilla (copia)",
            14,
            [
                {"tipo": "insumo", "id": 21, "cantidad": 450, "unidad": "gr"},
                {"tipo": "producto", "id": 32, "cantidad": 2, "unidad": "unidad"},
            ],
            rendimiento=3.5,
        )
        backup.assert_called_once()
        self.assertEqual(original["nombre"], "Bizcocho vainilla")

    def test_duplicate_returns_not_found_when_original_does_not_exist(self):
        with (
            patch.object(app, "obtener_receta_detalle", return_value=None),
            patch.object(app, "guardar_receta") as save_recipe,
        ):
            response = self.client.post("/api/receta/999/duplicar")

        self.assertEqual(response.status_code, 404)
        save_recipe.assert_not_called()


if __name__ == "__main__":
    unittest.main()
