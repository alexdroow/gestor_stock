import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import app as app_module
import privacidad


class PrivacyImplementationTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "privacy.sqlite"

        def connect_db():
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA foreign_keys=ON")
            return conn

        self.connect_db = connect_db
        self.schema_patch = patch.object(privacidad, "get_db", side_effect=connect_db)
        self.schema_patch.start()
        privacidad._schema_ready = False
        self.client = app_module.app.test_client()

    def tearDown(self):
        self.schema_patch.stop()
        privacidad._schema_ready = False
        self.temp_dir.cleanup()

    def _csrf(self):
        return self.client.get("/api/privacidad/preferencias").get_json()["csrf_token"]

    def test_public_pages_are_available_without_login_and_policy_is_not_fabricated(self):
        for path in ("/privacidad", "/privacidad/politica", "/privacidad/preferencias", "/privacidad/derechos", "/privacidad/seguimiento"):
            response = self.client.get(path)
            self.assertEqual(response.status_code, 200, path)
        policy = self.client.get("/privacidad/politica").get_data(as_text=True)
        self.assertIn("Aun no hay una politica aprobada", policy)
        self.assertNotIn("[razon social]", policy.lower())

    def test_optional_preferences_default_off_then_are_recorded_and_can_be_revoked(self):
        csrf = self._csrf()
        first = self.client.post("/api/privacidad/preferencias", json={"analytics_internal": True}, headers={"X-CSRF-Token": csrf})
        self.assertEqual(first.status_code, 200)
        self.assertTrue(first.get_json()["preferences"]["analytics_internal"])
        self.assertTrue(first.headers.get("Set-Cookie", "").startswith("sucree_privacy="))

        revoked = self.client.post("/api/privacidad/preferencias", json={}, headers={"X-CSRF-Token": csrf})
        self.assertEqual(revoked.status_code, 200)
        self.assertFalse(revoked.get_json()["preferences"]["analytics_internal"])
        conn = self.connect_db()
        try:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM privacy_preference_events").fetchone()[0], 2)
        finally:
            conn.close()

    def test_rights_request_has_private_tracking_token_and_admin_workflow(self):
        csrf = self._csrf()
        response = self.client.post(
            "/api/privacidad/solicitudes",
            json={"request_type": "acceso", "full_name": "Cliente Prueba", "email": "cliente@example.cl", "description": "Solicito acceso a mis datos"},
            headers={"X-CSRF-Token": csrf},
        )
        self.assertEqual(response.status_code, 201)
        result = response.get_json()
        self.assertRegex(result["case_code"], r"^PRV-[A-F0-9]{10}$")
        tracked = self.client.post("/api/privacidad/solicitudes/consultar", json={"case_code": result["case_code"], "tracking_token": result["tracking_token"]}, headers={"X-CSRF-Token": csrf})
        self.assertTrue(tracked.get_json()["success"])
        self.assertNotIn("email", tracked.get_json())

        with self.client.session_transaction() as sess:
            sess["admin_autenticado"] = True
            sess["admin_user_name"] = "Admin de prueba"
            sess["privacy_csrf"] = csrf
        update = self.client.post(
            "/api/admin/privacidad/solicitudes/1",
            json={"status": "en_revision", "identity_status": "pendiente", "response_note": "Verificar identidad"},
            headers={"X-CSRF-Token": csrf},
        )
        self.assertEqual(update.status_code, 200)
        evidence = self.client.get("/api/admin/privacidad/solicitudes/1/evidencia")
        self.assertEqual(evidence.status_code, 200)
        self.assertIn("attachment; filename=\"PRV-", evidence.headers["Content-Disposition"])

    def test_policy_cannot_publish_until_complete_then_versions_are_snapshotted(self):
        csrf = self._csrf()
        with self.client.session_transaction() as sess:
            sess["admin_autenticado"] = True
            sess["admin_user_name"] = "Admin de prueba"
            sess["privacy_csrf"] = csrf
        denied = self.client.post("/api/admin/privacidad/publicar", json={}, headers={"X-CSRF-Token": csrf})
        self.assertEqual(denied.status_code, 400)
        self.assertTrue(denied.get_json()["missing"])

        draft = {key: f"Dato verificado para {key}" for key in privacidad._POLICY_FIELDS if key != "legal_reviewed"}
        draft.update({"contact_email": "privacidad@example.cl", "effective_date": "2026-12-01", "legal_reviewed": True})
        saved = self.client.post("/api/admin/privacidad/config", json=draft, headers={"X-CSRF-Token": csrf})
        self.assertEqual(saved.status_code, 200)
        published = self.client.post("/api/admin/privacidad/publicar", json={}, headers={"X-CSRF-Token": csrf})
        self.assertEqual(published.status_code, 200)
        self.assertEqual(published.get_json()["version"], "0.1")
        page = self.client.get("/privacidad/politica").get_data(as_text=True)
        self.assertIn("Version 0.1", page)
        history = self.client.get("/api/admin/privacidad/versiones")
        self.assertEqual(history.get_json()["versions"][0]["version"], "0.1")

    def test_analytics_without_preference_does_not_write_visit_data(self):
        response = self.client.post("/api/tienda/track", json={"session_id": "session-test-123", "evento": "view"})
        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.get_json()["counted"])
        conn = self.connect_db()
        try:
            self.assertIsNone(conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='tienda_visitas'").fetchone())
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
