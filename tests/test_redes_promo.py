import os
import re
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

import app
import database
import redes
from redes import redes_promo_validate_checkout


class RedesWelcomePromoTests(unittest.TestCase):
    def setUp(self):
        with redes._PROMO_RATE_LOCK:
            redes._PROMO_RATE_BUCKETS.clear()
        self.temp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.temp.name, "promo.sqlite")
        self.db_patch = patch.object(database, "DB_PATH", self.db_path)
        self.db_patch.start()
        connect = sqlite3.connect

        def connect_with_rows(*args, **kwargs):
            conn = connect(*args, **kwargs)
            conn.row_factory = sqlite3.Row
            return conn

        with patch.object(database.sqlite3, "connect", side_effect=connect_with_rows):
            database.init_db()
        conn = database.get_db()
        try:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS tienda_clientes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, nombre TEXT DEFAULT '', email TEXT NOT NULL,
                    telefono TEXT NOT NULL, fecha_nacimiento TEXT, email_confirmado INTEGER DEFAULT 0,
                    direccion_default TEXT, direccion_lat REAL, direccion_lng REAL, activo INTEGER DEFAULT 1,
                    actualizado_en TEXT DEFAULT CURRENT_TIMESTAMP, ultimo_login TEXT,
                    nivel_id INTEGER, puntos_actual INTEGER DEFAULT 0, puntos_total INTEGER DEFAULT 0,
                    UNIQUE(email, telefono)
                );
                CREATE TABLE IF NOT EXISTS tienda_clientes_niveles (
                    id INTEGER PRIMARY KEY, nombre TEXT, slug TEXT, orden INTEGER DEFAULT 0,
                    puntos_minimos INTEGER DEFAULT 0, beneficios_json TEXT DEFAULT '{}',
                    descuento_pct REAL DEFAULT 0, activo INTEGER DEFAULT 1
                );
                INSERT OR IGNORE INTO tienda_clientes_niveles(id,nombre,slug,puntos_minimos,beneficios_json,activo)
                    VALUES(1,'Bronce','bronce',0,'{}',1);
                CREATE TABLE IF NOT EXISTS tienda_cupones (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, codigo TEXT NOT NULL UNIQUE, nombre TEXT NOT NULL DEFAULT '',
                    tipo_descuento TEXT NOT NULL DEFAULT 'porcentaje', valor_descuento REAL NOT NULL DEFAULT 0,
                    activo INTEGER NOT NULL DEFAULT 1, fecha_inicio TEXT, fecha_fin TEXT, hora_inicio TEXT,
                    hora_fin TEXT, usos_max_total INTEGER, usos_max_por_cliente INTEGER,
                    monto_minimo REAL NOT NULL DEFAULT 0, solo_sin_oferta INTEGER NOT NULL DEFAULT 0,
                    alcance TEXT NOT NULL DEFAULT 'tienda'
                );
                CREATE TABLE IF NOT EXISTS tienda_cliente_cupones (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, cliente_id INTEGER NOT NULL, cliente_ref TEXT NOT NULL DEFAULT '',
                    cupon_id INTEGER NOT NULL, activo INTEGER NOT NULL DEFAULT 1, usado INTEGER NOT NULL DEFAULT 0,
                    usado_venta_id INTEGER, usos_realizados INTEGER NOT NULL DEFAULT 0, usos_max INTEGER NOT NULL DEFAULT 1,
                    nota TEXT DEFAULT '', asignado_por TEXT NOT NULL DEFAULT 'admin',
                    fecha_asignado TEXT DEFAULT CURRENT_TIMESTAMP, fecha_vencimiento TEXT, fecha_usado TEXT
                );
                """
            )
            conn.commit()
        finally:
            conn.close()
        app.app.config["TESTING"] = True
        self.client = app.app.test_client()
        self.client.get("/admin/redes")
        with self.client.session_transaction() as session:
            session["admin_autenticado"] = True
            session["admin_user_name"] = "Promo test"
            session["redes_csrf"] = "promo-test-csrf"
        self.csrf = "promo-test-csrf"

    def tearDown(self):
        self.db_patch.stop()
        self.temp.cleanup()

    def admin_post(self, path, payload):
        return self.client.post(path, json=payload, headers={"X-CSRF-Token": self.csrf})

    def enable_promo(self, **overrides):
        payload = {
            "active": True,
            "discount_pct": 15,
            "validity_days": 7,
            "starts_on": "",
            "ends_on": "",
        }
        payload.update(overrides)
        return self.admin_post("/api/redes/admin/promo", payload)

    def registration_token(self):
        response = self.client.get("/redes")
        match = re.search(r'<meta name="redes-promo-csrf" content="([^"]+)"', response.get_data(as_text=True))
        self.assertIsNotNone(match)
        return match.group(1)

    def register(self, email="cliente@example.cl", phone="12345678"):
        return self.client.post(
            "/api/redes/promo/register",
            json={"email": email, "telefono": "+569" + phone, "consent": True},
            headers={"X-CSRF-Token": self.registration_token()},
        )

    def test_admin_config_drives_announcement_and_popup(self):
        saved = self.enable_promo(discount_pct=12.5, validity_days=14, ends_on="2026-12-31")
        self.assertEqual(saved.status_code, 200)
        html = self.client.get("/redes").get_data(as_text=True)
        self.assertIn("12.5% de descuento", html)
        self.assertIn('data-promo-date="2026-12-31"', html)
        self.assertIn("dentro de 14 días", html)
        self.assertIn("id=\"promo-phone\"", html)
        self.assertIn("+569", html)

    def test_registration_creates_one_store_coupon_bound_to_contact(self):
        self.assertEqual(self.enable_promo().status_code, 200)
        response = self.register()
        self.assertEqual(response.status_code, 200, response.get_json())
        result = response.get_json()
        self.assertTrue(re.fullmatch(r"SU[A-F0-9]{12}", result["codigo"]))
        with self.client.session_transaction() as session:
            registration_id = session["redes_promo_registration_id"]
        import redes

        contact = redes.redes_promo_registration_contact(registration_id, result["codigo"])
        self.assertEqual(contact["email"], "cliente@example.cl")
        self.assertEqual(contact["telefono"], "+56912345678")
        conn = database.get_db()
        try:
            coupon = conn.execute("SELECT * FROM tienda_cupones WHERE codigo=?", (result["codigo"],)).fetchone()
            assigned = conn.execute("SELECT usos_max,usado FROM tienda_cliente_cupones WHERE cupon_id=?", (coupon["id"],)).fetchone()
        finally:
            conn.close()
        self.assertEqual(coupon["valor_descuento"], 15)
        self.assertEqual(assigned["usos_max"], 1)
        self.assertEqual(assigned["usado"], 0)
        client_ref = app._normalizar_cliente_ref("cliente@example.cl", "+56912345678")
        self.assertTrue(redes_promo_validate_checkout(result["codigo"], client_ref)["ok"])
        wrong_owner = redes_promo_validate_checkout(result["codigo"], app._normalizar_cliente_ref("otra@example.cl", "+56987654321"))
        self.assertFalse(wrong_owner["ok"])

    def test_duplicate_contact_and_missing_consent_are_rejected(self):
        self.assertEqual(self.enable_promo().status_code, 200)
        token = self.registration_token()
        no_consent = self.client.post(
            "/api/redes/promo/register",
            json={"email": "cliente@example.cl", "telefono": "+56912345678", "consent": False},
            headers={"X-CSRF-Token": token},
        )
        self.assertEqual(no_consent.status_code, 400)
        first = self.register()
        self.assertEqual(first.status_code, 200)
        duplicate = self.register(email="otro@example.cl", phone="12345678")
        self.assertEqual(duplicate.status_code, 409)
        self.assertNotIn("\u00c3", duplicate.get_json()["error"])
        public_html = self.client.get("/redes").get_data(as_text=True)
        self.assertIn("fixPromoText(data.error", public_html)

    def test_same_active_registration_reports_expiry_without_issuing_another_code(self):
        self.assertEqual(self.enable_promo().status_code, 200)
        first = self.register().get_json()
        repeated = self.register().get_json()
        self.assertTrue(repeated["already_registered"])
        self.assertTrue(repeated["active_code"])
        self.assertEqual(repeated["expires_on"], first["expires_on"])
        self.assertGreaterEqual(repeated["remaining_days"], 0)
        self.assertEqual(repeated["codigo"], first["codigo"])
        conn = database.get_db()
        try:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM redes_promo_codes").fetchone()[0], 1)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM tienda_clientes").fetchone()[0], 1)
        finally:
            conn.close()

    def test_any_existing_customer_contact_blocks_promo_and_store_signup(self):
        self.assertEqual(self.enable_promo().status_code, 200)
        conn = database.get_db()
        try:
            conn.execute(
                "INSERT INTO tienda_clientes(nombre,email,telefono) VALUES(?,?,?)",
                ("Cliente existente", "registrado@example.cl", "+56912345678"),
            )
            conn.commit()
        finally:
            conn.close()

        promo_email_duplicate = self.register(email="nuevo@example.cl", phone="12345678")
        self.assertEqual(promo_email_duplicate.status_code, 409)
        promo_phone_duplicate = self.register(email="registrado@example.cl", phone="87654321")
        self.assertEqual(promo_phone_duplicate.status_code, 409)

        registration = self.client.post("/api/tienda/clientes/registrar", json={
            "nombre": "Otro Cliente", "email": "otro@example.cl", "email_confirm": "otro@example.cl",
            "telefono": "+56912345678", "fecha_nacimiento": "1990-01-01",
        })
        self.assertEqual(registration.status_code, 409)
        conn = database.get_db()
        try:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM redes_promo_codes").fetchone()[0], 0)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM tienda_clientes").fetchone()[0], 1)
        finally:
            conn.close()

    def test_pending_code_endpoint_returns_coupon_for_registered_contact(self):
        self.assertEqual(self.enable_promo().status_code, 200)
        created = self.register().get_json()
        response = self.client.post("/api/redes/promo/pending", json={
            "email": "cliente@example.cl", "telefono": "+56912345678",
        })
        self.assertEqual(response.status_code, 200)
        data = response.get_json()
        self.assertTrue(data["pending"])
        self.assertEqual(data["codigo"], created["codigo"])

        conn = database.get_db()
        try:
            conn.execute("UPDATE redes_promo_codes SET used_sale_id=55 WHERE codigo=?", (created["codigo"],))
            conn.commit()
        finally:
            conn.close()
        used_response = self.client.post("/api/redes/promo/pending", json={
            "email": "cliente@example.cl", "telefono": "+56912345678",
        })
        self.assertFalse(used_response.get_json()["pending"])

    def test_cart_has_pending_promo_reminder_and_account_paths_refresh_it(self):
        html = self.client.get("/tienda").get_data(as_text=True)
        self.assertIn("Tienes un descuento de bienvenida pendiente", html)
        self.assertIn("Aplicar mi código", html)
        self.assertIn("/api/redes/promo/pending", html)
        self.assertGreaterEqual(html.count("await cargarPromoPendienteTienda()"), 2)
        public_html = self.client.get("/redes").get_data(as_text=True)
        self.assertIn("Tu cuenta ya está creada", public_html)
        self.assertIn("Te quedan ${days}", public_html)
        self.assertIn("promoMessage.textContent=''", public_html)
        self.assertIn(".promo-code[hidden],.promo-copy[hidden]{display:none}", public_html)

    def test_campaign_can_be_disabled_and_codes_are_marked_used(self):
        self.assertEqual(self.enable_promo().status_code, 200)
        created = self.register().get_json()
        self.assertEqual(self.enable_promo(active=False).status_code, 200)
        client_ref = app._normalizar_cliente_ref("cliente@example.cl", "+56912345678")
        self.assertFalse(redes_promo_validate_checkout(created["codigo"], client_ref)["ok"])
        self.assertEqual(self.enable_promo().status_code, 200)
        conn = database.get_db()
        try:
            conn.execute("UPDATE redes_promo_codes SET used_sale_id=321,used_at=CURRENT_TIMESTAMP WHERE codigo=?", (created["codigo"],))
            conn.commit()
        finally:
            conn.close()
        result = redes_promo_validate_checkout(created["codigo"], client_ref)
        self.assertFalse(result["ok"])
        self.assertIn("utilizado", result["error"])

    def test_invalid_admin_configuration_is_rejected(self):
        response = self.enable_promo(discount_pct=101)
        self.assertEqual(response.status_code, 400)
        response = self.enable_promo(validity_days=0)
        self.assertEqual(response.status_code, 400)

    def test_admin_history_uses_wrapping_layout_for_contact_details(self):
        with self.client.session_transaction() as session:
            session["admin_autenticado"] = True
        html = self.client.get("/admin/redes").get_data(as_text=True)
        self.assertIn(".promo-code-row{grid-template-columns:minmax(0,1fr) auto", html)
        self.assertIn("overflow-wrap:anywhere", html)
        self.assertIn("promo-code-info", html)


if __name__ == "__main__":
    unittest.main()
