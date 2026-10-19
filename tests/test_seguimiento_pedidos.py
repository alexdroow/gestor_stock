import os
import sqlite3
import tempfile
import unittest
from io import BytesIO
from datetime import datetime, timedelta
from unittest.mock import patch

import app
import database


class SeguimientoPedidosTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.temp.name, "tracking.sqlite")
        self.db_patch = patch.object(database, "DB_PATH", self.db_path)
        self.db_patch.start()
        conn = sqlite3.connect(self.db_path)
        conn.execute(
            """CREATE TABLE agenda_eventos (
                id INTEGER PRIMARY KEY, tipo TEXT, titulo TEXT, fecha TEXT, hora_inicio TEXT,
                hora_entrega TEXT, cliente TEXT, telefono TEXT, cliente_email TEXT,
                cliente_telefono TEXT, es_envio INTEGER, direccion TEXT, ingredientes TEXT,
                total REAL DEFAULT 0, abono REAL DEFAULT 0, estado TEXT,
                seguimiento_estado TEXT, seguimiento_actualizado TEXT, creado TEXT
            )"""
        )
        rows = [
            (1, "torta", "Torta activa", "2026-10-20", "12:00", "12:00", "Ana Perez", "+56912345678", "ana@example.cl", "+56912345678", 1, "Calle 123, Santiago", "", 30000, 10000, "pendiente", "recepcionado", "2026-10-19 12:00:00", "2026-10-19 10:00:00"),
            (2, "torta", "Torta entregada", "2026-10-18", "12:00", "12:00", "Ana Perez", "+56912345678", "ana@example.cl", "+56912345678", 0, "", "", 10000, 10000, "completado", "entregado", "2026-10-18 12:00:00", "2026-10-18 10:00:00"),
            (3, "torta", "Torta cancelada", "2026-10-21", "12:00", "12:00", "Ana Perez", "+56912345678", "ana@example.cl", "+56912345678", 0, "", "", 10000, 0, "cancelado", "pendiente", "2026-10-18 12:00:00", "2026-10-18 10:00:00"),
            (4, "torta", "Otro activo", "2026-10-22", "12:00", "12:00", "Luis", "987654321", "luis@example.cl", "987654321", 0, "", "", 15000, 0, "pendiente", "pendiente", "2026-10-19 12:00:00", "2026-10-19 10:00:00"),
        ]
        conn.executemany("INSERT INTO agenda_eventos VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)
        conn.commit()
        conn.close()

    def tearDown(self):
        self.db_patch.stop()
        self.temp.cleanup()

    def test_contact_search_returns_only_matching_active_orders(self):
        by_email = database.buscar_eventos_seguimiento_por_contacto(email="ANA@example.cl")
        by_phone = database.buscar_eventos_seguimiento_por_contacto(telefono="+56912345678")

        self.assertEqual([item["id"] for item in by_email], [1])
        self.assertEqual([item["id"] for item in by_phone], [1])

    def test_public_contact_lookup_api_returns_active_orders(self):
        response = app.app.test_client().post(
            "/api/seguimiento/buscar",
            json={"tipo": "email", "valor": "ana@example.cl"},
        )

        self.assertEqual(response.status_code, 200)
        data = response.get_json()
        self.assertEqual([item["codigo_pedido"] for item in data["pedidos"]], [""])
        self.assertEqual(data["pedidos"][0]["estado_pago"], "Abono registrado; saldo pendiente")

    def test_ready_agenda_order_accepts_photo_and_publicly_serves_it(self):
        conn = sqlite3.connect(self.db_path)
        conn.execute("UPDATE agenda_eventos SET seguimiento_estado='espera_envio' WHERE id=1")
        conn.commit()
        conn.close()
        from PIL import Image
        image = Image.new("RGB", (8, 8), "orange")
        raw = BytesIO()
        image.save(raw, format="PNG")
        raw.seek(0)
        client = app.app.test_client()
        with client.session_transaction() as session:
            session[app._ADMIN_SESSION_KEY] = True
        with patch.object(app, "crear_backup"), patch.object(app, "_directorio_fotos_seguimiento", return_value=os.path.join(self.temp.name, "images")):
            response = client.post(
                "/api/agenda/evento/1/foto-seguimiento",
                data={"foto": (raw, "cake.png")},
                content_type="multipart/form-data",
            )
            photo_url = response.get_json().get("foto_url") if response.is_json else ""
            photo = client.get(photo_url) if photo_url else None

        self.assertEqual(response.status_code, 200, response.get_data(as_text=True))
        self.assertIsNotNone(photo)
        self.assertEqual(photo.status_code, 200)
        self.assertEqual(photo.mimetype, "image/jpeg")
        self.assertEqual(photo.headers.get("Cache-Control"), "private, no-store, max-age=0")
        photo.get_data()
        photo.close()

    def test_payment_summary_has_no_amount_and_map_links_for_delivery(self):
        payload = app._seguimiento_agenda_payload({
            "codigo_pedido": "AGD-20261020-000001", "titulo": "Torta", "fecha": "2026-10-20",
            "seguimiento_estado": "espera_envio", "es_envio": 1, "direccion": "Calle 123, Santiago",
            "total": 30000, "abono": 10000,
        })

        self.assertEqual(payload["estado_pago"], "Abono registrado; saldo pendiente")
        self.assertNotIn("total", payload)
        self.assertNotIn("abono", payload)
        self.assertIn("google.com/maps", payload["mapa_google"])
        self.assertIn("maps.apple.com", payload["mapa_apple"])
        self.assertIn("waze.com", payload["mapa_waze"])

        self.assertEqual(app._seguimiento_agenda_payload({"total": 30000, "abono": 0})["estado_pago"], "Pago pendiente")
        self.assertEqual(app._seguimiento_agenda_payload({"total": 30000, "abono": 30000})["estado_pago"], "Pagado")

    def test_photo_expiration_clears_database_record_after_72_hours(self):
        now = datetime.now()
        old_time = (now - timedelta(hours=73)).strftime("%Y-%m-%d %H:%M:%S")
        new_time = (now - timedelta(hours=71)).strftime("%Y-%m-%d %H:%M:%S")
        database.limpiar_fotos_seguimiento_expiradas(ahora=now)
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute("UPDATE agenda_eventos SET seguimiento_foto=?, seguimiento_foto_subida=? WHERE id=1", ("a" * 32 + ".jpg", old_time))
            conn.execute("UPDATE agenda_eventos SET seguimiento_foto=?, seguimiento_foto_subida=? WHERE id=4", ("b" * 32 + ".jpg", new_time))
            conn.commit()
        finally:
            conn.close()

        expired = database.limpiar_fotos_seguimiento_expiradas(ahora=now)
        self.assertEqual(expired, ["a" * 32 + ".jpg"])
        self.assertIsNone(database.obtener_foto_seguimiento_por_nombre("a" * 32 + ".jpg"))
        self.assertIsNotNone(database.obtener_foto_seguimiento_por_nombre("b" * 32 + ".jpg"))

    def test_tracking_page_has_contact_search_and_map_photo_support(self):
        response = app.app.test_client().get("/seguimiento")
        html = response.get_data(as_text=True)

        self.assertEqual(response.status_code, 200)
        self.assertIn('id="search-type"', html)
        self.assertIn('value="email"', html)
        self.assertIn('value="telefono"', html)
        self.assertIn("/api/seguimiento/buscar", html)
        self.assertIn("Google Maps", html)
        self.assertIn("Asi quedo tu pedido", html)
        self.assertIn("Estado del pago", html)

    def test_agenda_exposes_photo_upload_controls_for_ready_orders(self):
        with app.app.test_client() as client:
            with client.session_transaction() as session:
                session[app._ADMIN_SESSION_KEY] = True
            response = client.get("/agenda")
        html = response.get_data(as_text=True)

        self.assertEqual(response.status_code, 200)
        self.assertIn('id="seguimiento-foto-input"', html)
        self.assertIn("subirFotoSeguimientoAdmin()", html)
        self.assertIn("/foto-seguimiento", html)


if __name__ == "__main__":
    unittest.main()
