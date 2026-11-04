import os
import json
from io import BytesIO
import sqlite3
import tempfile
import unittest
from unittest.mock import patch
from pathlib import Path

import app as app_module
import cocteleria as cocteleria_module
import database as database_module


class CocteleriaCatalogTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.temp.name, "cocteleria.sqlite")
        self.db_patch = patch.object(cocteleria_module, "DB_PATH", self.db_path)
        self.db_patch.start()
        self.client = app_module.app.test_client()

    def tearDown(self):
        self.db_patch.stop()
        self.temp.cleanup()

    def login_admin(self):
        with self.client.session_transaction() as sess:
            sess[app_module._ADMIN_SESSION_KEY] = True

    def create_product(self, category="dulce"):
        self.login_admin()
        response = self.client.post("/api/tienda/admin/cocteleria", json={
            "name": "Mini tartaletas",
            "category": category,
            "description": "Tartaletas para compartir",
            "short_description": "Bocados artesanales",
            "ingredients": "Harina, crema",
            "variety_summaries": [{"name": "Harina", "summary": "Masa horneada de textura crujiente."}],
            "allergens": "Gluten y lácteos",
            "min_quantity": 1,
            "max_quantity": 8,
            "min_lead_hours": 48,
            "presentations": [{"label": "Bandeja 20", "units": 20, "unit_label": "unidades", "price_clp": 18000}],
        })
        self.assertEqual(response.status_code, 201, response.get_data(as_text=True))
        return response.get_json()["item"]

    def add_cover_and_publish(self, item):
        cocteleria_module.ensure_cocteleria_schema()
        conn = sqlite3.connect(self.db_path)
        conn.execute("INSERT INTO tienda_cocteleria_imagenes(model_id,path,alt,is_cover,active) VALUES(?,?,?,?,1)",
                     (item["id"], "/static/cocteleria/test-cover.jpg", item["name"], 1))
        conn.commit()
        conn.close()
        published = self.client.post(f"/api/tienda/admin/cocteleria/{item['id']}/status", json={"status": "published"})
        self.assertEqual(published.status_code, 200, published.get_data(as_text=True))

    def test_public_catalog_and_admin_are_separate_and_admin_requires_session(self):
        self.assertEqual(self.client.get("/tienda/cocteleria").status_code, 200)
        denied = self.client.get("/ventas/cocteleria")
        self.assertEqual(denied.status_code, 302)
        self.assertEqual(self.client.get("/api/tienda/admin/cocteleria").status_code, 401)

        item = self.create_product("salado")
        self.add_cover_and_publish(item)
        public = self.client.get("/api/tienda/cocteleria").get_json()
        self.assertTrue(public["success"])
        self.assertEqual(len(public["items"]), 1)
        self.assertEqual(public["items"][0]["category"], "salado")
        self.assertEqual(public["items"][0]["presentations"][0]["price_clp"], 18000)
        self.assertEqual(public["items"][0]["variety_summaries"], [{"name": "Harina", "summary": "Masa horneada de textura crujiente."}])

        page = self.client.get("/tienda/cocteleria")
        self.assertEqual(page.status_code, 200)
        self.assertIn(b"Todos", page.data)
        self.assertIn(b"Salado", page.data)

    def test_admin_can_duplicate_product_as_draft_and_keep_presentation(self):
        item = self.create_product()
        duplicate = self.client.post(f"/api/tienda/admin/cocteleria/{item['id']}/duplicate", json={})
        self.assertEqual(duplicate.status_code, 201)
        copy = duplicate.get_json()["item"]
        self.assertEqual(copy["status"], "draft")
        self.assertEqual(copy["presentations"][0]["price_clp"], 18000)
        self.assertEqual(copy["variety_summaries"], [{"name": "Harina", "summary": "Masa horneada de textura crujiente."}])
        self.assertNotEqual(copy["slug"], item["slug"])

    def test_booking_type_and_product_price_are_authoritative(self):
        item = self.create_product()
        self.add_cover_and_publish(item)
        presentation = item["presentations"][0]
        actual = cocteleria_module.get_published_cocktail(item["id"], presentation["id"])
        self.assertEqual(actual["price_clp"], 18000)
        self.assertEqual(actual["min_lead_hours"], 48)
        self.assertEqual(app_module._normalizar_tipo_reserva_tienda("cocteleria"), "cocteleria")
        self.assertEqual(app_module._minutos_anticipacion_reserva("cocteleria", min_horas_categoria=48), 48 * 60)

    def test_cart_schema_and_public_page_support_multiple_persistent_lines(self):
        cocteleria_module.ensure_cocteleria_schema()
        conn = sqlite3.connect(self.db_path)
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        conn.close()
        self.assertIn("tienda_cocteleria_reserva_lineas", tables)

        response = self.client.get("/tienda/cocteleria")
        self.assertEqual(response.status_code, 200)
        html = response.get_data(as_text=True)
        for marker in ("id=\"cart-summary\"", "id=\"cart-items\"", "id=\"cart-bar\"", "id=\"success-pdf\"", "Total actual del pedido", "function mergeCartLines(lines)", "localStorage", "cocteleria:{items:", "function syncSelectedCart()", "syncSelectedCart();"):
            self.assertIn(marker, html)
        for marker in ("Variedad en la bandeja", "Toca las variedades con resumen para revisar sus rellenos y detalles.", "cocktail-variety-list", "cocktail-variety-icon", "cocktail-variety-bubble", "cocktail-variety-choice", "/static/icons/cocteleria-banquet.png", "function shuffleProducts(items)", "const randomizedProducts=shuffleProducts(products)", "randomizedProducts.filter("):
            self.assertIn(marker, html)
        self.assertNotIn("Ingredientes:</b>", html)
        self.assertNotIn('id="add-to-cart"', html)

        admin_template = Path(__file__).resolve().parents[1] / "templates" / "tienda_cocteleria_admin.html"
        admin_html = admin_template.read_text(encoding="utf-8")
        for marker in ("Resumen opcional de cada variedad", "id=\"variety-summaries\"", "variety_summaries:readVarietySummaries()"):
            self.assertIn(marker, admin_html)

    def test_existing_agenda_editor_recognizes_cocktail_reservations(self):
        template = Path(__file__).resolve().parents[1] / "templates" / "agenda.html"
        html = template.read_text(encoding="utf-8")
        self.assertIn('<option value="cocteleria">', html)
        self.assertIn("evento.tipo === 'cocteleria'", html)
        self.assertEqual(app_module._label_origen_evento({"fuente": "agenda", "tipo": "cocteleria", "origen": "tienda_online"}), "Cóctelería (tienda online)")

    def test_agenda_has_independent_manual_cocktail_booking_tool(self):
        template = Path(__file__).resolve().parents[1] / "templates" / "agenda.html"
        partial = Path(__file__).resolve().parents[1] / "templates" / "_agenda_cocteleria_manual.html"
        html = template.read_text(encoding="utf-8")
        modal = partial.read_text(encoding="utf-8")
        for marker in ("+ Agendar cóctelería", "abrirAgendaCocteleriaManual()", "_agenda_cocteleria_manual.html", "agenda-cocteleria-manual.js"):
            self.assertIn(marker, html)
        for marker in ("agenda-cocteleria-modal", "agenda-coct-days", "agenda-coct-hours", "agenda-coct-lines", "agenda-coct-extras", "agenda-coct-customer", "agenda-coct-pdf"):
            self.assertIn(marker, modal)
        denied = self.client.post("/api/agenda/cocteleria/manual", json={})
        self.assertEqual(denied.status_code, 401)

    def test_manual_agenda_event_persists_cocktail_snapshot_and_email(self):
        conn = sqlite3.connect(self.db_path)
        conn.executescript("""
            CREATE TABLE agenda_eventos (
                id INTEGER PRIMARY KEY AUTOINCREMENT, tipo TEXT, titulo TEXT, fecha TEXT,
                hora_inicio TEXT, hora_fin TEXT, hora_entrega TEXT, cliente TEXT, telefono TEXT,
                cliente_email TEXT, es_envio INTEGER, direccion TEXT, ingredientes TEXT, total REAL,
                abono REAL, motivo TEXT, alerta_minutos INTEGER, estado TEXT,
                codigo_operacion TEXT, codigo_pedido TEXT
            );
            CREATE TABLE agenda_recordatorios_descartados (evento_id INTEGER);
        """)
        cocteleria_module.ensure_cocteleria_schema(conn)
        conn.commit()
        conn.close()
        snapshot = {
            "kind": "COCTELERIA", "item_count": 1, "total_clp": 18000,
            "extras": [{"description": "Montaje", "value_clp": 2500}], "extras_total_clp": 2500,
            "items": [{"model_id": 5, "presentation_id": 9, "product_name": "Bocados", "presentation_label": "Bandeja 20", "quantity": 1, "unit_price_clp": 18000, "total_clp": 18000}],
        }
        event = {
            "tipo": "cocteleria", "titulo": "Coctelería de prueba", "fecha": "2026-12-20",
            "hora_inicio": "14:00", "hora_fin": "15:00", "hora_entrega": "14:00",
            "cliente": "Cliente de prueba", "telefono": "+56912345678", "cliente_email": "cliente@example.com",
            "ingredientes": "--- Reserva de cocteleria ---\n- Bocados / Bandeja 20",
            "total": 20500, "abono": 0, "estado": "pendiente",
            "_cocteleria_snapshot": snapshot, "_cocteleria_request_key": "manual-test-key",
        }
        with patch.object(database_module, "DB_PATH", self.db_path):
            saved = database_module.guardar_evento_agenda(event)
        self.assertTrue(saved["success"], saved)
        conn = sqlite3.connect(self.db_path)
        self.assertEqual(conn.execute("SELECT cliente_email,total FROM agenda_eventos WHERE id=?", (saved["id"],)).fetchone(), ("cliente@example.com", 20500.0))
        stored = conn.execute("SELECT snapshot_json FROM tienda_cocteleria_reservas WHERE event_id=?", (saved["id"],)).fetchone()
        self.assertEqual(json.loads(stored[0])["extras_total_clp"], 2500)
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM tienda_cocteleria_reserva_lineas WHERE event_id=?", (saved["id"],)).fetchone()[0], 1)
        conn.close()


    def test_cocktail_history_is_grouped_by_reservation_and_contains_customer(self):
        cocteleria_module.ensure_cocteleria_schema()
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("""CREATE TABLE agenda_eventos (id INTEGER PRIMARY KEY,cliente TEXT,telefono TEXT,
            codigo_pedido TEXT,fecha TEXT,hora_inicio TEXT,estado TEXT,total REAL,es_envio INTEGER,direccion TEXT)""")
        conn.execute("INSERT INTO agenda_eventos VALUES(?,?,?,?,?,?,?,?,?,?)", (901, "Cliente Test", "+56911112222", "AGD-901", "2026-11-03", "15:00", "pendiente", 31500, 0, ""))
        snapshot = {"items": [
            {"product_name": "Mini dulce", "presentation_label": "Bandeja 10", "quantity": 1, "unit_price_clp": 18000, "total_clp": 18000},
            {"product_name": "Mini salado", "presentation_label": "Bandeja 20", "quantity": 1, "unit_price_clp": 13500, "total_clp": 13500},
        ], "total_clp": 31500, "item_count": 2}
        conn.execute("INSERT INTO tienda_cocteleria_reservas(request_key,event_id,model_id,presentation_id,snapshot_json) VALUES(?,?,?,?,?)", ("history-key", 901, 7, 8, json.dumps(snapshot)))
        for mid, pid, item in ((7, 8, snapshot["items"][0]), (9, 10, snapshot["items"][1])):
            conn.execute("INSERT INTO tienda_cocteleria_reserva_lineas(event_id,model_id,presentation_id,snapshot_json) VALUES(?,?,?,?)", (901, mid, pid, json.dumps(item)))
        conn.commit()
        conn.close()
        self.login_admin()

        response = self.client.get("/api/tienda/admin/cocteleria/7/reservas")
        self.assertEqual(response.status_code, 200, response.get_data(as_text=True))
        records = response.get_json()["items"]
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["cliente"], "Cliente Test")
        self.assertEqual(records[0]["telefono"], "+56911112222")
        self.assertEqual(len(json.loads(records[0]["snapshot_json"])["items"]), 2)

        template = Path(__file__).resolve().parents[1] / "templates" / "tienda_cocteleria_admin.html"
        html = template.read_text(encoding="utf-8")
        self.assertIn('<details class="reservation-record">', html)
        self.assertIn("reservation-main", html)

    def test_deleting_agenda_reservation_removes_cocktail_history(self):
        cocteleria_module.ensure_cocteleria_schema()
        conn = sqlite3.connect(self.db_path)
        conn.execute("CREATE TABLE agenda_eventos (id INTEGER PRIMARY KEY, fecha TEXT)")
        conn.execute("CREATE TABLE agenda_recordatorios_descartados (evento_id INTEGER)")
        conn.executemany("INSERT INTO agenda_eventos(id,fecha) VALUES(?,?)", [(911, "2030-01-01"), (912, "2019-01-01")])
        conn.executemany("INSERT INTO tienda_cocteleria_reservas(request_key,event_id,model_id,presentation_id,snapshot_json) VALUES(?,?,?,?,?)", [
            ("delete-911", 911, 1, 1, "{}"), ("delete-912", 912, 1, 1, "{}")
        ])
        conn.executemany("INSERT INTO tienda_cocteleria_reserva_lineas(event_id,model_id,presentation_id,snapshot_json) VALUES(?,?,?,?)", [
            (911, 1, 1, "{}"), (912, 1, 1, "{}")
        ])
        conn.commit()
        conn.close()

        def connect_test_db():
            return sqlite3.connect(self.db_path)

        with patch.object(database_module, "get_db", side_effect=connect_test_db):
            self.assertTrue(database_module.eliminar_evento_agenda(911)["success"])
            self.assertTrue(database_module.eliminar_eventos_agenda_pasados("2020-01-01")["success"])

        conn = sqlite3.connect(self.db_path)
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM tienda_cocteleria_reservas").fetchone()[0], 0)
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM tienda_cocteleria_reserva_lineas").fetchone()[0], 0)
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM agenda_eventos").fetchone()[0], 0)
        conn.close()

    def test_cocktail_reservation_pdf_contains_each_item_and_price(self):
        if app_module.canvas is None:
            self.skipTest("ReportLab no esta instalado")
        cocteleria_module.ensure_cocteleria_schema()
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("CREATE TABLE IF NOT EXISTS tienda_tortas_listas_reservas (event_id INTEGER, snapshot_json TEXT)")
        conn.execute("""CREATE TABLE agenda_eventos (
            id INTEGER PRIMARY KEY, codigo_pedido TEXT, codigo_operacion TEXT, fecha TEXT,
            hora_entrega TEXT, hora_inicio TEXT, cliente TEXT, telefono TEXT, direccion TEXT,
            ingredientes TEXT, motivo TEXT, total REAL, abono REAL, estado TEXT,
            documento_tipo TEXT, titulo TEXT, es_envio INTEGER
        )""")
        snapshot = {
            "kind": "COCTELERIA", "item_count": 2, "total_clp": 31500,
            "extras": [{"description": "Montaje de mesa", "value_clp": 4200}], "extras_total_clp": 4200,
            "items": [
                {"product_name": "Mini tartaletas", "category": "dulce", "presentation_label": "Bandeja 20", "units_per_pack": 20, "unit_label": "unidades", "quantity": 1, "unit_price_clp": 18000, "total_clp": 18000},
                {"product_name": "Empanaditas cocktail", "category": "salado", "presentation_label": "Bandeja 30", "units_per_pack": 30, "unit_label": "unidades", "quantity": 1, "unit_price_clp": 13500, "total_clp": 13500},
            ],
        }
        conn.execute("INSERT INTO tienda_cocteleria_reservas(request_key,event_id,model_id,presentation_id,snapshot_json) VALUES(?,?,?,?,?)", ("test-cocktail-pdf", 812, 1, 1, json.dumps(snapshot)))
        conn.execute(
            "INSERT INTO agenda_eventos VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (812, "AGD-TEST-COCTEL", "", "2026-11-03", "15:00", "15:00", "Cliente de prueba", "+56912345678", "", "--- Reserva de cocteleria ---\n- Mini tartaletas / Bandeja 20\n- Empanaditas cocktail / Bandeja 30", "Reserva cliente tienda online", 35700, 10100, "pendiente", "cotizacion", "Cocteleria - 2 productos", 0),
        )
        conn.commit()
        conn.close()

        def connect_test_db():
            db = sqlite3.connect(self.db_path)
            db.row_factory = sqlite3.Row
            return db

        with patch.object(app_module, "get_db", side_effect=connect_test_db), patch.object(app_module, "static_dir", self.temp.name):
            denied = self.client.get("/api/tienda/cocteleria/reserva/812/comprobante?token=incorrecto")
            self.assertEqual(denied.status_code, 404)
            response = self.client.get("/api/tienda/cocteleria/reserva/812/comprobante?token=test-cocktail-pdf")

        from pypdf import PdfReader
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.mimetype, "application/pdf")
        pdf_content = response.data
        response.close()
        pages = PdfReader(BytesIO(pdf_content)).pages
        self.assertEqual(len(pages), 1)
        text = "\n".join(page.extract_text() or "" for page in pages)
        for expected in ("DE COCTELERIA", "PRODUCTOS DE COCTELERIA", "Mini tartaletas", "Bandeja 20", "Empanaditas cocktail", "Montaje de mesa", "$4.200", "$18.000", "$13.500", "$31.500"):
            self.assertIn(expected, text)
        total_final_pos = text.index("TOTAL FINAL")
        self.assertIn("$35.700", text[total_final_pos:])
        self.assertIn("$25.600", text)


if __name__ == "__main__":
    unittest.main()
