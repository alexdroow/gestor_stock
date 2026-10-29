import os
import tempfile
import unittest
from io import BytesIO
from unittest.mock import patch

from PIL import Image

import app
import database


class RedesTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.temp.name, "redes.sqlite")
        self.db_patch = patch.object(database, "DB_PATH", self.db_path)
        self.db_patch.start()
        self.upload_dir = os.path.join(self.temp.name, "uploads", "redes")
        self.upload_patch = patch.dict(app.app.config, {"REDES_UPLOAD_DIR": self.upload_dir})
        self.upload_patch.start()
        self.client = app.app.test_client()
        app.app.config["TESTING"] = True
        self.client.get("/admin/redes")
        with self.client.session_transaction() as session:
            session["admin_autenticado"] = True
            session["admin_user_name"] = "Redes test"
        self.client.get("/admin/redes")
        with self.client.session_transaction() as session:
            self.csrf = session["redes_csrf"]

    def tearDown(self):
        self.upload_patch.stop()
        self.db_patch.stop()
        self.temp.cleanup()

    def mutate(self, method, path, data=None):
        return self.client.open(path, method=method, json=data, headers={"X-CSRF-Token": self.csrf})

    def create_link(self, **overrides):
        data = {"title": "Agenda tu torta", "url": "/tienda/agendapedidos", "subtitle": "Personaliza tu pedido", "layout": "featured", "status": "active"}
        data.update(overrides)
        response = self.mutate("POST", "/api/redes/admin/links", data)
        return response, response.get_json()

    def test_public_page_is_empty_until_admin_publishes_real_links(self):
        response = self.client.get("/redes")
        self.assertEqual(response.status_code, 200)
        html = response.get_data(as_text=True)
        self.assertIn("Todo el universo Sucrée en un lugar", html)
        self.assertIn("Estamos preparando este espacio", html)
        self.assertNotIn("3.842", html)
        self.assertNotIn("linktr.ee", html.lower())
        self.assertIn('href="/redes"', self.client.get("/tienda").get_data(as_text=True))

    def test_admin_is_protected_and_api_mutations_require_csrf(self):
        with self.client.session_transaction() as session:
            session.pop("admin_autenticado", None)
            session.pop("redes_csrf", None)
        self.assertEqual(self.client.get("/admin/redes").status_code, 302)
        self.assertEqual(self.client.get("/api/redes/admin/state").status_code, 401)
        with self.client.session_transaction() as session:
            session["admin_autenticado"] = True
            session["redes_csrf"] = "known-token"
        denied = self.client.post("/api/redes/admin/links", json={"title": "Tienda", "url": "/tienda"})
        self.assertEqual(denied.status_code, 403)

    def test_invalid_urls_are_rejected_and_links_are_not_visible_until_published(self):
        invalid, _ = self.create_link(url="javascript:alert(1)")
        self.assertEqual(invalid.status_code, 400)
        created, data = self.create_link()
        self.assertEqual(created.status_code, 201)
        link_id = data["link"]["id"]
        self.assertTrue(self.client.get("/api/redes/admin/state").get_json()["has_changes"])
        unavailable = self.client.get(f"/redes/go/{link_id}")
        self.assertEqual(unavailable.status_code, 404)
        self.assertIn("Este enlace no está disponible", unavailable.get_data(as_text=True))
        html = self.client.get("/redes").get_data(as_text=True)
        self.assertNotIn("Agenda tu torta", html)
        self.assertEqual(self.mutate("POST", "/api/redes/admin/publish", {}).status_code, 200)
        self.assertFalse(self.client.get("/api/redes/admin/state").get_json()["has_changes"])
        html = self.client.get("/redes").get_data(as_text=True)
        self.assertIn("Agenda tu torta", html)
        self.assertIn(f'data-redes-link="{link_id}"', html)

    def test_visible_visit_click_rate_and_csv_export_use_real_events(self):
        _, data = self.create_link(layout="quick")
        link_id = data["link"]["id"]
        self.assertEqual(self.mutate("POST", "/api/redes/admin/publish", {}).status_code, 200)
        with self.client.session_transaction() as session:
            session.pop("admin_autenticado", None)
        view = self.client.post("/api/redes/view", json={"referrer": "https://www.instagram.com/story", "utm_source": "instagram"})
        self.assertEqual(view.get_json()["counted"], True)
        click = self.client.get(f"/redes/go/{link_id}")
        self.assertEqual(click.status_code, 302)
        self.assertEqual(click.location, "/tienda/agendapedidos")
        with self.client.session_transaction() as session:
            session["admin_autenticado"] = True
        stats = self.client.get("/api/redes/admin/state?period=30").get_json()["stats"]
        self.assertEqual(stats["views"], 1)
        self.assertEqual(stats["clicks"], 1)
        self.assertEqual(stats["rate"], 100.0)
        self.assertEqual(stats["by_link"][0]["id"], link_id)
        export = self.client.get("/api/redes/admin/metrics.csv?period=30")
        self.assertEqual(export.status_code, 200)
        self.assertIn("text/csv", export.content_type)
        self.assertIn("instagram.com", export.get_data(as_text=True))

    def test_admin_visits_and_clicks_are_excluded_from_metrics(self):
        _, data = self.create_link(layout="quick")
        link_id = data["link"]["id"]
        self.assertEqual(self.mutate("POST", "/api/redes/admin/publish", {}).status_code, 200)
        view = self.client.post("/api/redes/view", json={})
        self.assertFalse(view.get_json()["counted"])
        self.assertEqual(self.client.get(f"/redes/go/{link_id}").status_code, 302)
        stats = self.client.get("/api/redes/admin/state?period=30").get_json()["stats"]
        self.assertEqual(stats["views"], 0)
        self.assertEqual(stats["clicks"], 0)

    def test_metrics_reset_requires_csrf_and_explicit_confirmation(self):
        _, data = self.create_link(layout="quick")
        link_id = data["link"]["id"]
        self.assertEqual(self.mutate("POST", "/api/redes/admin/publish", {}).status_code, 200)
        with self.client.session_transaction() as session:
            session.pop("admin_autenticado", None)
        self.client.post("/api/redes/view", json={})
        self.client.get(f"/redes/go/{link_id}")
        with self.client.session_transaction() as session:
            session["admin_autenticado"] = True
        denied = self.client.post("/api/redes/admin/metrics/reset", json={"confirm": "BORRAR ESTADISTICAS"})
        self.assertEqual(denied.status_code, 403)
        wrong = self.mutate("POST", "/api/redes/admin/metrics/reset", {"confirm": "borrar"})
        self.assertEqual(wrong.status_code, 400)
        before = self.client.get("/api/redes/admin/state?period=30").get_json()["stats"]
        self.assertEqual(before["views"], 1)
        self.assertEqual(before["clicks"], 1)
        reset = self.mutate("POST", "/api/redes/admin/metrics/reset", {"confirm": "BORRAR ESTADISTICAS"})
        self.assertEqual(reset.status_code, 200)
        self.assertEqual(reset.get_json()["deleted"], 2)
        after = self.client.get("/api/redes/admin/state?period=30").get_json()["stats"]
        self.assertEqual(after["views"], 0)
        self.assertEqual(after["clicks"], 0)
        self.assertEqual(self.client.get(f"/redes/go/{link_id}").status_code, 302)

    def test_draft_archive_does_not_change_live_link_until_publish(self):
        _, data = self.create_link()
        link_id = data["link"]["id"]
        self.assertEqual(self.mutate("POST", "/api/redes/admin/publish", {}).status_code, 200)
        self.assertEqual(self.mutate("POST", f"/api/redes/admin/links/{link_id}/archive", {}).status_code, 200)
        self.assertTrue(self.client.get("/api/redes/admin/state").get_json()["has_changes"])
        self.assertEqual(self.client.get(f"/redes/go/{link_id}").status_code, 302)
        self.assertEqual(self.mutate("POST", "/api/redes/admin/publish", {}).status_code, 200)
        self.assertFalse(self.client.get("/api/redes/admin/state").get_json()["has_changes"])
        unavailable = self.client.get(f"/redes/go/{link_id}")
        self.assertEqual(unavailable.status_code, 404)
        self.assertIn("Este enlace no está disponible", unavailable.get_data(as_text=True))

    def test_standard_links_render_group_label_from_saved_data(self):
        created, _ = self.create_link(title="Instagram Sucrée", url="https://instagram.com/pasteleria_sucree", layout="standard", group="Síguenos")
        self.assertEqual(created.status_code, 201)
        self.assertEqual(self.mutate("POST", "/api/redes/admin/publish", {}).status_code, 200)
        html = self.client.get("/redes").get_data(as_text=True)
        self.assertIn("Más de Pastelería Sucrée", html)
        self.assertIn("Síguenos", html)
        self.assertIn("Instagram Sucrée", html)

    def test_social_links_stay_in_three_columns_on_mobile(self):
        _, _ = self.create_link(title="Facebook", url="https://facebook.com/pasteleria_sucree", layout="social")
        self.assertEqual(self.mutate("POST", "/api/redes/admin/publish", {}).status_code, 200)
        html = self.client.get("/redes").get_data(as_text=True)
        self.assertIn(".social-row{display:grid;grid-template-columns:repeat(3,minmax(0,1fr))", html)
        self.assertIn('<div class="social-row"><a class="social-card"', html)

    def test_profile_version_conflict_and_publish_revision_restore(self):
        _, data = self.create_link(layout="quick")
        self.assertEqual(self.mutate("POST", "/api/redes/admin/publish", {}).status_code, 200)
        state = self.client.get("/api/redes/admin/state").get_json()
        profile = dict(state["profile"], title="Nuevo borrador")
        saved = self.mutate("POST", "/api/redes/admin/profile", {"profile": profile, "version": state["version"]})
        self.assertEqual(saved.status_code, 200)
        stale = self.mutate("POST", "/api/redes/admin/profile", {"profile": profile, "version": state["version"]})
        self.assertEqual(stale.status_code, 409)
        self.assertEqual(self.mutate("POST", "/api/redes/admin/publish", {}).status_code, 200)
        state = self.client.get("/api/redes/admin/state").get_json()
        revision_id = state["revisions"][0]["id"]
        restored = self.mutate("POST", f"/api/redes/admin/revisions/{revision_id}/restore", {})
        self.assertEqual(restored.status_code, 200)
        self.assertIn("Nuevo borrador", self.client.get("/redes").get_data(as_text=True))
        state = self.client.get("/api/redes/admin/state").get_json()
        self.assertNotEqual(state["profile"]["title"], "Nuevo borrador")

    def test_prefetch_does_not_record_view_and_image_upload_is_optimized(self):
        response = self.client.post("/api/redes/view", json={}, headers={"Purpose": "prefetch"})
        self.assertFalse(response.get_json()["counted"])
        image = Image.new("RGB", (12, 8), "orange")
        data = BytesIO()
        image.save(data, format="PNG")
        data.seek(0)
        upload = self.client.post("/api/redes/admin/image", data={"image": (data, "cover.png", "image/png")}, headers={"X-CSRF-Token": self.csrf}, content_type="multipart/form-data")
        self.assertEqual(upload.status_code, 200)
        url = upload.get_json()["url"]
        self.assertRegex(url, r"^/static/uploads/redes/[a-f0-9]{32}\.jpg$")
        filename = os.path.basename(url)
        with open(os.path.join(self.upload_dir, filename), "rb") as image_file:
            self.assertTrue(image_file.read(3).startswith(b"\xff\xd8\xff"))


    def test_qr_link_is_generated_downloadable_and_tracks_global_scans(self):
        page = self.client.get("/admin/redes").get_data(as_text=True)
        self.assertIn('id="qr-form"', page)
        self.assertIn('id="qr-modal"', page)
        created = self.mutate("POST", "/api/redes/admin/qr", {
            "title": "Catalogo QR",
            "kind": "link",
            "url": "https://pasteleriasucree.cl/tienda",
        })
        self.assertEqual(created.status_code, 201)
        item = created.get_json()["qr"]
        self.assertTrue(item["tracking_path"].startswith("/redes/qr/"))

        image = self.client.get(f"/api/redes/admin/qr/{item['id']}/image.png?download=1")
        self.assertEqual(image.status_code, 200)
        self.assertEqual(image.mimetype, "image/png")
        self.assertIn(b"PNG", image.data[:8])

        with self.client.session_transaction() as session:
            session.pop("admin_autenticado", None)
        scan = self.client.get(item["tracking_path"], follow_redirects=False)
        self.assertEqual(scan.status_code, 302)
        self.assertEqual(scan.location, "https://pasteleriasucree.cl/tienda")
        with self.client.session_transaction() as session:
            session["admin_autenticado"] = True
        metrics = self.client.get(f"/api/redes/admin/qr/{item['id']}/metrics").get_json()
        self.assertEqual(metrics["total_scans"], 1)
        self.assertEqual(metrics["recent"][0]["source"], "desconocido")
        state = self.client.get("/api/redes/admin/state").get_json()
        self.assertEqual(state["qr_codes"][0]["scans"], 1)
        self.assertEqual(state["qr_codes"][0]["destination"], "https://pasteleriasucree.cl/tienda")
        removed = self.mutate("POST", f"/api/redes/admin/qr/{item['id']}/deactivate", {})
        self.assertEqual(removed.status_code, 200)
        self.assertEqual(self.client.get(item["tracking_path"]).status_code, 404)
        self.assertEqual(self.client.get(f"/api/redes/admin/qr/{item['id']}/metrics").get_json()["total_scans"], 1)

    def test_qr_signup_configuration_and_scan_open_registration_popup(self):
        saved = self.mutate("POST", "/api/redes/admin/qr-signup", {
            "active": True, "discount_pct": 17.5, "validity_days": 21,
        })
        self.assertEqual(saved.status_code, 200)
        self.assertEqual(saved.get_json()["config"]["discount_pct"], 17.5)
        created = self.mutate("POST", "/api/redes/admin/qr", {
            "title": "Registro QR", "kind": "signup",
        })
        self.assertEqual(created.status_code, 201)
        item = created.get_json()["qr"]
        self.assertEqual(item["purpose"], "signup")
        self.assertEqual(item["destination"], "/redes?qr_registro=1")
        with self.client.session_transaction() as session:
            session.pop("admin_autenticado", None)
        scan = self.client.get(item["tracking_path"], follow_redirects=False)
        self.assertEqual(scan.status_code, 302)
        page = self.client.get(scan.location)
        html = page.get_data(as_text=True)
        self.assertIn('name="redes-promo-source" content="qr"', html)
        self.assertIn("17.5% de descuento", html)
        self.assertIn("forcePromo=true", html)

    def test_qr_whatsapp_uses_prefilled_message_and_validation(self):
        created = self.mutate("POST", "/api/redes/admin/qr", {
            "title": "WhatsApp QR",
            "kind": "whatsapp",
            "phone": "+56912345678",
            "message": "Hola Sucrée, quiero consultar",
        })
        self.assertEqual(created.status_code, 201)
        item = created.get_json()["qr"]
        self.assertEqual(item["destination"], "https://wa.me/56912345678?text=Hola+Sucr%C3%A9e%2C+quiero+consultar")
        invalid = self.mutate("POST", "/api/redes/admin/qr", {
            "title": "Sin mensaje", "kind": "whatsapp", "phone": "+56912345678", "message": "",
        })
        self.assertEqual(invalid.status_code, 400)
        too_long = self.mutate("POST", "/api/redes/admin/qr", {
            "title": "Mensaje extenso", "kind": "whatsapp", "phone": "+56912345678", "message": "🙂" * 300,
        })
        self.assertEqual(too_long.status_code, 400)

    def test_qr_admin_endpoints_require_auth_and_csrf_for_creation(self):
        with self.client.session_transaction() as session:
            session.pop("admin_autenticado", None)
        self.assertEqual(self.client.get("/api/redes/admin/state").status_code, 401)
        with self.client.session_transaction() as session:
            session["admin_autenticado"] = True
        denied = self.client.post("/api/redes/admin/qr", json={"title": "QR", "kind": "link", "url": "/tienda"})
        self.assertEqual(denied.status_code, 403)


if __name__ == "__main__":
    unittest.main()
