import os
import tempfile
import unittest
import zipfile
from io import BytesIO
from unittest.mock import patch

import app


def apk_bytes():
    stream = BytesIO()
    with zipfile.ZipFile(stream, "w") as apk:
        apk.writestr("AndroidManifest.xml", b"manifest")
    return stream.getvalue()


class ApksAdminTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.uploads = os.path.join(self.temp.name, "uploads")
        self.seed = os.path.join(self.temp.name, "seed")
        os.makedirs(self.seed)
        self.config_patch = patch.dict(app.app.config, {
            "TESTING": True,
            "APKS_UPLOAD_DIR": self.uploads,
            "APKS_SEED_DIR": self.seed,
        })
        self.config_patch.start()
        self.client = app.app.test_client()
        with open(os.path.join(self.seed, "Seed-1.0.0.apk"), "wb") as seed:
            seed.write(apk_bytes())
        with self.client.session_transaction() as session:
            session["admin_autenticado"] = True

    def tearDown(self):
        self.config_patch.stop()
        self.temp.cleanup()

    def csrf(self):
        self.client.get("/apks")
        with self.client.session_transaction() as session:
            return session["apks_csrf"]

    def test_page_lists_seed_but_requires_admin(self):
        response = self.client.get("/apks")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Seed-1.0.0.apk", response.data)
        with self.client.session_transaction() as session:
            session.pop("admin_autenticado", None)
        self.assertEqual(self.client.get("/apks").status_code, 302)

    def test_upload_download_and_delete_uploaded_apk(self):
        token = self.csrf()
        response = self.client.post("/admin/apks/subir", data={
            "csrf_token": token,
            "archivo": (BytesIO(apk_bytes()), "CallSpam-1.0.0.apk"),
        }, content_type="multipart/form-data")
        self.assertEqual(response.status_code, 302)
        self.assertTrue(os.path.isfile(os.path.join(self.uploads, "CallSpam-1.0.0.apk")))
        download = self.client.get("/admin/apks/archivo/uploaded/CallSpam-1.0.0.apk")
        self.assertEqual(download.status_code, 200)
        self.assertEqual(download.mimetype, "application/vnd.android.package-archive")
        download.close()
        deleted = self.client.post("/admin/apks/eliminar/CallSpam-1.0.0.apk", data={"csrf_token": token})
        self.assertEqual(deleted.status_code, 302)
        self.assertFalse(os.path.exists(os.path.join(self.uploads, "CallSpam-1.0.0.apk")))

    def test_rejects_non_apk_and_invalid_payload(self):
        token = self.csrf()
        bad_extension = self.client.post("/admin/apks/subir", data={
            "csrf_token": token,
            "archivo": (BytesIO(apk_bytes()), "not-an-apk.zip"),
        }, content_type="multipart/form-data")
        self.assertIn("Solo se permiten".encode(), self.client.get(bad_extension.location).data)
        invalid_package = self.client.post("/admin/apks/subir", data={
            "csrf_token": token,
            "archivo": (BytesIO(b"not an apk file"), "invalid.apk"),
        }, content_type="multipart/form-data")
        self.assertIn("no parece ser".encode(), self.client.get(invalid_package.location).data)
        self.assertFalse(os.path.exists(os.path.join(self.uploads, "invalid.apk")))

    def test_upload_requires_csrf_token(self):
        response = self.client.post("/admin/apks/subir", data={
            "archivo": (BytesIO(apk_bytes()), "Nope.apk"),
        }, content_type="multipart/form-data")
        self.assertEqual(response.status_code, 302)
        self.assertFalse(os.path.exists(os.path.join(self.uploads, "Nope.apk")))


if __name__ == "__main__":
    unittest.main()
